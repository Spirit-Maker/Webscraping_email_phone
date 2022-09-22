#!/usr/bin/env python3

import coloredlogs
import logging
from threading import Thread
from queue import Queue
from time import time
import requests
import re
import csv
import json
import pandas as pd
from bs4 import BeautifulSoup

requests.packages.urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

coloredlogs.install(logger=logger)


# function to remove duplicates

def remove_dup_email(x):
  return list(dict.fromkeys(x))

def remove_dup_phone(x):
  return list(dict.fromkeys(x))


# functions to get information
def get_email(html):
    try:
        email = re.findall("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,3}",html)
        nodup_email = remove_dup_email(email)
        return [i.strip() for i in nodup_email]
    except Exception as e:
        logger.error(f"Email search error: {e}")

def get_phone(html):
    try:
        phone = re.findall(r"(\d{2} \d{2,4} \d{3,8})", html)
        phone1= re.findall(r"((?:\d{2,3}|\(\d{2,3}\))?(?:\s|-|\.)?\d{3,4}(?:\s|-|\.)\d{4})",html)
        for p in phone1:
             phone.append(p)
        nodup_phone = remove_dup_phone(phone)
        return [i.strip() for i in nodup_phone]
    except Exception as e:
        logger.error(f"Phone search error: {e}")


def read_file():
    urls = []
    with open('web_urls.txt', 'r') as f:
        for line in f.readlines():
            urls.append(line.strip())
    
    return urls



def scrape_links(filters=None):
    pass

# Threaded function for queue processing.
def crawl(q, result):
    while not q.empty():
        url = q.get()                      #fetch new work from the Queue
        try:
            res = requests.get(url[1], verify=False)
            logger.info(f'searched home url: {res.url}')

            if res.status_code != 200:
                result[url[0]] = {}
                logger.warning(f"{url[1]} Status code: {res.status_code}")
                continue

            info = BeautifulSoup(res.text,'lxml')
            # extract contact data from home url

            emails_home = get_email(info.get_text())
            phones_home = get_phone(info.get_text())

            emails_f = emails_home
            phones_f = phones_home

            contacts_f = {'website':res.url,'Email':'','Phone':''}

            # extract contact of the link if available
            try:
                contact = info.find('a', text = re.compile('contact', re.IGNORECASE))['href']
                if 'http' in contact:
                    contact_url = contact
                else:
                    contact_url = res.url[0:-1]+ "/" + contact

                if contact_url != res.url:

                    # searching contact URL
                    
                    res_contact = requests.get(contact_url, verify=False)

                    contact_info = BeautifulSoup(res_contact.text, 'lxml').get_text()

                    logger.info(f'searched contact url: {res_contact.url}')

                    # extract contact data
                    
                    emails_contact = get_email(contact_info)
                    phones_contact = get_phone(contact_info)

                    #combining email contacts and email home into a single list

                    emails_f = emails_home

                    for ele1 in emails_contact:
                        emails_f.append(ele1)

                    #combining phone contacts and phone contacts into a single list
                
                    phones_f = phones_home

                    for ele2 in phones_contact:
                        phones_f.append(ele2)
                
            except Exception as e:
                logger.error(f'Error in contact URL: {e}')

            # removing duplicates

            emails_f = remove_dup_email(emails_f)
            phones_f = remove_dup_email(phones_f)

            contacts_f['Email']= emails_f
            contacts_f['Phone']= phones_f
            
            # converting into a data set
            
            
            logger.debug(f"Scrape Data: {contacts_f}")

            result[url[0]] = contacts_f          #Store data back at correct index
        except Exception as e:
            logger.error(f"Request error in threads: {e}")
            result[url[0]] = {}
        #signal to the queue that task has been processed
        finally:
            q.task_done()
            logger.debug(f"Queue task no {url[0]} completed.")
    return True




def main():
    urls = read_file()

    # Sertup queue to hol urls
    q = Queue(maxsize=0)

    # Use many threads (50 max, or one for each url)
    num_theads = min(50, len(urls))

    #Populating Queue with tasks
    results = [{} for x in urls];

    #load up the queue with the urls to fetch and the index for each job (as a tuple):
    for i in range(len(urls)):
        #need the index and the url in each queue item.
        q.put((i,urls[i]))

    for i in range(num_theads):
        logger.debug(f"Starting threads: {i}")
        worker = Thread(target=crawl, args=(q,results))
        worker.daemon = True
        worker.start()

    q.join()
    
    print(json.dumps(results, indent=4))


if __name__ == "__main__":
    main()
