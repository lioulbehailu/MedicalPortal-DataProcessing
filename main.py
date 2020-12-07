from __future__ import print_function

from pymongo import MongoClient
from pprint import pprint

import schedule
import time

import DataStoring

client = MongoClient()
db = client.MedicalPortal

Applicationapisettings = db.applicationapisettings

scheduleData = Applicationapisettings.find_one()

fetchDate = scheduleData['fetchDate']
fetchDay = scheduleData['fetchDay']
fetchHour = scheduleData['fetchHour']
fetchFrequency = scheduleData['fetchFrequency']

isFetching = scheduleData['isFetching']

# print(fetchDate,fetchDay,fetchHour,fetchFrequency,isFetching)

def job():
    if not isFetching:
        # print('True')
        result = DataStoring.StoreData()
        if result:
            print('Data Processing Done')

schedule.every().second.do(job)
# schedule.every(1).minutes.do(job)
# schedule.every().hour.do(job)
# schedule.every().day.at("10:30").do(job)


while 1:
    schedule.run_pending()
    time.sleep(1)