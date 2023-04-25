from dotenv import load_dotenv
import os
from pymongo import MongoClient
from datetime import date, timedelta, datetime
import pytest

load_dotenv()

harvestdate = date.today() - timedelta(days = 1)
successdate = datetime(2023, 4, 22)

def test_success_date():

    mongo_url = os.environ.get('MONGO_URL')
    mongo_dbname = os.environ.get('MONGO_DBNAME')
    mongo_client = MongoClient(mongo_url, maxPoolSize=1)

    mongo_db = mongo_client[mongo_dbname]
    testcoll = mongo_db['test_jstor_harvested_summary']
    rec = testcoll.find({"repository_id":"713", "success":True}, {"harvest_date":1}).sort("harvest_date", -1).limit(1)
    latest_mongo_success_date = rec[0]['harvest_date']

    mongo_client.close()

    assert latest_mongo_success_date != harvestdate
    assert latest_mongo_success_date == successdate

