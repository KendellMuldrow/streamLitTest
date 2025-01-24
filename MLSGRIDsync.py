# Developed by Andrew Pantera for TLCengine on 5/20/2021
# Download MLSGRID data and upload to MongoDB on TFS
# API Documentation https://docs.mlsgrid.com/api-documentation/api-version-2.0
# Each resource of the API will be a Collection in Mongo
# This file uses python 3.7 features
import asyncio
from concurrent.futures import Executor, ThreadPoolExecutor, as_completed
import concurrent.futures
import collections
import datetime
from decimal import Decimal
from dotenv import load_dotenv
import json
import logging
import os
from pprint import pprint
import pymongo
from pymongo import MongoClient, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
import requests
import signal
import traceback
from tqdm import tqdm
import time
from typing import Iterable 
import sys
import urllib3
import urllib

def SentenceCase(s):
    return " ".join(map(
        lambda x: x[0].upper() + x[1:].lower() if x else '', 
        s.split(' ')
    ))

def transformListing(listing: dict) -> dict:
    # listing is a dictionary, replace the names of the keys in the dictionary
    listing["_id"] = listing.pop("@odata.id")
    for key in ('City', 'CountyOrParish', 'MlsStatus', 'PropertyType'):
        if key in listing and isinstance(listing[key], str):
            listing[key] = SentenceCase(listing[key])
    return listing

async def uploadListings(jsonValueField: Iterable[dict], collection: pymongo.collection.Collection) -> None:
    # Takes an iterable of listings, scrubs their id, and updates them in the supplied mongo collection
    listings = tuple(map(
        transformListing,
        jsonValueField
    ))
    requests = []
    for doc in listings:
        requests.append(
            UpdateOne(
                {"_id": doc["_id"]}, # this is the filter, update the listing in the db that has the same _id filter as this listing
                {"$set": doc}, # This is the update, here we replace every field in the doc. If a field no longer exists in the new doc, it persists in the db
                upsert=True # if no listing with _id of _id exists in the db, a new one will be added
            )
        )
    replacementResult = collection.bulk_write(requests, ordered=False)
    logging.info(f'    MLSGRID: Listings returned: {len(listings)}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings')

async def seed(skip: str = "0", filter: str = None) -> None:
    load_dotenv(verbose=True) # Load db credentials from .env
    mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    mongoClient = MongoClient(mongoConnectionString) # Our database connector
    dbCursor = mongoClient["mlsgrid"]["Property"]

    headers = {"Authorization": "Bearer " + os.getenv("MLSGRID_TOKEN")} 
    payload = {
        "$filter": "OriginatingSystemName eq 'mred'",
    }
    if filter:
        payload["$filter"] = payload["$filter"] + " and " + filter
    if skip != "0":
        payload["$skip"] = skip

    response = requests.get(os.getenv("MLSGRID_URL"), headers=headers, params=urllib.parse.urlencode(payload))

    startTime = time.time()
    iterations = 0
    if (response.status_code == 200) and response.json() and ('@odata.nextLink' not in response.json()) and not response.json()['value']:
        logging.info(f"    MLSGRID: No listings found for filter {filter}")
        return None
    while (response.status_code == 200) and response.json() and ('@odata.nextLink' in response.json()):
        listings = response.json()['value']
        task = asyncio.create_task(uploadListings(listings, dbCursor))

        iterations += 1
        timeElapsed = time.time() - startTime
        logging.info(f"    MLSGRID: {round(iterations/timeElapsed, 3)} iterations per second.")

        response = requests.get(response.json()['@odata.nextLink'], headers=headers)
        # here we upload the previous batch of listings to the database and get the new batch concurrently
        await task 


    if response.status_code == 200 and response.json():
        await uploadListings(response.json()['value'], dbCursor)
    else:
        
        logging.info(f"     MLSGRID: last request returned status code: {response.status_code}")
        logging.info(response.json())


def update(timeDelta: datetime.timedelta) -> None:
    # MLSGRID stores their timezones in UTC offset 0 time
    zulu = datetime.timezone(datetime.timedelta(0))
    oldestTimestamp = datetime.datetime.now(tz=zulu) - timeDelta
    oldestTimestampFormatted = oldestTimestamp.isoformat()[:-9] + 'Z' # The Z is the UTC timezone 'Zulu' represeting a zero offset from UTC
    filter = f"ModificationTimestamp ge {oldestTimestampFormatted}"
    
    asyncio.run(seed(filter=filter))

def main():
    # asyncio.run(seed("1890000"))
    update(datetime.timedelta(0, 30))


if __name__=="__main__":
    main()
