# Developed by Andrew Pantera for TLCengine on 3/23/2021, last updated 4/18/2021
# Download MLSPIN data and upload to MongoDB on TFS
# Each resource of the API will be a Collection in Mongo
from bson.code import Code
from bson.decimal128 import Decimal128
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
from rets.client import RetsClient
import requests
import signal
import traceback
from tqdm import tqdm
import time
import sys
import urllib3

failedQueries = []
lastSuccessfulQuery = ""

def convert_decimal(dict_item):
    # function adapted from: https://stackoverflow.com/questions/61456784/pymongo-cannot-encode-object-of-type-decimal-decimal
    # This function iterates a dictionary looking for types of Decimal and converts them to Decimal128
    # Embedded dictionaries and lists are called recursively.
    if dict_item is None: return None
    if not (isinstance(dict_item, dict) or isinstance(dict_item, collections.OrderedDict)): return dict_item
    for k, v in list(dict_item.items()):
        if isinstance(v, dict):
            convert_decimal(v)
        elif isinstance(v, list):
            for l in v:
                convert_decimal(l)
        elif isinstance(v, Decimal):
            dict_item[k] = Decimal128(str(v))
    return dict_item

def uploadMany(docs, collection, className):
    requests = []
    for doc in docs:
        doc["MLSPIN_CLASS"] = className
        requests.append(
            UpdateOne(
                {"LIST_NO": doc["LIST_NO"]},
                {"$set": doc},
                upsert=True
            )
        )
    try:
        replacementResult = collection.bulk_write(requests, ordered=False)
        logging.info(f'    MLSPIN: Listings returned: {len(docs)}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings')
    except BulkWriteError as bwe:
        logging.info(bwe.details)

def getAndUpload(rClass, query, collection):
    try:
        st = time.time()
        search_result = list(map( 
            lambda x: convert_decimal(x.data), 
            rClass.search(query=query).data)
        )
        st = time.time()-st
    except requests.exceptions.HTTPError as exc:
        logging.info("    MLSPIN: failed, requests.exceptions.HTTPError, retrying...")
        traceback.print_exc()
        time.sleep(.2)
        retsClient =  RetsClient( # An attempt to fix requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: https://bridge-rets.mlspin.com:12109/rets/search
            login_url=os.getenv("MLSPIN_LOGIN_URL"),
            username=os.getenv("MLSPIN_USERNAME"),
            password=os.getenv("MLSPIN_PASSWORD"),
            auth_type='basic',
        )
        rClass = retsClient.get_resource('RESI').get_class(rClass.name)
        return getAndUpload(rClass, query, collection)
    except urllib3.exceptions.MaxRetryError as exc:
        logging.info("    MLSPIN: failed")
        logging.info(exc)
        traceback.print_exc()
        failedQueries.append(query)
        time.sleep(1)
        retsClient =  RetsClient(
            login_url=os.getenv("MLSPIN_LOGIN_URL"),
            username=os.getenv("MLSPIN_USERNAME"),
            password=os.getenv("MLSPIN_PASSWORD"),
            auth_type='basic',
        )
        rClass = retsClient.get_resource('RESI').get_class(rClass.name)
        return getAndUpload(rClass, query, collection)
    except AttributeError as exc:
        logging.info("    MLSPIN: failed")
        logging.info(exc)
        traceback.print_exc()
        failedQueries.append(query)
        return rClass
    except Exception as exc:
        # AttributeError is not a result of the query coming back empty, this needs to be fixed
        logging.info("    MLSPIN: failed")
        logging.info(exc)
        traceback.print_exc()
        failedQueries.append(query)
        time.sleep(.5)
        return rClass
    else:
        lastSuccessfulQuery = query
        batchSize = 1000
        resultsCount = len(search_result)
        it = time.time()
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = {}
            while len(search_result) != 0:
                futures[executor.submit(
                    uploadMany,
                    search_result[:batchSize],
                    collection,
                    rClass.name
                )] = len(search_result[:batchSize])
                search_result = search_result[batchSize:]
            # with tqdm(total=resultsCount, desc="Pushing batches of max size " + str(batchSize), ncols=100) as pbar:
            for future in concurrent.futures.as_completed(futures):
                # pbar.update(futures[future])
                try:
                    future.result()
                except Exception as exc:
                    logging.info(f'    MLSPIN: Exception: {exc}')
        it = time.time()-it
        logging.info(f"    MLSPIN: Inserting {resultsCount} docs from query {query} took {round(st, 1)} seconds to search and {round(it, 1)} seconds to insert.")
        return rClass

def seedRESI(skipClasses={}, skipRESI=0):
    """Seed the database with all the listings from the API

    Args:
        skipClasses (dict, optional): Classes to not seed. MLSPIN RESI classes is one of [CC, MH, MF, RN, SF, LD]. Defaults to {}.
        skipRESI (int, optional): The number of iterations to skip. Used to resume after failure. Defaults to 0.
    """
    retsClient =  RetsClient(
        login_url=os.getenv("MLSPIN_LOGIN_URL"),
        username=os.getenv("MLSPIN_USERNAME"),
        password=os.getenv("MLSPIN_PASSWORD"),
        auth_type='basic',
    )
    resource = retsClient.get_resource('RESI')
    collection = dbCursor[resource.name]
    for rClass in resource.classes:
        if rClass.name not in skipClasses:
            logging.info("Syncing class:", rClass.name)
            skipped = 0
            for maxListNo in tqdm(range(100000, 100100000, 100000), desc="Syncing batches of 100k", ncols=120): # Ive seen list numbers has high as 30 million and as low as 4, so we check every 10k chunk from 0 to 100 million. This is 10 thousand total searches
                skipped += 1
                if skipped > skipRESI:
                    query = f"(LIST_NO={maxListNo-100000}+), (LIST_NO={maxListNo-1}-)"
                    # logging.info("    Syncing class:", rClass.name, "queried by", query) # Too many messages
                    # The data comes back from the API as a tuple of objects of type rets.client.record.Record. we call .data on these objects to get them to type collections.OrderedDict, which Mongo accepts
                    # pymongo doesn't like decimals, they need to be cast to Decimal128 from bson, so take care of that here aswell
                    rClass = getAndUpload(rClass, query, collection)
            query = f"(LIST_NO={100000000-1}+)"
            getAndUpload(rClass, query, collection)
            
def updateLongTerm(dbCollection, timeDelta: datetime.timedelta) -> None:
    """Updates the collection in MongoDB with all the listings modified after oldestTimestamp
    This function is built to handle long amounts of time, and can seed the entire database.

    Args:
        oldestTimestamp (datetime.datetime): All listings modified at 
        or after this time will be requested from the API and uploaded
    """
    retsClient =  RetsClient(
        login_url=os.getenv("MLSPIN_LOGIN_URL"),
        username=os.getenv("MLSPIN_USERNAME"),
        password=os.getenv("MLSPIN_PASSWORD"),
        auth_type='basic',
    )
    resource = retsClient.get_resource('RESI')
    utcMinusFour = datetime.timezone(datetime.timedelta(0, -4*3600))
    oldestTime = (datetime.datetime.now(tz=utcMinusFour) - timeDelta).isoformat().split('.')[0]
    for rClass in resource.classes:
        logging.info(f"    MLSPIN: Updating class: {rClass.name}")
        for maxListNo in range(100000, 100100000, 100000): 
            query = f"(LIST_NO={maxListNo-100000}+), (LIST_NO={maxListNo-1}-), (UPDATE_DATE={oldestTime}+)"
            rClass = getAndUpload(rClass, query, dbCollection)
        query = f"(LIST_NO={100000000-1}+), (UPDATE_DATE={oldestTime}+)"
        getAndUpload(rClass, query, dbCollection)
        query = f"(LIST_NO={100000}-), (UPDATE_DATE={oldestTime}+)"
        getAndUpload(rClass, query, dbCollection)

def updateShortTerm(dbCollection, timeDelta: datetime.timedelta) -> None:
    """Updates the collection in MongoDB with all the listings modified after oldestTimestamp
    This function is built to handle a small amount of time very quickly. I would not use this
    function for time periods greater than 100 days

    Args:
        oldestTimestamp (datetime.datetime): All listings modified at 
        or after this time will be requested from the API and uploaded
    """
    retsClient =  RetsClient(
        login_url=os.getenv("MLSPIN_LOGIN_URL"),
        username=os.getenv("MLSPIN_USERNAME"),
        password=os.getenv("MLSPIN_PASSWORD"),
        auth_type='basic',
    )
    resource = retsClient.get_resource('RESI')

    # It looks like MLSPIN uses UCT-4
    utcMinusFour = datetime.timezone(datetime.timedelta(0, -4*3600))
    oldestTime = (datetime.datetime.now(tz=utcMinusFour) - timeDelta).isoformat().split('.')[0]
    for rClass in resource.classes:
        logging.info(f"    MLSPIN: Updating class: {rClass.name}")
        query = f"(LIST_NO=0+), (UPDATE_DATE={oldestTime}+)"
        rClass = getAndUpload(rClass, query, dbCollection)

def update(timeDelta: datetime.timedelta) -> None:
    load_dotenv(verbose=True) # Load db credentials from .env
    mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    mongoClient = MongoClient(mongoConnectionString) # Our database connector
    dbCollection = mongoClient["mlspin"]['RESI']

    if timeDelta < datetime.timedelta(35):
        updateShortTerm(dbCollection, timeDelta)
    else:
        # updateLongTerm is more complex but is more robust and blocks in shorter time intervals
        updateLongTerm(dbCollection, timeDelta)



def seed(retsClient, skipResources={'office', 'memberassociation', 'virtualtour', 'member', 'comm', 'oh', 'officeassociation', 'memberlicense'}, skipClasses={}, skipRESI=0):
    for resource in retsClient.resources:
        if resource.name.lower() not in skipResources: # For re-running without duplicates
            # Each resource will be its own collection in the MongoDB
            collection = dbCursor[resource.name]
            logging.info("Syncing resource:", resource.name)
            for rClass in resource.classes:
                if rClass.name not in skipClasses:
                    if resource.name == "RESI": # We need to split up the queries for RESI because there's too much data, but we can't split up the queries in the same way for the other resources because they don't all have the same fields
                        seedRESI(skipResources, skipClasses, skipRESI)
                    else:
                        logging.info("    Syncing class:", rClass.name)
                        search_result = tuple(map( 
                            lambda x: convert_decimal(x.data), 
                            rClass.search(query='').data))
                        insert_result = collection.insert_many(search_result)
                        filter = {"_id": {"$in": insert_result.inserted_ids}}
                        update = {"$set": {"MLSPIN_CLASS": rClass.name}}
                        collection.update_many(filter, update)
                else:
                    logging.info("    Skipping", rClass.name)
        else:
            logging.info("Skipping", resource.name)

if __name__ == "__main__":
    load_dotenv(verbose=True) # Load db credentials from .env
    mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    mongoClient = MongoClient(mongoConnectionString) # Our database connector
    dbCollection = mongoClient["mlspin"]['RESI']

    updateShortTerm(dbCollection, datetime.timedelta(30))
    # logging.info(datetime.datetime.today().isoformat()[:-3])

    # seedRESI(skipRESI=296)

# cursor = collection.aggregate(
#     [
#         {"$group": {"_id": "$LIST_NO", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
#         {"$match": {"count": { "$gte": 2 }}}
#     ],
#     allowDiskUse=True
# )

# response = []
# for doc in cursor:
#     del doc["unique_ids"][0]
#     for id in doc["unique_ids"]:
#         response.append(id)

    # signal.signal(signal.SIGHUP, receiveSignal)
    # signal.signal(signal.SIGINT, receiveSignal)
    # signal.signal(signal.SIGQUIT, receiveSignal)
    # signal.signal(signal.SIGILL, receiveSignal)
    # signal.signal(signal.SIGTRAP, receiveSignal)
    # signal.signal(signal.SIGABRT, receiveSignal)
    # signal.signal(signal.SIGBUS, receiveSignal)
    # signal.signal(signal.SIGFPE, receiveSignal)
    # #signal.signal(signal.SIGKILL, receiveSignal)
    # signal.signal(signal.SIGUSR1, receiveSignal)
    # signal.signal(signal.SIGSEGV, receiveSignal)
    # signal.signal(signal.SIGUSR2, receiveSignal)
    # signal.signal(signal.SIGPIPE, receiveSignal)
    # signal.signal(signal.SIGALRM, receiveSignal)
    # signal.signal(signal.SIGTERM, receiveSignal)

# collection.delete_many({"_id": {"$in": response}})

# logging.info(client.resources)
# logging.info("")

# member = client.get_resource('Member')
# memberClass = member.get_class('Member')
# resi = client.get_resource('RESI')
# search_result = memberClass.search(query='', limit=10)

# search_result = tuple(map(lambda x: x.data, retsClient.get_resource('RESI').get_class('SF').search(query='', limit=1).data))
# mem = search_result[0]
# logging.info(mem)
# logging.info(type(mem))

# logging.info("")
# logging.info(resi.classes)
# for c in ["SF", "MF", "MH", "LD", "RN", "CC"]:
#     logging.info(c, ": ", client.get_resource('RESI').get_class(c).search(query='', limit=1).count, end='. ')

# logging.info(type(retsClient.get_resource('Member').get_class('Member').search(query='').data))

# logging.info(client.get_resource('COMM').get_class('BU').search(query='', limit=1).count)
# logging.info("")

# logging.info(client.get_resource('RESI:SF').get_class('Member').search(query='', limit=10).data[0].data)

# rets_client = Session(os.getenv("MLSPIN_LOGIN_URL"), os.getenv("MLSPIN_USERNAME"), os.getenv("MLSPIN_PASSWORD"))
# rets_client.login()
# # system_data = rets_client.get_system_metadata()
# resources = rets_client.get_resource_metadata(resource='Agent')
# search_results = rets_client.search(resource='Property', resource_class='RES', limit=100, dmql_query='(ListPrice=15000+)')

# logging.info(resources)
