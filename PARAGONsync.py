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
        doc["PARAGON_CLASS"] = className
        requests.append(
            UpdateOne(
                {"L_ListingID": doc["L_ListingID"]},
                {"$set": doc},
                upsert=True
            )
        )
    try:
        replacementResult = collection.bulk_write(requests, ordered=False)
        print(f'    PARAGON: Listings returned: {len(docs)}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings')
    except BulkWriteError as bwe:
        print(bwe.details)

def getAndUpload(rClass, query, collection):
    try:
        st = time.time()
        search_result = list(map( 
            lambda x: convert_decimal(x.data), 
            rClass.search(query=query).data)
        )
        st = time.time()-st
    except requests.exceptions.HTTPError as exc:
        print("    PARAGON: failed, requests.exceptions.HTTPError, retrying...")
        traceback.print_exc()
        time.sleep(.2)
        retsClient =  RetsClient( # An attempt to fix requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
            login_url=os.getenv("PARAGON_LOGIN_URL"),
            username=os.getenv("PARAGON_USERNAME"),
            password=os.getenv("PARAGON_PASSWORD"),
        )
        rClass = retsClient.get_resource('Property').get_class(rClass.name)
        return getAndUpload(rClass, query, collection)
    except urllib3.exceptions.MaxRetryError as exc:
        print("    PARAGON: failed", exc)
        traceback.print_exc()
        time.sleep(1)
        retsClient =  RetsClient(
            login_url=os.getenv("PARAGON_LOGIN_URL"),
            username=os.getenv("PARAGON_USERNAME"),
            password=os.getenv("PARAGON_PASSWORD"),
        )
        rClass = retsClient.get_resource('Property').get_class(rClass.name)
        return getAndUpload(rClass, query, collection)
    except AttributeError as exc:
        print("    PARAGON: failed", exc)
        traceback.print_exc()
        return rClass
    except Exception as exc:
        # AttributeError is not a result of the query coming back empty, this needs to be fixed
        print("    PARAGON: failed", exc)
        traceback.print_exc()
        time.sleep(.5)
        return rClass
    else:
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
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print('    PARAGON: Exception: %s' % exc)
        it = time.time()-it
        print(f"    PARAGON: Inserting {resultsCount} docs from query {query} took {round(st, 1)} seconds to search and {round(it, 1)} seconds to insert.")
        return rClass

def seed(batches:int = 1) -> None:
    load_dotenv(verbose=True)
    mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    mongoClient = MongoClient(mongoConnectionString) # Our database connector
    dbCollection = mongoClient["paragon"]['Property']

    retsClient =  RetsClient(
        login_url=os.getenv("PARAGON_LOGIN_URL"),
        username=os.getenv("PARAGON_USERNAME"),
        password=os.getenv("PARAGON_PASSWORD"),
    )

    for rClass in retsClient.get_resource('Property').classes:
        print("    PARAGON: Updating class:", rClass.name)
        maxID = 50000000
        step = maxID // batches
        for minID in range(0, maxID, step):
            query = f"(L_ListingID={minID}-{minID+step})"
            rClass = getAndUpload(rClass, query, dbCollection)
        for query in ["(L_ListingID=0-)", "(L_ListingID=50000000+)"]:
            rClass = getAndUpload(rClass, query, dbCollection)


if __name__ == "__main__":
    seed(1000)