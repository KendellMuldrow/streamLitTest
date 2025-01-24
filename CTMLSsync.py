# Developed by Andrew Pantera for TLCengine
# Download CTMLS data and upload to MongoDB on TFS
# Each resource of the API will be a Collection in Mongo
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
import rets
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

def seed(retsClient, dbCursor, skipResources={'office', 'memberassociation', 'virtualtour', 'member', 'comm', 'oh', 'officeassociation', 'memberlicense'}, skipClasses={}):
    for resource in retsClient.resources:
        if resource.name.lower() not in skipResources: # For re-running without duplicates
            # Each resource will be its own collection in the MongoDB
            collection = dbCursor[resource.name]
            logging.info(f"    CTMLS: Syncing resource: {resource.name}")
            for rClass in resource.classes:
                if rClass.name not in skipClasses:
                    if resource.name == "RESI": # We need to split up the queries for RESI because there's too much data, but we can't split up the queries in the same way for the other resources because they don't all have the same fields
                        # seedRESI(skipResources, skipClasses, skipRESI)
                        pass
                    else:
                        logging.info("    Syncing class:", rClass.name)
                        resourceQueries = {'Teams': '(TeamKeyNumeric=0+)', 'TeamMembers':'(ModificationTimestamp=1960-01-01+)'}
                        query = resourceQueries.get(resource.name)
                        query = '(Matrix_Unique_ID=0+)' if not query else query
                        search_result = tuple(map( 
                            lambda x: convert_decimal(x.data), 
                            rClass.search(query=query).data))
                        insert_result = collection.insert_many(search_result)
                        filter = {"_id": {"$in": insert_result.inserted_ids}}
                        update = {"$set": {"CTMLS_CLASS": rClass.name}}
                        collection.update_many(filter, update)
                else:
                    logging.info("    Skipping", rClass.name)
        else:
            logging.info("Skipping", resource.name)

def uploadListings(docs, collection):
    requests = []
    for doc in docs:
        requests.append(
            UpdateOne(
                {"Matrix_Unique_ID": doc["Matrix_Unique_ID"]},
                {"$set": doc},
                upsert=True
            )
        )
    replacementResult = collection.bulk_write(requests, ordered=False)
    logging.info(f'    CTMLS: Listings returned: {len(docs)}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings')

def getListings(rClass, query, collection, LastUpdatedID=None):
    queryWithID = query
    if LastUpdatedID is not None:
        if query:
            queryWithID = query + f", (Matrix_Unique_ID={LastUpdatedID}+)"
        else:
            queryWithID = f"(Matrix_Unique_ID={LastUpdatedID}+)"
        
    try:
        st = time.time()
        search_result = list(map( 
            lambda x: convert_decimal(x.data), 
            rClass.search(query=queryWithID).data)
        )
        st = time.time()-st
    except requests.exceptions.HTTPError:
        logging.info("    CTMLS: failed, requests.exceptions.HTTPError, retrying...")
        time.sleep(.2)
        retsClient =  RetsClient(
            login_url=os.getenv("CTMLS_LOGIN_URL"),
            username=os.getenv("CTMLS_USERNAME"),
            password=os.getenv("CTMLS_PASSWORD"),
        )
        rClass = retsClient.get_resource('Property').get_class('Listing')
        return getListings(rClass, query, collection, LastUpdatedID)
    except rets.errors.RetsApiError as exc:
        # if "Too many outstanding queries" is recieved, wait 10 seconds, log in again, and try again
        logging.info(f"    CTMLS: failed, {exc}, retrying...")
        time.sleep(10)
        retsClient =  RetsClient(
            login_url=os.getenv("CTMLS_LOGIN_URL"),
            username=os.getenv("CTMLS_USERNAME"),
            password=os.getenv("CTMLS_PASSWORD"),
        )
        rClass = retsClient.get_resource('Property').get_class('Listing')
        return getListings(rClass, query, collection, LastUpdatedID)
    except TypeError as exc:
        # Every once in a while an entire batch fails because the rets connector library doesn't like the format of some of the data returned. When this happens, we should process the data in smaller batches so that we don't lose 5000 listings all because 1 is bad
        logging.info("    CTMLS: failed", exc)
        traceback.print_exc()
        if LastUpdatedID:
            logging.info("    CTMLS: Skipping 5000 IDs and trying again...")
            # Because of the density of IDs within this MLS, 5000 Ids will likely contain less than 500 listings, but at max 5000 listings
            return getListings(rClass, query, collection, LastUpdatedID+5000)
        return rClass
    else:
        if search_result:
            batchSize = 1000
            resultsCount = len(search_result)
            if resultsCount == 5000:
                LastUpdatedID = search_result[-1]['Matrix_Unique_ID']
            it = time.time()
            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = {}
                while len(search_result) != 0:
                    futures[executor.submit(
                        uploadListings,
                        search_result[:batchSize],
                        collection
                    )] = len(search_result[:batchSize])
                    search_result = search_result[batchSize:]
                # with tqdm(total=resultsCount, desc="Pushing batches of max size " + str(batchSize), ncols=100) as pbar:
                for future in concurrent.futures.as_completed(futures):
                    # pbar.update(futures[future])
                    try:
                        future.result()
                    except Exception as exc:
                        logging.info('    CTMLS: Exception: %s' % exc)
            it = time.time()-it
            logging.info(f"    CTMLS: Inserting {resultsCount} docs from query {queryWithID} took {round(st, 1)} seconds to search and {round(it, 1)} seconds to insert.")
            if resultsCount == 5000:
                return getListings(rClass, query, collection, LastUpdatedID)
        return rClass

def seedProperty(dbCollection):
    # Seed just the listing resource of the property class
    retsClient =  RetsClient(
        login_url=os.getenv("CTMLS_LOGIN_URL"),
        username=os.getenv("CTMLS_USERNAME"),
        password=os.getenv("CTMLS_PASSWORD"),
    )
    rClass = retsClient.get_resource('Property').get_class('Listing')
    getListings(rClass, "", dbCollection, LastUpdatedID=0)

def update(timeDelta: datetime.timedelta) -> None:
    load_dotenv(verbose=True)
    mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    mongoClient = MongoClient(mongoConnectionString) # Our database connector
    dbCollection = mongoClient["ctmls"]["Property"]

    try:
        retsClient =  RetsClient(
            login_url=os.getenv("CTMLS_LOGIN_URL"),
            username=os.getenv("CTMLS_USERNAME"),
            password=os.getenv("CTMLS_PASSWORD"),
        )
    except requests.exceptions.HTTPError as exc:
        logging.info("    CTMLS: failed, requests.exceptions.HTTPError, retrying...")
        logging.info(exc)
        time.sleep(15)
        retsClient =  RetsClient(
            login_url=os.getenv("CTMLS_LOGIN_URL"),
            username=os.getenv("CTMLS_USERNAME"),
            password=os.getenv("CTMLS_PASSWORD"),
        )
        
    utcMinusFour = datetime.timezone(datetime.timedelta(0, -4*3600))
    oldestTimestamp = (datetime.datetime.now(tz=utcMinusFour) - timeDelta).isoformat().split('.')[0]
    rClass = retsClient.get_resource('Property').get_class('Listing')
    query = f"(MatrixModifiedDT={oldestTimestamp}+)"
    getListings(rClass, query, dbCollection, LastUpdatedID=118791173)



def main():
    update(datetime.timedelta(0, 120))

    # rClass = retsClient.get_resource('Property').get_class('Listing')
    # query = f"(MatrixModifiedDT={anHourAgo.isoformat()[:-3]}+)"
    
    # result = rClass.search(query=query).data
    # for listing in result:
    #     logging.info(listing.data['Matrix_Unique_ID'], listing.data['MatrixModifiedDT'])

    # logging.info(len(result))
    

if __name__=="__main__":
    main()
