# Developed by Andrew Pantera for TLCengine
from bson.decimal128 import Decimal128
import collections
import datetime
from decimal import Decimal
from dotenv import load_dotenv
import logging
import os
from pymongo import MongoClient, UpdateOne
import rets
from rets.client import RetsClient
import time

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

# Most values for MatrixModifiedDT are datetime.datetime(2016, 7, 26, 14, 34, 5, 137000), the exact same date and time, however, many listings have MatrixModifiedDT values later, up to current day. So it looks like that date in 2016 might be when the field was added
def uploadListings(dbCursor, searchResult):
    listings = list(map( 
        lambda x: convert_decimal(x.data), 
        searchResult)
    )
    requests = []
    for listing in listings:
        requests.append(
            UpdateOne( # I think updating is faster than replacing. A possible downside is fields that are entire removed do not get removed, although I don't think fields can be removed from the MLS because every document has every field, if there is no data for that field it is just None
                {"Matrix_Unique_ID": listing["Matrix_Unique_ID"]},
                {"$set": listing},
                upsert=True
            )
        )
    replacementResult = dbCursor.bulk_write(requests, ordered=False)
    logging.info(f'    MLSMATRIX: Listings returned: {len(listings)}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings')


def seedProperty(startID=0, startDateTime=None):
    load_dotenv(verbose=True) # Load db credentials from .env
    mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    mongoClient = MongoClient(mongoConnectionString) # Our database connector
    dbCursor = mongoClient["mlsmatrix"]["Property"]

    retsClient =  RetsClient(
        login_url=os.getenv("MLSMATRIX_LOGIN_URL"),
        username=os.getenv("MLSMATRIX_USERNAME"),
        password=os.getenv("MLSMATRIX_PASSWORD"),
    )
    retsPropertyClass = retsClient.get_resource("Property").get_class("Listing")
    
    query = f"(Matrix_Unique_ID={startID}+)" + (f",(MatrixModifiedDT={startDateTime}+)" if startDateTime else "")
    try:
        result = retsPropertyClass.search(query)
    except TypeError:
        # getting TypeError: int() argument must be a string, a bytes-like object or a number, not 'NoneType'
        # This is just a problem with the API, there are listings before and after the Matrix_Unique_ID query that resulted in this error. Because of this, we skip 1000 listings and try again
        logging.info(f"    MLSMATRIX: Skipped listings with Matrix_Unique_ID {startID} to {startID+999} because of a type error.")
        return seedProperty(startID=startID+1000, startDateTime=startDateTime)
    except rets.errors.RetsApiError as exc:
        # Getting "Too many outstanding requests"
        logging.info("rets.errors.RetsApiError")
        logging.info(exc)
        logging.info("Waiting 15 seconds and trying again...")
        time.sleep(15)
        return seedProperty(startID=startID, startDateTime=startDateTime)

    resultData = result.data
    if resultData == tuple():
        logging.info("    MLSMATRIX: Query returned no listings")
        return None
    uploadListings(dbCursor, resultData)
    lastID = resultData[-1].data["Matrix_Unique_ID"]
    logging.info(f"    MLSMATRIX: Max Matrix_Unique_ID reached: {lastID}")
    if len(resultData) == 5000:
        return seedProperty(lastID, startDateTime)
    else:
        logging.info(f"    MLSMATRIX: Seed finished")

def update(timeDelta: datetime.timedelta) -> None:
    utcMinusFour = datetime.timezone(datetime.timedelta(0, -4*3600))
    oldestTimestamp = datetime.datetime.now(tz=utcMinusFour) - timeDelta
    return seedProperty(startDateTime=oldestTimestamp.isoformat().split('.')[0])

def main():
    update(datetime.timedelta(12))
    

if __name__=="__main__":
    main()
