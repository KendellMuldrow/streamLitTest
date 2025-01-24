import time
from bson.objectid import ObjectId
import datetime
from dotenv import load_dotenv
import logging
import os
import requests
import schedule
from pymongo import MongoClient, UpdateOne, ReplaceOne
import urllib3
from pprint import pprint

load_dotenv(verbose=True)
mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') 
mongoClient = MongoClient(mongoConnectionString)
Collection = mongoClient["rebny"]["Property"]

def uploadListings(listings, skip):
    bulkRequests = []
    for doc in listings:
        doc.pop("@odata.context")
        doc.pop("@odata.id")
        bulkRequests.append(
            ReplaceOne(
                {"ListingKey": doc["ListingKey"]}, 
                doc,
                upsert=True
            )
        )
    replacementResult = Collection.bulk_write(bulkRequests, ordered=False)
    latestTime = listings[-1]['ModificationTimestamp']
    logging.info(f'    REBNY: Inserted: {replacementResult.inserted_count}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings for skip {skip} with most recent time {latestTime}')
    return latestTime

def getRequest(query, headers=None):
    # A wrapper around requests.get in order to catch errors
    try:
        response = requests.get(query, headers=headers)
    except (urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError):
        logging.info(f"Max retries exceeded for query {query}. Waiting 10 seconds and trying again...")
        time.sleep(10)
        return requests.get(query, headers=headers)
    if not response.ok:
        logging.warning(f"Response code {response.status_code} for query {query}. Waiting 10 seconds and trying again...")
        time.sleep(10)
        return requests.get(query, headers=headers)
    return response

def seedREBNY(skip=None, oldestTimestamp="2000-07-29T02:25:16.000Z"):
    # sourcery skip: extract-duplicate-method
    # "2021-02-10T01:02:14.848Z" was the oldest timestamp i got in a query for 200 sorting by timestamp, so it looks like you cant sort by timestamp
    """Request listings from REBNY Analytics RETS API and upload those 
    listings to the 'Property' collection in the 'rebny' database in 
    MongoDB on TFS.

    Args:
        skip ([int], optional): The number of API calls to skip, used to continue
            execution after failure. Defaults to None.
        oldestTimestamp ([string], optional): ISO format. The oldest timestamp to 
            request listings from. Defaults to "2000-07-29T02:25:16.000Z", the 
            modification timestamp of the oldest listing the API has, this seeds
            every listing from the API

    Returns:
        None
    """
    # Assemble the first API call
    first = f'https://rls.perchwell.com/api/v1/OData/rebny/Property?$orderby=ModificationTimestamp' # It doesnt look like you can orderby according to https://rls-docs.perchwell.com/
    headers={'Authorization': "Bearer " + os.getenv("REBNY_TOKEN") }
    if skip:
        first += f'&$skip={skip}'
    else:
        skip = 0
    first += f'&$filter=ModificationTimestamp ge {oldestTimestamp}'
    first += '&$top=200'
    # Make the first API call
    result = getRequest(first, headers)
    resJson = result.json()
    if not resJson:
        logging.info("    REBNY: Query did not return json:")
        logging.info(result.content)
        return None
    elif 'value' not in resJson or (resJson['value'] == []):
        logging.info("    REBNY: Query returned no listings")
        logging.info(result.content)
        return None
    while resJson and 'value' in resJson:
        # oldestTimestamp = uploadListings(resJson['value'], skip)
        uploadListings(resJson['value'], skip)
        if '@odata.nextLink' in resJson:
            # The subsequent API calls will be provided by the API 
            skip = resJson['@odata.nextLink'].split("skip=")[1].split("&")[0]
            result = getRequest(resJson['@odata.nextLink'], headers)
            resJson = result.json()
        else:
            resJson = None
    logging.info(f"    REBNY: Sync finished. Skip: {skip}, oldestTimestamp: {oldestTimestamp}, final status code: {result.status_code}")
    if not result.ok:
        logging.info(f"    REBNY: Final API request failed. content: {result.content}")

def update(timeDelta: datetime.timedelta) -> None:
    # REBNYAnalytics API uses the Zulu timezone, UTC zero.
    zulu = datetime.timezone(datetime.timedelta(0))
    oldestTimestamp = (datetime.datetime.now(tz=zulu) - timeDelta).isoformat().split('.')[0]
    return seedREBNY(oldestTimestamp=oldestTimestamp)

if __name__ == "__main__":
    # update(datetime.timedelta(0, 120))
    logging.basicConfig(filename='./rebny_'+time.strftime("%b-%d-%Y")+'.log',
                            level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

    seedREBNY(skip=282000)
    