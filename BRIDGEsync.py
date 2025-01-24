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

load_dotenv(verbose=True)
mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') 
client = MongoClient(mongoConnectionString)
db = client["housing-prices"]
Collection = db["bridge"]

def uploadListings(listings, skip):
    bulkRequests = []
    for doc in listings:
        doc.pop("@odata.context")
        doc.pop("@odata.id")
        bulkRequests.append(
            ReplaceOne(
                {"ListingKeyNumeric": doc["ListingKeyNumeric"]}, 
                doc,
                upsert=True
            )
        )
    replacementResult = Collection.bulk_write(bulkRequests, ordered=False)
    latestTime = listings[-1]['ModificationTimestamp']
    logging.info(f'    BRIDGE: Inserted: {replacementResult.inserted_count}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings for skip {skip} with most recent time {latestTime}')
    return latestTime

def getRequest(query):
    # A wrapper around requests.get in order to catch errors
    try:
        return requests.get(query)
    except (urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError):
        logging.info(f"Max retries exceeded for query {query}. Waiting 10 seconds and trying again...")
        time.sleep(10)
        return requests.get(query)

def seedBridge(skip=None, oldestTimestamp="2005-07-29T02:25:16.000Z"):
    """Request listings from Bridge Analytics RETS API and upload those 
    listings to the 'bridge' collection in the 'housing-prices' database in 
    MongoDB on TFS.

    Args:
        skip ([int], optional): The number of API calls to skip, used to continue
            execution after failure. Defaults to None.
        oldestTimestamp ([string], optional): ISO format. The oldest timestamp to 
            request listings from. Defaults to "2005-07-29T02:25:16.000Z", the 
            modification timestamp of the oldest listing the API has, this seeds
            every listing from the API

    Returns:
        None
    """
    # Assemble the first API call
    first = f'https://api.bridgedataoutput.com/api/v2/OData/jerseymls/Property?access_token={os.getenv("BRIDGE_ACCESS_TOKEN")}&$orderby=ModificationTimestamp'
    if skip:
        first += f'&$skip={skip}'
    else:
        skip = 0
    first += f'&$filter=ModificationTimestamp ge {oldestTimestamp}'
    first += '&$top=200'

    # Make the first API call
    result = getRequest(first)
    resJson = result.json()
    if not resJson:
        logging.info("    BRIDGE: Query did not return json:")
        logging.info(result.content)
        return None
    elif ('value' in resJson and resJson['value'] == []) or 'value' not in resJson:
        logging.info("    BRIDGE: Query returned no listings")
        logging.info(result.content)
        return None
    while resJson and 'value' in resJson:
        oldestTimestamp = uploadListings(resJson['value'], skip)
        if '@odata.nextLink' in resJson:
            # The subsequent API calls will be provided by the API 
            skip = resJson['@odata.nextLink'].split("skip=")[1].split("&")[0]
            if int(skip) >= 10000:
                # The API won't let us skip more than 10k listings, we need to start again, querying by modification timestamp to get to where we were, because we're ordering our query by modification timestamp
                return seedBridge(oldestTimestamp=oldestTimestamp)
            result = getRequest(resJson['@odata.nextLink'])
            resJson = result.json()
        else:
            resJson = None
    logging.info(f"    BRIDGE: Sync finished. Skip: {skip}, oldestTimestamp: {oldestTimestamp}, final status code: {result.status_code}")
    if not result.ok:
        logging.info(f"    BRIDGE: Final API request failed. content: {result.content}")

def update(timeDelta: datetime.timedelta) -> None:
    # BridgeAnalytics API uses the Zulu timezone, UTC zero.
    zulu = datetime.timezone(datetime.timedelta(0))
    oldestTimestamp = (datetime.datetime.now(tz=zulu) - timeDelta).isoformat().split('.')[0]
    return seedBridge(oldestTimestamp=oldestTimestamp)

if __name__ == "__main__":
    update(datetime.timedelta(0, 120))
    