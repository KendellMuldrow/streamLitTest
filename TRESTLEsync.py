import cachetools
import json
import rets
import time
import datetime
from dotenv import load_dotenv
import logging
import os
import requests
import schedule
from pymongo import MongoClient, UpdateOne, ReplaceOne
import urllib3
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

data_uri = "https://api-trestle.corelogic.com/trestle/odata/Property"
load_dotenv(verbose=True)
mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') 
client = MongoClient(mongoConnectionString)
db = client["housing-prices"]
Collection = db["trestle"]

listings_uploaded = 0

def getAccessToken():
    # POST to trestle with credentials to retrieve Access Token
    c = BackendApplicationClient(client_id=os.getenv("TRESTLE_USERNAME"))
    session = OAuth2Session(client=c)
    session.fetch_token(
                    token_url='https://api-trestle.corelogic.com/trestle/oidc/connect/token', 
                    client_id=os.getenv("TRESTLE_USERNAME"),
                    client_secret=os.getenv("TRESTLE_PASSWD"),
                    scope='api')
    return session

def uploadListings(listings):
    
    bulkRequests = []
    for doc in listings:
        bulkRequests.append(
            ReplaceOne(
                {"ListingKeyNumeric": doc["ListingKeyNumeric"]}, 
                doc,
                upsert=True
            )
        )
    replacementResult = Collection.bulk_write(bulkRequests, ordered=False)
    latestTime = listings[-1]['ModificationTimestamp']
    logging.info(f'    TRESTLE: Inserted: {replacementResult.inserted_count}, upserted: {replacementResult.upserted_count}, modified: {replacementResult.modified_count} listings for skip {skip} with most recent time {latestTime}')
    return latestTime

def getRequest(session, query):
    # A wrapper around requests.get in order to catch errors
    try:
        return session.get(query)
    except (urllib3.exceptions.MaxRetryError, requests.exceptions.ConnectionError):
        logging.info(f"Max retries exceeded for query {query}. Waiting 10 seconds and trying again...")
        time.sleep(10)
        return session.get(query)

def update(timeDelta):
    seedTrestle(None, timeDelta)
    
    ## TODO: Fix oldestTimestamp not filtering out correctly
def seedTrestle(skip=None, timeDelta: datetime.timedelta=None,  oldestTimestamp="2000-07-29T02:25:16.000Z"):
    """Request listings from Trestle Analytics RESO API and upload those 
    listings to the 'trestle' collection in the 'housing-prices' database in 
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

    session = getAccessToken()
    str = f"{data_uri}"
    str += '?$top=1000'
    str += f'&&$filter=ModificationTimestamp ge {oldestTimestamp}&replication=true'
    

    # Make the first API call
    result = session.get(str)
    resJson = result.json()
    
    
    
    if not resJson:
        logging.info("    TRESTLE: Query did not return json:")
        logging.info(session)
        return None
    elif ('value' in resJson and resJson['value'] == []) or 'value' not in resJson:
        logging.info("    TRESTLE: Query returned no listings")
        logging.info(session)
        return None
    while resJson and 'value' in resJson:
        if session.token: # Check if token expired
            oldestTimestamp = uploadListings(resJson['value'])
            if '@odata.nextLink' in resJson:
                result = session.get(resJson['@odata.nextLink'])
                logging.info(f"Next Request Link: {resJson['@odata.nextLink']}")
                resJson = result.json()
            else:
                resJson = None
        else: # Access Token Expired: Get token and call next
            session = getAccessToken()
            result = session.get(resJson['@odata.nextLink'])
            resJson = result.json()
    logging.info(f"    TRESTLE: Sync finished. Skip: {skip}, oldestTimestamp: {oldestTimestamp}, final status code: {result.status_code}")
    if not result.ok:
        logging.info(f"    TRESTLE: Final API request failed. content: {result.content}")


if __name__ == "__main__":
    update()
