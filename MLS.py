# Author: Andrew Pantera for TLCengine
# Last Updated: 7/8/2021
# Define MLS objects to be used in the Streamlit Housing Prices 
# Dashboard app. The MLS should provide functionality for returning 
# State, Counties, Cities, and Zip codes, as well as returning 
# listings based on queries for Counties, Cities, and Zip Codes.
# In the future, this might be used to inform ETL.py

import functools
import json
import os
import time
from datetime import datetime, timedelta
from typing import Callable, Sequence

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

cacheValidDays = 60 # Number of days before updating the cache

class MLS:
    def __init__(self, state: str, stateMLSName: str, fieldConversions: dict, database: str, collection: str, client: MongoClient, counties: Sequence = None, cities: Sequence = None, zips: Sequence = None, fixListings: Callable = None):
        """Construct an MLS object with all the functionality to return what State, Counties, Cities, and Zip Codes
            this MLS covers, as well as return standardized listings based on queries for County, City, or Zip Code

        Args:
            state (str): State. Only one MLS object can exist for each state because of caching
            stateMLSName (str): What the MLS has for the name of the state. Sometimes the abbreviation, sometimes the full state name, sometimes something weird like 'NY - New York'
            fieldConversions (dict): A dictionary keyed by MLS field names in RESO format, associated to values of MLS field names that this MLS uses.
            database (str): The name of the MongoDB database on TFS where the MLS data is stored
            collection (str): The name of the MongoDB collection in the database
            client (MongoClient): The MongoDB client pointing to the MongoDB database on TFS
            counties (Sequence, optional): Default list of counties to return
            cities (Sequence, optional): Default list of cities to return
            zips (Sequence, optional): Default list of zip codes to return
            fixListings (Callable, optional): A function that takes a Sequence of listings and returns the same Sequence of listings with fields filled in and erronius fields nullified, on an MLS specific basis
        """
        startTime = time.time()
        self.state = state
        self.stateMLSName = stateMLSName
        self.counties = counties
        self.cities = cities
        self.zips = zips
        self.fieldConversions = fieldConversions
        self.database = database
        self.collection = collection
        self.fixListings = fixListings
        self.client = client

        self.fieldConversionsReversed = {v: k for k, v in fieldConversions.items()}
        self.requestFields = tuple(set(fieldConversions.values())) #Convert to set first to cull diplicates
        self.checkCache()
        print(f"Initializing {self.state} took {round(time.time()-startTime, 5)} seconds.")

    def checkCache(self):
        # See if cache exists for this state and if it is recent enough to be valid , cacheValidDays = 60 days
        try:
            with open(f'{self.state}.json') as json_file:
                cache = json.load(json_file)
                if not cache.get("timestamp") or not datetime.fromtimestamp(cache.get("timestamp")) > (datetime.today() - timedelta(cacheValidDays)):
                    cache = None
        except FileNotFoundError:
            cache = None

        # Populate fields either from cache or database
        self.counties = self.counties if self.counties else cache.get("counties") if cache and "counties" in cache else self.getCounties()
        self.citiesCount = cache.get("citiesCount") if cache and "citiesCount" in cache else self.getCitiesCount()
        self.zips = self.zips if self.zips else cache.get("zips") if cache and "zips" in cache else self.getZipCodes()

        # Update Cache
        with open(f'{self.state}.json', 'w') as outfile:
            timestamp = cache.get("timestamp") if cache else datetime.today().timestamp()
            json.dump(
                {
                    'timestamp': timestamp,
                    'counties': self.counties,
                    'citiesCount': self.citiesCount,
                    'zips': self.zips
                },
                outfile
            )
        
    def getOnMarketDate(self, closeDate, daysOnMarket):
        # Much of the data is missing an OnMarketDate, but the DOM and Close dates are populated, so we can calculate the OnMarketDate by subtracting DOM from CloseDate 
        # I could not find a more dataframe native way to do this
        return pd.Series(map(
            lambda cd, dom: (datetime.strptime(str(cd), "%Y-%m-%d") - timedelta(days=int(dom))).strftime("%Y-%m-%d"),
                tuple(closeDate),
                tuple(daysOnMarket)
        ))

    def getListings(self, queryField: str, targetUnits: Sequence[str]) -> pd.DataFrame:
        startTime = time.time() # So that we can display the amount of time this method takes to run

        if queryField and targetUnits:
            filter = {
                self.fieldConversions["StateOrProvince"]: self.stateMLSName,
                self.fieldConversions[queryField]: {"$in": targetUnits}
            }

            # Get the listings from the database
            projection = dict(zip(self.requestFields, [1]*len(self.requestFields)))

            listings = pd.DataFrame(iter(self.client[self.database][self.collection].find(filter=filter, projection=projection)))
            if not listings.empty:
                # Translate columns to RESO format
                listings.rename(columns=self.fieldConversionsReversed, inplace=True)
                # Clean the listings (MLS non-specific)
                for field in tuple(self.fieldConversions):
                    if field not in listings:
                        listings[field] = None
                listings.loc[listings["OnMarketDate"].isin(("1800-01-01", "1900-01-01")), "OnMarketDate"] = pd.to_datetime(listings["CloseDate"]) - pd.to_timedelta(listings["DaysOnMarket"]) # CJMLS Specific but won't affect other MLSs
                listings.loc[listings["OffMarketDate"].isna(), "OffMarketDate"] = listings["CloseDate"] 
                listings.loc[listings["CloseDate"].isna(), "CloseDate"] = listings["OffMarketDate"]
                if "ExpirationDate" in listings:
                    listings.loc[listings["CloseDate"].isna(), "CloseDate"] = listings["ExpirationDate"] 
                listings.loc[lambda listings: listings["CloseDate"].isna() & ~listings["OnMarketDate"].isna() & ~listings["DaysOnMarket"].isna(), "CloseDate"] = pd.to_datetime(listings["OnMarketDate"], utc=True) + pd.to_timedelta(listings["DaysOnMarket"]) # Using utc=True is not correct for some of the MLSs, but for the streamlit dashboard this isn't important
                listings.loc[lambda listings: listings["OnMarketDate"].isna() & ~listings["CloseDate"].isna() & ~listings["DaysOnMarket"].isna(), "OnMarketDate"] = pd.to_datetime(listings["CloseDate"], utc=True) - pd.to_timedelta(listings["DaysOnMarket"])
                listings.loc[lambda listings: listings["DaysOnMarket"].isna() & ~listings["OnMarketDate"].isna() & ~listings["CloseDate"].isna(), "DaysOnMarket"] = pd.to_timedelta(pd.to_datetime(listings["CloseDate"], utc=True) - pd.to_datetime(listings["OnMarketDate"], utc=True)).dt.days
                listings.DaysOnMarket = listings.DaysOnMarket.astype(float)
                
                listings.loc[listings["BuildingAreaTotal"] == 0, "BuildingAreaTotal"] = np.nan # Replace 0 values with None
                if "ListPricePerSQFT" in listings:
                    listings.loc[lambda l: l["ListPricePerSQFT"].isna() & ~l["ListPrice"].isna() & ~l["BuildingAreaTotal"].isna(), "ListPricePerSQFT"] = listings["ListPrice"].astype('str').replace("None", "nan").astype('float') / listings["BuildingAreaTotal"].astype('str').replace("None", "nan").astype('float')
                else:
                    listings["ListPricePerSQFT"] = listings["ListPrice"].astype('str').replace("None", "nan").astype('float') / listings["BuildingAreaTotal"].astype('str').replace("None", "nan").astype('float')
                if "LotSizeSquareFeet" in listings:
                    listings.loc[lambda l: l["LotSizeSquareFeet"].isna() & ~l["BuildingAreaTotal"].isna() , "LotSizeSquareFeet"] = listings["BuildingAreaTotal"]
                else:   
                    listings["LotSizeSquareFeet"] = listings["BuildingAreaTotal"]
                if "BathroomsFull" in listings and "BathroomsHalf" in listings:
                    if "BathroomsTotalDecimal" in listings:
                        listings.loc[lambda l: l["BathroomsTotalDecimal"].isna() & ~l["BathroomsFull"].isna() & ~l["BathroomsHalf"].isna(), "BathroomsTotalDecimal"] = listings["BathroomsFull"] + .5 * listings["BathroomsHalf"]
                    else:
                        listings["BathroomsTotalDecimal"] = listings["BathroomsFull"] + .5 * listings["BathroomsHalf"]
                # Clean the listings (MLS specific)
                listings = self.fixListings(listings) if self.fixListings else listings
                
                # Fix field formatting
                if (listings is not None) and (not listings.empty):
                    # Make date fields datetimes
                    for field in ['OnMarketDate', 'CloseDate', 'OffMarketDate', 'YearBuilt']:
                        if field in listings.columns:
                            listings[field] = pd.to_datetime(listings[field], utc=True) # mabelo: added errors='coerce'
                            # mabelo edit 2024: astype does not work with datetime64, need to use tz_convert
                            #listings[field] = listings[field].astype('datetime64[ns]') # get rid of any timezone
                            listings[field] = listings[field].dt.tz_convert('UTC').dt.tz_localize(None)
                            listings[field].dt.tz_localize(None)
                    # Make Decimal128 fields floats
                    for field in ['ListPrice', 'ClosePrice', 'ListPricePerSQFT', 'Latitude', 'Longitude', 'LotSizeSquareFeet', 'OriginalListPrice', 'BuildingAreaTotal']:
                        if field in listings.columns:
                            listings[field] = listings[field].astype('str').replace("None", "nan").astype('float')
                print("Listings fetch took", time.time()-startTime, "seconds.")
            return listings

    def getCounties(self) -> Sequence[str]:
        return sorted(filter(bool, list(self.client[self.database][self.collection].distinct(
            self.fieldConversions["CountyOrParish"],
            {self.fieldConversions["StateOrProvince"]: self.stateMLSName}
        ))))

    def getCitiesCount(self) -> Sequence[dict]:
        # Returns a list of dictionaries representing the name of the city and the number of documents we have for that city in the form {'_id': 'Congerville', 'count': 12}
        citiesCount = tuple(self.client[self.database][self.collection].aggregate([
            {"$match" : {self.fieldConversions["StateOrProvince"]: self.stateMLSName}},
            {"$group" : {'_id':f"${self.fieldConversions['City']}", 'count':{"$sum":1}}}
        ]))
        return dict(functools.reduce(lambda agg, cityDict: agg + [(cityDict['_id'], cityDict['count']-1)], citiesCount, []))

    def getZipCodes(self) -> Sequence[str]:
        zipCodes = tuple(self.client[self.database][self.collection].distinct(
            self.fieldConversions["PostalCode"],
            {self.fieldConversions["StateOrProvince"]: self.stateMLSName}
        ))
        return sorted(list(filter(lambda x: x and len(x) == 5 and x.isnumeric(), zipCodes)))

class CaliforniaMLS(MLS):
    caCounties = {'San Bernardino': ['Adelanto', 'Apple Valley', 'Barstow', 'Big Bear Lake', 'Chino', 'Chino Hills', 'Colton', 'Fontana', 'Grand Terrace', 'Hesperia', 'Highland', 'Loma Linda', 'Montclair', 'Needles', 'Ontario', 'Rancho Cucamonga', 'Redlands', 'Rialto', 'San Bernardino', 'Twentynine Palms', 'Upland', 'Victorville', 'Yucaipa', 'Yucca Valley'], 'Los Angeles': ['Agoura Hills', 'Alhambra', 'Arcadia', 'Artesia', 'Avalon', 'Azusa', 'Baldwin Park', 'Bell', 'Bell Gardens', 'Bellflower', 'Beverly Hills', 'Bradbury', 'Burbank', 'Calabasas', 'Carson', 'Cerritos', 'Claremont', 'Commerce', 'Compton', 'Covina', 'Cudahy', 'Culver City', 'Diamond Bar', 'Downey', 'Duarte', 'El Monte', 'El Segundo', 'Gardena', 'Glendale', 'Glendora', 'Hawaiian Gardens', 'Hawthorne', 'Hermosa Beach', 'Hidden Hills', 'Huntington Park', 'Industry', 'Inglewood', 'Irwindale', 'La Cañada Flintridge', 'La Habra Heights', 'La Mirada', 'La Puente', 'La Verne', 'Lakewood', 'Lancaster', 'Lawndale', 'Lomita', 'Long Beach', 'Los Angeles', 'Lynwood', 'Malibu', 'Manhattan Beach', 'Maywood', 'Monrovia', 'Montebello', 'Monterey Park', 'Norwalk', 'Palmdale', 'Palos Verdes Estates', 'Paramount', 'Pasadena', 'Pico Rivera', 'Pomona', 'Rancho Palos Verdes', 'Redondo Beach', 'Rolling Hills', 'Rolling Hills Estates', 'Rosemead', 'San Dimas', 'San Fernando', 'San Gabriel', 'San Marino', 'Santa Clarita', 'Santa Fe Springs', 'Santa Monica', 'Sierra Madre', 'Signal Hill', 'South El Monte', 'South Gate', 'South Pasadena', 'Temple City', 'Torrance', 'Vernon', 'Walnut', 'West Covina', 'West Hollywood', 'Westlake Village', 'Whittier'], 'Alameda': ['Alameda', 'Albany', 'Berkeley', 'Dublin', 'Emeryville', 'Fremont', 'Hayward', 'Livermore', 'Newark', 'Oakland', 'Piedmont', 'Pleasanton', 'San Leandro', 'Union City'], 'Orange': ['Aliso Viejo', 'Anaheim', 'Brea', 'Buena Park', 'Costa Mesa', 'Cypress', 'Dana Point', 'Fountain Valley', 'Fullerton', 'Garden Grove', 'Huntington Beach', 'Irvine', 'La Habra', 'La Palma', 'Laguna Beach', 'Laguna Hills', 'Laguna Niguel', 'Laguna Woods', 'Lake Forest', 'Los Alamitos', 'Mission Viejo', 'Newport Beach', 'Orange', 'Placentia', 'Rancho Santa Margarita', 'San Clemente', 'San Juan Capistrano', 'Santa Ana', 'Seal Beach', 'Stanton', 'Tustin', 'Villa Park', 'Westminster', 'Yorba Linda'], 'Modoc': ['Alturas'], 'Amador': ['Amador City', 'Ione', 'Jackson', 'Plymouth', 'Sutter Creek'], 'Napa': ['American Canyon', 'Calistoga', 'Napa', 'St. Helena', 'Yountville'], 'Shasta': ['Anderson', 'Redding', 'Shasta Lake'], 'Calaveras': ['Angels Camp'], 'Contra Costa': ['Antioch', 'Brentwood', 'Clayton', 'Concord', 'Danville', 'El Cerrito', 'Hercules', 'Lafayette', 'Martinez', 'Moraga', 'Oakley', 'Orinda', 'Pinole', 'Pittsburg', 'Pleasant Hill', 'Richmond', 'San Pablo', 'San Ramon', 'Walnut Creek'], 'Humboldt': ['Arcata', 'Blue Lake', 'Eureka', 'Ferndale', 'Fortuna', 'Rio Dell', 'Trinidad'], 'San Luis Obispo': ['Arroyo Grande', 'Atascadero', 'Grover Beach', 'Morro Bay', 'Paso Robles', 'Pismo Beach', 'San Luis Obispo'], 'Kern': ['Arvin', 'Bakersfield', 'California City', 'Delano', 'Maricopa', 'McFarland', 'Ridgecrest', 'Shafter', 'Taft', 'Tehachapi', 'Wasco'], 'San Mateo': ['Atherton', 'Belmont', 'Brisbane', 'Burlingame', 'Colma', 'Daly City', 'East Palo Alto', 'Foster City', 'Half Moon Bay', 'Hillsborough', 'Menlo Park', 'Millbrae', 'Pacifica', 'Portola Valley', 'Redwood City', 'San Bruno', 'San Carlos', 'San Mateo', 'South San Francisco', 'Woodside'], 'Merced': ['Atwater', 'Dos Palos', 'Gustine', 'Livingston', 'Los Banos', 'Merced'], 'Placer': ['Auburn', 'Colfax', 'Lincoln', 'Loomis', 'Rocklin', 'Roseville'], 'Kings': ['Avenal', 'Corcoran', 'Hanford', 'Lemoore'], 'Riverside': ['Banning', 'Beaumont', 'Blythe', 'Calimesa', 'Canyon Lake', 'Cathedral City', 'Coachella', 'Corona', 'Desert Hot Springs', 'Eastvale', 'Hemet', 'Indian Wells', 'Indio', 'Jurupa Valley', 'La Quinta', 'Lake Elsinore', 'Menifee', 'Moreno Valley', 'Murrieta', 'Norco', 'Palm Desert', 'Palm Springs', 'Perris', 'Rancho Mirage', 'Riverside', 'San Jacinto', 'Temecula', 'Wildomar'], 'Marin': ['Belvedere', 'Corte Madera', 'Fairfax', 'Larkspur', 'Mill Valley', 'Novato', 'Ross', 'San Anselmo', 'San Rafael', 'Sausalito', 'Tiburon'], 'Solano': ['Benicia', 'Dixon', 'Fairfield', 'Rio Vista', 'Suisun City', 'Vacaville', 'Vallejo'], 'Butte': ['Biggs', 'Chico', 'Gridley', 'Oroville', 'Paradise'], 'Inyo': ['Bishop'], 'Imperial': ['Brawley', 'Calexico', 'Calipatria', 'El Centro', 'Holtville', 'Imperial', 'Westmorland'], 'Santa Barbara': ['Buellton', 'Carpinteria', 'Goleta', 'Guadalupe', 'Lompoc', 'Santa Barbara', 'Santa Maria', 'Solvang'], 'Ventura': ['Camarillo', 'Fillmore', 'Moorpark', 'Ojai', 'Oxnard', 'Port Hueneme', 'Santa Paula', 'Simi Valley', 'Thousand Oaks', 'Ventura'], 'Santa Clara': ['Campbell', 'Cupertino', 'Gilroy', 'Los Altos', 'Los Altos Hills', 'Los Gatos', 'Milpitas', 'Monte Sereno', 'Morgan Hill', 'Mountain View', 'Palo Alto', 'San Jose', 'Santa Clara', 'Saratoga', 'Sunnyvale'], 'Santa Cruz': ['Capitola', 'Santa Cruz', 'Scotts Valley', 'Watsonville'], 'San Diego': ['Carlsbad', 'Chula Vista', 'Coronado', 'Del Mar', 'El Cajon', 'Encinitas', 'Escondido', 'Imperial Beach', 'La Mesa', 'Lemon Grove', 'National City', 'Oceanside', 'Poway', 'San Diego', 'San Marcos', 'Santee', 'Solana Beach', 'Vista'], 'Monterey': ['Carmel-by-the-Sea', 'Del Rey Oaks', 'Gonzales', 'Greenfield', 'King City', 'Marina', 'Monterey', 'Pacific Grove', 'Salinas', 'Sand City', 'Seaside', 'Soledad'], 'Stanislaus': ['Ceres', 'Hughson', 'Modesto', 'Newman', 'Oakdale', 'Patterson', 'Riverbank', 'Turlock', 'Waterford'], 'Madera': ['Chowchilla', 'Madera'], 'Sacramento': ['Citrus Heights', 'Elk Grove', 'Folsom', 'Galt', 'Isleton', 'Rancho Cordova', 'Sacramento'], 'Lake': ['Clearlake', 'Lakeport'], 'Sonoma': ['Cloverdale', 'Cotati', 'Healdsburg', 'Petaluma', 'Rohnert Park', 'Santa Rosa', 'Sebastopol', 'Sonoma', 'Windsor'], 'Fresno': ['Clovis', 'Coalinga', 'Firebaugh', 'Fowler', 'Fresno', 'Huron', 'Kerman', 'Kingsburg', 'Mendota', 'Orange Cove', 'Parlier', 'Reedley', 'San Joaquin', 'Sanger', 'Selma'], 'Colusa': ['Colusa', 'Williams'], 'Tehama': ['Corning', 'Red Bluff', 'Tehama'], 'Del Norte': ['Crescent City'], 'Yolo': ['Davis', 'West Sacramento', 'Winters', 'Woodland'], 'Tulare': ['Dinuba', 'Exeter', 'Farmersville', 'Lindsay', 'Porterville', 'Tulare', 'Visalia', 'Woodlake'], 'Siskiyou': ['Dorris', 'Dunsmuir', 'Etna', 'Fort Jones', 'Montague', 'Mount Shasta', 'Tulelake', 'Weed', 'Yreka'], 'San Joaquin': ['Escalon', 'Lathrop', 'Lodi', 'Manteca', 'Ripon', 'Stockton', 'Tracy'], 'Mendocino': ['Fort Bragg', 'Point Arena', 'Ukiah', 'Willits'], 'Nevada': ['Grass Valley', 'Nevada City', 'Truckee'], 'San Benito': ['Hollister', 'San Juan Bautista'], 'Sutter': ['Live Oak', 'Yuba City'], 'Sierra': ['Loyalton'], 'Mono': ['Mammoth Lakes'], 'Yuba': ['Marysville', 'Wheatland'], 'Glenn': ['Orland', 'Willows'], 'El Dorado': ['Placerville', 'South Lake Tahoe'], 'Plumas': ['Portola'], 'San Francisco': ['San Francisco'], 'Tuolumne': ['Sonora'], 'Lassen': ['Susanville']}
    def getListings(self, queryField: str, targetUnits: Sequence[str]) -> pd.DataFrame:
        # If california is queried by county we just translate the counties to a sequence of cities within those counties, because the listings don't have a reliable county field
        startTime = time.time()
        if queryField == "CountyOrParish":
            cities = []
            for county in targetUnits:
                cities += self.caCounties[county]
            cities = tuple(map(
                lambda x: x.upper(),
                cities
            ))
            queryField = "City"
            targetUnits = cities
        return super().getListings(queryField, targetUnits)

def getMLSs() -> dict:
    MLSDict = {} # A dictionary of all the MLS objects to be used by streamlit
    load_dotenv(verbose=True) 
    mongoString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
    client = MongoClient(mongoString, connect=False)

    # CJMLS fields are already in RESO format
    MLSDict["New Jersey"] = MLS(
        "New Jersey", 
        'NJ',
        {
            "StateOrProvince" : "StateOrProvince",
            "CountyOrParish" : "CountyOrParish",
            "City" : "City",
            "PostalCode" : "PostalCode",
            "OnMarketDate" : "OnMarketDate", 
            "ClosePrice" : "ClosePrice", 
            "ListPrice" : "ListPrice", 
            "OriginalListPrice" : "OriginalListPrice", 
            "City" : "City", 
            "CountyOrParish": "CountyOrParish",
            "Latitude" : "Latitude", 
            "Longitude" : "Longitude", 
            "StandardStatus" : "StandardStatus", 
            "CloseDate" : "CloseDate",
            "OffMarketDate" : "OffMarketDate",
            "BedroomsTotal" : "BedroomsTotal", 
            "BathroomsTotalDecimal" : "BathroomsTotalDecimal", 
            "BuildingAreaTotal" : "BuildingAreaTotal",
            "ListPricePerSQFT" : "ListPricePerSQFT",
            "DaysOnMarket" : "DaysOnMarket", 
            "LotSizeSquareFeet" : "LotSizeSquareFeet", 
            "YearBuilt" : "YearBuilt", 
            "PropertyType" : "PropertyType"
        },
        'housing-prices',
        'bridge',
        client
    )

    MLSDict["Massachusetts"] = MLS(
        "Massachusetts", 
        'Massachusetts',
        {
            "StateOrProvince" : "STATE",
            "CountyOrParish" : "COUNTY",
            "City" : "TOWN",
            "PostalCode" : "ZIP_CODE",
            "OnMarketDate" : "LIST_DATE", 
            "ClosePrice" : "SALE_PRICE", 
            "ListPrice" : "LIST_PRICE", 
            "OriginalListPrice" : "ORIG_PRICE",
            "City" : "TOWN", 
            "CountyOrParish": "COUNTY",
            "Latitude" : "Latitude", 
            "Longitude" : "Longitude", 
            "StandardStatus" : "STATUS", 
            "CloseDate" : "SETTLED_DATE",
            "OffMarketDate" : "OFF_MKT_DATE",
            "BedroomsTotal" : "NO_BEDROOMS", 
            "BathroomsTotalDecimal" : "NO_FULL_BATHS", 
            "BuildingAreaTotal" : "SQUARE_FEET",
            "ListPricePerSQFT" : "LIST_PRICE_PER_SQFT",
            "DaysOnMarket" : "MARKET_TIME", 
            "LotSizeSquareFeet" : "LOT_SIZE", 
            "YearBuilt" : "YEAR_BUILT", 
            "PropertyType" : "PROP_TYPE"
        },
        'mlspin',
        'RESI',
        client
    )

    def fixListingsAcresToSqft(listings: pd.DataFrame) -> pd.DataFrame:
        # CTMLS, MLSMatrix, and Paragon don't have LotSizeSquareFeet, but instead has the a field for acres', that needs to be converted to square feet. 
        listings.LotSizeSquareFeet = listings.LotSizeSquareFeet.astype('str').replace("None", "nan").astype('float') * 43560
        return listings

    # CT. {CloseDate: 1, OffMarketDate: 1, Status: 1, DateContract: 1, OriginalEntryTimestamp: 1, ListingContractDate: 1, ClosePrice: 1, ListPrice: 1, LastListPrice: 1, OriginalListPrice: 1, City: 1, BedsTotal: 1, BathsTotal: 1, SqFtAvailable: 1, SqFtBusieness: 1, SqFtDescription: 1, SqFtResidential: 1, SqFtTotal: 1, Acres: 1}
    MLSDict["Connecticut"] = MLS(
        "Connecticut", 
        'Connecticut',
        {
            "StateOrProvince" : "StateOrProvince",
            "CountyOrParish" : "CountyOrParish",
            "City" : "City",
            "PostalCode" : "PostalCode",
            "OnMarketDate" : "ListingContractDate", #ListingContractDate, OriginalEntryTimestamp is sometimes after ListingContractDate
            "ClosePrice" : "ClosePrice", 
            "ListPrice" : "ListPrice", 
            "OriginalListPrice" : "OriginalListPrice",
            "City" : "City", 
            "CountyOrParish": "CountyOrParish",
            "Latitude" : "Latitude", 
            "Longitude" : "Longitude", 
            "StandardStatus" : "Status", 
            "CloseDate" : "CloseDate",
            "OffMarketDate" : "OffMarketDate", #DateContract, offMarketDate is usually after CloseDate
            "BedroomsTotal" : "BedsTotal", 
            "BathroomsTotalDecimal" : "BathsTotal", 
            "BuildingAreaTotal" : "SqFtTotal",
            "ListPricePerSQFT" : "RATIO_ListPrice_By_SqFt", # May need to divide this number by 100 to get it to match the rest of the data
            "DaysOnMarket" : "DOM", 
            "LotSizeSquareFeet" : "Acres", # Need to multiply by 43560 to get sqft
            "YearBuilt" : "YearBuilt", 
            "PropertyType" : "PropertyType", # Not formatted like the others: 0:"Condo/Co-Op For Sale", 1:"Multi-Family For Sale", 2:"Single Family For Sale", 3:"Lots and Land For Sale", 4:"Business For Sale", 5:"Residential Rental", 6:null, 7:"Commercial For Sale", 8:"Commercial For Lease"
            "LastListPrice" : "LastListPrice" #This is not present in NJ data, but we need it nonetheless
        },
        'ctmls',
        'Property',
        client,
        fixListings = fixListingsAcresToSqft
    )

    # IL. PurchaseContractDate, ListingContractDate, OriginalEntryTimestamp, StatusChangeTimestamp, ModificationTimestamp, PhotosChangeTimestamp, OriginatingSystemModificationTimestamp, MRD_LSZ, LivingArea
    MLSDict["Illinois"] = MLS(
        "Illinois", 
        'IL',
        { # zip is "PostalCode", City is "City", County is "CountyOrParish", state is "StateOrProvince"
            "StateOrProvince" : "StateOrProvince",
            "CountyOrParish" : "CountyOrParish",
            "City" : "City",
            "PostalCode" : "PostalCode",
            "OnMarketDate" : "OriginalEntryTimestamp", 
            "ClosePrice" : "ClosePrice", 
            "ListPrice" : "ListPrice", 
            "OriginalListPrice" : "OriginalListPrice",
            "City" : "City", 
            "CountyOrParish": "CountyOrParish",
            # "Latitude" : "Latitude", # MLSGRID doesnt have long or lat
            # "Longitude" : "Longitude",  
            "StandardStatus" : "MlsStatus", 
            "CloseDate" : "CloseDate",
            "OffMarketDate" : "OffMarketDate",
            "BedroomsTotal" : "BedroomsTotal", 
            "BathroomsTotalDecimal" : "BathroomsTotalInteger", 
            "BuildingAreaTotal" : "LivingArea", # This is in sqft, but if its not reported its just 0, not none. Need to filter those out. 
            # "ListPricePerSQFT" : "", Does not haev a listPricePerSQFT, need to populate
            "DaysOnMarket" : "DaysOnMarket", 
            "LotSizeSquareFeet" : "LotSizeAcres", # Need to multiply by 43560 to get sqft
            "YearBuilt" : "YearBuilt", 
            "PropertyType" : "PropertyType",
            "LastListPrice" : "PreviousListPrice", #This is not present in NJ data
        },
        'mlsgrid',
        'Property',
        client,
        fixListings = fixListingsAcresToSqft
    )

    # NY. Most values for MatrixModifiedDT are datetime.datetime(2016, 7, 26, 14, 34, 5, 137000), the exact same date and time, however, many listings have MatrixModifiedDT values later, up to current day. So it looks like that date in 2016 might be when the field was added
    # ExpirationDate, ListingContractDate, CurrentPrice, OriginalListPrice, PendingDate, ListPrice, LastListPrice, OriginalEntryTimestamp, LeasedPriceperSqFt, LeasedSquareFeet, SPLP, SPSqFt, LPSqFt
    MLSDict["New York"] = MLS(
        "New York", 
        'NY - New York',
        { # zip is "PostalCode", City is "City", County is "CountyOrParish", state is "StateOrProvince"
            "StateOrProvince" : "StateOrProvince",
            "CountyOrParish" : "CountyOrParish",
            "City" : "City",
            "PostalCode" : "PostalCode",
            "OnMarketDate" : "ListingContractDate",  # could also maybe use OriginalEntryTimestamp
            "ClosePrice" : "ClosePrice", 
            "ListPrice" : "ListPrice", # CurrentPrice also exists
            "OriginalListPrice" : "OriginalListPrice",
            "City" : "City", 
            "CountyOrParish": "CountyOrParish",
            "Latitude" : "Latitude", 
            "Longitude" : "Longitude", 
            "StandardStatus" : "Status", # a possible value is 'S-Closed/Rented', this could be a problem, because we filter out all listings that include the word 'rent', which could possibly filter out all closed listings.
            "CloseDate" : "CloseDate",
            "OffMarketDate" : "OffMarketDate",
            "BedroomsTotal" : "BedsTotal", 
            "BathroomsTotalDecimal" : "BathsTotal", 
            "BuildingAreaTotal" : "SqFtTotal",
            "ListPricePerSQFT" : "RATIO_ListPrice_By_SQFT", #RATIO_ClosePrice_By_SQFT, SPSqFt also exists, presumable Sale Proce per Square Foot
            "DaysOnMarket" : "DOM", 
            # "LotSizeSquareFeet" : "SqFtTotal", # I dont think this is correct for this field, but I can't find anything else that would work. Maybe LotDimensionsDepth x LotDimensionsFrontage?
            "YearBuilt" : "YearBuilt", 
            "PropertyType" : "PropertyType",
            "ExpirationDate": "ExpirationDate"
        },
        'mlsmatrix',
        'Property',
        client
    )


    caCounties = {'San Bernardino': ['Adelanto', 'Apple Valley', 'Barstow', 'Big Bear Lake', 'Chino', 'Chino Hills', 'Colton', 'Fontana', 'Grand Terrace', 'Hesperia', 'Highland', 'Loma Linda', 'Montclair', 'Needles', 'Ontario', 'Rancho Cucamonga', 'Redlands', 'Rialto', 'San Bernardino', 'Twentynine Palms', 'Upland', 'Victorville', 'Yucaipa', 'Yucca Valley'], 'Los Angeles': ['Agoura Hills', 'Alhambra', 'Arcadia', 'Artesia', 'Avalon', 'Azusa', 'Baldwin Park', 'Bell', 'Bell Gardens', 'Bellflower', 'Beverly Hills', 'Bradbury', 'Burbank', 'Calabasas', 'Carson', 'Cerritos', 'Claremont', 'Commerce', 'Compton', 'Covina', 'Cudahy', 'Culver City', 'Diamond Bar', 'Downey', 'Duarte', 'El Monte', 'El Segundo', 'Gardena', 'Glendale', 'Glendora', 'Hawaiian Gardens', 'Hawthorne', 'Hermosa Beach', 'Hidden Hills', 'Huntington Park', 'Industry', 'Inglewood', 'Irwindale', 'La Cañada Flintridge', 'La Habra Heights', 'La Mirada', 'La Puente', 'La Verne', 'Lakewood', 'Lancaster', 'Lawndale', 'Lomita', 'Long Beach', 'Los Angeles', 'Lynwood', 'Malibu', 'Manhattan Beach', 'Maywood', 'Monrovia', 'Montebello', 'Monterey Park', 'Norwalk', 'Palmdale', 'Palos Verdes Estates', 'Paramount', 'Pasadena', 'Pico Rivera', 'Pomona', 'Rancho Palos Verdes', 'Redondo Beach', 'Rolling Hills', 'Rolling Hills Estates', 'Rosemead', 'San Dimas', 'San Fernando', 'San Gabriel', 'San Marino', 'Santa Clarita', 'Santa Fe Springs', 'Santa Monica', 'Sierra Madre', 'Signal Hill', 'South El Monte', 'South Gate', 'South Pasadena', 'Temple City', 'Torrance', 'Vernon', 'Walnut', 'West Covina', 'West Hollywood', 'Westlake Village', 'Whittier'], 'Alameda': ['Alameda', 'Albany', 'Berkeley', 'Dublin', 'Emeryville', 'Fremont', 'Hayward', 'Livermore', 'Newark', 'Oakland', 'Piedmont', 'Pleasanton', 'San Leandro', 'Union City'], 'Orange': ['Aliso Viejo', 'Anaheim', 'Brea', 'Buena Park', 'Costa Mesa', 'Cypress', 'Dana Point', 'Fountain Valley', 'Fullerton', 'Garden Grove', 'Huntington Beach', 'Irvine', 'La Habra', 'La Palma', 'Laguna Beach', 'Laguna Hills', 'Laguna Niguel', 'Laguna Woods', 'Lake Forest', 'Los Alamitos', 'Mission Viejo', 'Newport Beach', 'Orange', 'Placentia', 'Rancho Santa Margarita', 'San Clemente', 'San Juan Capistrano', 'Santa Ana', 'Seal Beach', 'Stanton', 'Tustin', 'Villa Park', 'Westminster', 'Yorba Linda'], 'Modoc': ['Alturas'], 'Amador': ['Amador City', 'Ione', 'Jackson', 'Plymouth', 'Sutter Creek'], 'Napa': ['American Canyon', 'Calistoga', 'Napa', 'St. Helena', 'Yountville'], 'Shasta': ['Anderson', 'Redding', 'Shasta Lake'], 'Calaveras': ['Angels Camp'], 'Contra Costa': ['Antioch', 'Brentwood', 'Clayton', 'Concord', 'Danville', 'El Cerrito', 'Hercules', 'Lafayette', 'Martinez', 'Moraga', 'Oakley', 'Orinda', 'Pinole', 'Pittsburg', 'Pleasant Hill', 'Richmond', 'San Pablo', 'San Ramon', 'Walnut Creek'], 'Humboldt': ['Arcata', 'Blue Lake', 'Eureka', 'Ferndale', 'Fortuna', 'Rio Dell', 'Trinidad'], 'San Luis Obispo': ['Arroyo Grande', 'Atascadero', 'Grover Beach', 'Morro Bay', 'Paso Robles', 'Pismo Beach', 'San Luis Obispo'], 'Kern': ['Arvin', 'Bakersfield', 'California City', 'Delano', 'Maricopa', 'McFarland', 'Ridgecrest', 'Shafter', 'Taft', 'Tehachapi', 'Wasco'], 'San Mateo': ['Atherton', 'Belmont', 'Brisbane', 'Burlingame', 'Colma', 'Daly City', 'East Palo Alto', 'Foster City', 'Half Moon Bay', 'Hillsborough', 'Menlo Park', 'Millbrae', 'Pacifica', 'Portola Valley', 'Redwood City', 'San Bruno', 'San Carlos', 'San Mateo', 'South San Francisco', 'Woodside'], 'Merced': ['Atwater', 'Dos Palos', 'Gustine', 'Livingston', 'Los Banos', 'Merced'], 'Placer': ['Auburn', 'Colfax', 'Lincoln', 'Loomis', 'Rocklin', 'Roseville'], 'Kings': ['Avenal', 'Corcoran', 'Hanford', 'Lemoore'], 'Riverside': ['Banning', 'Beaumont', 'Blythe', 'Calimesa', 'Canyon Lake', 'Cathedral City', 'Coachella', 'Corona', 'Desert Hot Springs', 'Eastvale', 'Hemet', 'Indian Wells', 'Indio', 'Jurupa Valley', 'La Quinta', 'Lake Elsinore', 'Menifee', 'Moreno Valley', 'Murrieta', 'Norco', 'Palm Desert', 'Palm Springs', 'Perris', 'Rancho Mirage', 'Riverside', 'San Jacinto', 'Temecula', 'Wildomar'], 'Marin': ['Belvedere', 'Corte Madera', 'Fairfax', 'Larkspur', 'Mill Valley', 'Novato', 'Ross', 'San Anselmo', 'San Rafael', 'Sausalito', 'Tiburon'], 'Solano': ['Benicia', 'Dixon', 'Fairfield', 'Rio Vista', 'Suisun City', 'Vacaville', 'Vallejo'], 'Butte': ['Biggs', 'Chico', 'Gridley', 'Oroville', 'Paradise'], 'Inyo': ['Bishop'], 'Imperial': ['Brawley', 'Calexico', 'Calipatria', 'El Centro', 'Holtville', 'Imperial', 'Westmorland'], 'Santa Barbara': ['Buellton', 'Carpinteria', 'Goleta', 'Guadalupe', 'Lompoc', 'Santa Barbara', 'Santa Maria', 'Solvang'], 'Ventura': ['Camarillo', 'Fillmore', 'Moorpark', 'Ojai', 'Oxnard', 'Port Hueneme', 'Santa Paula', 'Simi Valley', 'Thousand Oaks', 'Ventura'], 'Santa Clara': ['Campbell', 'Cupertino', 'Gilroy', 'Los Altos', 'Los Altos Hills', 'Los Gatos', 'Milpitas', 'Monte Sereno', 'Morgan Hill', 'Mountain View', 'Palo Alto', 'San Jose', 'Santa Clara', 'Saratoga', 'Sunnyvale'], 'Santa Cruz': ['Capitola', 'Santa Cruz', 'Scotts Valley', 'Watsonville'], 'San Diego': ['Carlsbad', 'Chula Vista', 'Coronado', 'Del Mar', 'El Cajon', 'Encinitas', 'Escondido', 'Imperial Beach', 'La Mesa', 'Lemon Grove', 'National City', 'Oceanside', 'Poway', 'San Diego', 'San Marcos', 'Santee', 'Solana Beach', 'Vista'], 'Monterey': ['Carmel-by-the-Sea', 'Del Rey Oaks', 'Gonzales', 'Greenfield', 'King City', 'Marina', 'Monterey', 'Pacific Grove', 'Salinas', 'Sand City', 'Seaside', 'Soledad'], 'Stanislaus': ['Ceres', 'Hughson', 'Modesto', 'Newman', 'Oakdale', 'Patterson', 'Riverbank', 'Turlock', 'Waterford'], 'Madera': ['Chowchilla', 'Madera'], 'Sacramento': ['Citrus Heights', 'Elk Grove', 'Folsom', 'Galt', 'Isleton', 'Rancho Cordova', 'Sacramento'], 'Lake': ['Clearlake', 'Lakeport'], 'Sonoma': ['Cloverdale', 'Cotati', 'Healdsburg', 'Petaluma', 'Rohnert Park', 'Santa Rosa', 'Sebastopol', 'Sonoma', 'Windsor'], 'Fresno': ['Clovis', 'Coalinga', 'Firebaugh', 'Fowler', 'Fresno', 'Huron', 'Kerman', 'Kingsburg', 'Mendota', 'Orange Cove', 'Parlier', 'Reedley', 'San Joaquin', 'Sanger', 'Selma'], 'Colusa': ['Colusa', 'Williams'], 'Tehama': ['Corning', 'Red Bluff', 'Tehama'], 'Del Norte': ['Crescent City'], 'Yolo': ['Davis', 'West Sacramento', 'Winters', 'Woodland'], 'Tulare': ['Dinuba', 'Exeter', 'Farmersville', 'Lindsay', 'Porterville', 'Tulare', 'Visalia', 'Woodlake'], 'Siskiyou': ['Dorris', 'Dunsmuir', 'Etna', 'Fort Jones', 'Montague', 'Mount Shasta', 'Tulelake', 'Weed', 'Yreka'], 'San Joaquin': ['Escalon', 'Lathrop', 'Lodi', 'Manteca', 'Ripon', 'Stockton', 'Tracy'], 'Mendocino': ['Fort Bragg', 'Point Arena', 'Ukiah', 'Willits'], 'Nevada': ['Grass Valley', 'Nevada City', 'Truckee'], 'San Benito': ['Hollister', 'San Juan Bautista'], 'Sutter': ['Live Oak', 'Yuba City'], 'Sierra': ['Loyalton'], 'Mono': ['Mammoth Lakes'], 'Yuba': ['Marysville', 'Wheatland'], 'Glenn': ['Orland', 'Willows'], 'El Dorado': ['Placerville', 'South Lake Tahoe'], 'Plumas': ['Portola'], 'San Francisco': ['San Francisco'], 'Tuolumne': ['Sonora'], 'Lassen': ['Susanville']}

    # L_ContractDate, L_SystemPrice, L_UpdateDate
    # County could be LM_Char10_11, LM_char10_49, or LO1_board_id. L_Area could also contain county information
    # LFD_BATHNONPRMYINCLUDE_4: this is an array: 
        # 0:"Shower Over Tub" 1:"Solid Surface" 2:"Stall Shower"...
    # LFD_BATHPRIMARYINCLUDES_3: This is also an array:
        # 0:"Solid Surface" 1:"Stall Shower" ...2:"Tile"
    # bedrooms: L_Keyword2
    # total rooms: L_Keyword1 
    # full baths: L_Keyword3
    # half baths is L_Keyword4
    # 40760898
    # LM_Int4_12 is ppsqft sold
    # LM_Int4_15 is ppsqft listed
    MLSDict["California"] = CaliforniaMLS(
        "California", 
        'CA',
        { # zip is "L_Zip", City is "L_City", County is "", state is "L_State", example: "CA"
            "StateOrProvince" : "L_State",
            "CountyOrParish" : "L_Area",
            "City" : "L_City",
            "PostalCode" : "L_Zip",
            "OnMarketDate" : "L_ListingDate", 
            "ClosePrice" : "L_SoldPrice", 
            "ListPrice" : "L_AskingPrice", 
            "OriginalListPrice" : "L_OriginalPrice",
            "City" : "L_City", 
            "CountyOrParish": "L_Area",
            "StandardStatus" : "L_Status", 
            "CloseDate" : "L_ClosingDate",
            "OffMarketDate" : "L_OffMarketDate",
            "BedroomsTotal" : "L_Keyword2", 
            # "BathroomsTotalDecimal" : "", # full baths: L_Keyword3, half baths is L_Keyword4
            "BathroomsFull": "L_Keyword3",
            "BathroomsHalf": "L_Keyword4",
            "BuildingAreaTotal" : "L_SquareFeet",
            "ListPricePerSQFT" : "LM_Int4_15", # need to remove manual calculation now that i have the field
            "DaysOnMarket" : "L_DOM", 
            "LotSizeSquareFeet" : "L_NumAcres", # Need to multiply by 43560 to get sqft
            "YearBuilt" : "LM_Int4_7", 
            "PropertyType" : "L_Class"
        },
        'paragon',
        'Property',
        client,
        counties = tuple(caCounties),
        fixListings = fixListingsAcresToSqft
    )

    def fixListingsDimensionsToSqft(listings: pd.DataFrame) -> pd.DataFrame:
        # REBNY stores dimensions in two fields, LivingArea, and LivingAreaUnits. LivingArea is an int, and LivingAreaUnits is a string that so far i've found to only be 'SquareFeet', so ill just use LivingArea as BuildingAreaTotal until i see that change, at which time i will need this function
        # There is also BuildingSizeDimensions
        pass

    MLSDict["NYC"] = MLS(
        "New York City", 
        'NY',
        {
            "StateOrProvince" : "StateOrProvince",
            "CountyOrParish" : "CountyOrParish",
            "City" : "City",
            "PostalCode" : "PostalCode",
            "OnMarketDate" : "ListingContractDate", # OriginalEntryTimestamp
            "ClosePrice" : "ClosePrice", 
            "ListPrice" : "ListPrice", 
            "OriginalListPrice" : "OriginalListPrice", 
            "City" : "City", 
            "CountyOrParish": "CountyOrParish",
            "Latitude" : "Latitude", 
            "Longitude" : "Longitude", 
            "StandardStatus" : "StandardStatus", # 'Withdrawn' is a possible status

            "CloseDate" : "PurchaseContractDate", 
            # "OffMarketDate" : "OffMarketTimestamp",
            "OffMarketDate" : "PurchaseContractDate",
            "BedroomsTotal" : "BedroomsTotal", 
            "BathroomsTotalDecimal" : "BathroomsTotal", 
            "BuildingAreaTotal" : "LivingArea", # BuildingSizeDimensions, LivingAreaUnits
            # "ListPricePerSQFT" : "",
            "DaysOnMarket" : "DaysOnMarket", 
            # "LotSizeSquareFeet" : "", 
            "YearBuilt" : "YearBuilt", 
            "PropertyType" : "PropertyType"
        },
        'rebny',
        'Property',
        client
    )

    return MLSDict

if __name__ == "__main__":
    getMLSs()
