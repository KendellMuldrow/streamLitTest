#%%

import _thread
import altair as alt
from collections import OrderedDict
from datetime import datetime, timedelta
from dotenv import load_dotenv
import functools
import io
import numpy as np
import os
import pandas as pd
import pydeck as pdk
import pymongo
from pymongo import MongoClient
import statistics
# import streamlit as st
import json
from pprint import pprint
from tqdm import tqdm
import traceback
from scipy import stats
import altair as alt
import requests


load_dotenv(verbose=True) 

#%%
mongoConnectionString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') 
mongoClient = MongoClient(mongoConnectionString)
Collection = mongoClient["rebny"]["Property"]

#%%
print(
    Collection.distinct('LivingAreaUnits')
)
#%%
headers={'Authorization': "Bearer " + os.getenv("REBNY_TOKEN") }
oldestTimestamp="2000-07-29T02:25:16.000Z"
skip=None
first = f'https://rls.perchwell.com/api/v1/OData/rebny/Property?$orderby=ModificationTimestamp'
headers={'Authorization': "Bearer " + os.getenv("REBNY_TOKEN") }
if skip:
    first += f'&$skip={skip}'
else:
    skip = 0
first += f'&$filter=ModificationTimestamp ge {oldestTimestamp}'
first += '&$top=200'
response = requests.get(
    first, 
    headers=headers
)

#%%
rj = response.json()
print(list(rj))
print(rj['@odata.nextLink'])

#%%
print(response)
print(response.headers)
print(response.content)

# from vega_datasets import data

# source = data.cars()

# line = alt.Chart(source).mark_line().encode(
#     x='Year',
#     y='mean(Miles_per_Gallon)'
# )

# band = alt.Chart(source).mark_errorband(extent='ci').encode(
#     x='Year',
#     y=alt.Y('Miles_per_Gallon', title='Miles/Gallon'),
# )

# band + line



# mongoString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
# client = MongoClient(mongoString)

# headers = {"Authorization": "Bearer " + os.getenv("MLSGRID_TOKEN")} 

# filter = {'StateOrProvince': 'NY - New York', 'City': {'$in': ['Adams']}}

# projection = {'CountyOrParish': 1, 'Longitude': 1, 'City': 1, '': 1, 'Status': 1, 'CloseDate': 1, 'SqFtTotal': 1, 'YearBuilt': 1, 'BathsTotal': 1, 'PropertyType': 1, 'ListingContractDate': 1, 'OffMarketDate': 1, 'Latitude': 1, 'ClosePrice': 1, 'OriginalListPrice': 1, 'DOM': 1, 'ListPrice': 1, 'RATIO_ListPrice_By_SQFT': 1}

# print(list(client['mlsmatrix']['Property'].find(filter=filter, projection=projection)))

# print( tuple(client['paragon']['Property'].aggregate([
#     {"$match" : {"L_State": "CA"}},
#     {"$group" : {'_id':"$L_Status", 'count':{"$sum":1}}}
# ])) )


# method = ["IQRs", "Standard Deviations"][1]
# tails = ["Both", "Low Outliers", "High Outliers"][0]
# field = "field"
# # The z score is a measure of hoe many standard deviations below or above the population mean a raw score is
# limit = 1.5 if method == "IQRs" else 2.0
# filteredListings = pd.DataFrame(np.array(list(map(lambda x: [x], [1,50,50,50,50,50,50,50,100]))), columns=['field'])  

# print(filteredListings)
# print(stats.zscore(filteredListings[field]))

# filteredListings[field] = filteredListings[field].dropna()


# iqr = stats.iqr(filteredListings[field].dropna())
# q25, q75 = np.percentile(filteredListings[field].dropna(), 25), np.percentile(filteredListings[field].dropna(), 75)
# lower, upper = q25 - 1.5*iqr, q75 + 1.5*iqr
# filteredListings = filteredListings.loc[
#     {
#         "Both": (lambda fl: (fl[field] > lower) & (fl[field] < upper)) if method == 'IQRs' else 
#             (lambda fl: ((stats.zscore(fl[field]) > -limit) & (stats.zscore(fl[field]) < limit))), 
#         "Low Outliers": (filteredListings[field] > lower) if method == 'IQRs' else 
#             (stats.zscore(filteredListings[field]) > -limit), 
#         "High Outliers": (filteredListings[field] < upper) if method == 'IQRs' else 
#             (stats.zscore(filteredListings[field]) < limit)
#     }[tails]
# ]

# filteredListings = filteredListings.loc[{"Both": (lambda fl: (fl[field] > lower) & (fl[field] < upper)) if method == 'IQRs' else (lambda fl: ((stats.zscore(fl[field]) > -limit) & (stats.zscore(fl[field]) < limit))), "Low Outliers": (filteredListings[field] > lower) if method == 'IQRs' else (stats.zscore(filteredListings[field]) > -limit), "High Outliers": (filteredListings[field] < upper) if method == 'IQRs' else (stats.zscore(filteredListings[field]) < limit)}[tails]]

# print(filteredListings)

# def b():
#     print(2)

# class testClass:
#     def __init__(self, a, b):
#         print(a)
#         print(b)
#     def a(self, param):
#         print("superclass", param)

# class t2(testClass):
#     value = "abcd"
#     def a(self, param):
#         print(self.value)
#         print("subclass", param)
#         super().a(param)

# testObject = t2(1, 2)
# testObject.a("param")
# print(t2.value)

# response = requests.get(os.getenv("MLSGRID_URL"), auth=os.getenv("MLSGRID_TOKEN"))

# payload = {
#     "$filter": "OriginatingSystemName eq 'mred' and ModificationTimestamp gt 2021-12-30T23:59:59.99Z",
#     "$expand": "PropertyUnitTypes",
#     "$skip": "0",
# }

# response = requests.get(os.getenv("MLSGRID_URL"), headers=headers, params=urllib.parse.urlencode(payload))

# https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27actris%27%20and%20ModificationTimestamp%20gt%202020-12-30T23:59:59.99Z&$expand=Media,PropertyRooms,PropertyUnitTypes
# OriginatingSystemName eq 'actris' and ModificationTimestamp gt 2020-12-30T23:59:59.99Z&$expand=Media,PropertyRooms,PropertyUnitTypes

# while (response.status_code == 200) and response.json() and ('@odata.nextLink' in response.json()):
#     print("Success")
#     print(response.json()['@odata.nextLink'])
#     response = requests.get(response.json()['@odata.nextLink'], headers=headers)

# print(response.status_code)
# print(response.json())


# print(tuple(client['mlspin']['RESI'].aggregate([
#     {"$group" : {'_id':"$STATE", 'count':{"$sum":1}}}
# ])))

# print(tuple(client['ctmls']['Property'].aggregate([
#     {"$group" : {'_id':"$StateOrProvince", 'count':{"$sum":1}}}
# ])))

# for dic in ({'_id': 'Massachusetts', 'count': 3535486}, {'_id': 'Maine', 'count': 431}, {'_id': 'Vermont', 'count': 125}, {'_id': 'New York', 'count': 105}, {'_id': 'Connecticut', 'count': 10055}, {'_id': 'Florida', 'count': 945}, {'_id': 'Georgia', 'count': 16}, {'_id': 'Rhode Island', 'count': 27993}, {'_id': 'New Hampshire', 'count': 48493}):
#     print(dic["_id"], ":", dic["count"])

# print("")

# for dic in ({'_id': 'Massachusetts', 'count': 595}, {'_id': 'South Carolina', 'count': 1}, {'_id': 'Colorado', 'count': 2}, {'_id': 'New York', 'count': 1163}, {'_id': 'Vermont', 'count': 4}, {'_id': 'Connecticut', 'count': 2316391}, {'_id': 'Florida', 'count': 9}, {'_id': 'New Jersey', 'count': 2}, {'_id': 'Dist of Columbia', 'count': 1}, {'_id': 'Rhode Island', 'count': 260}, {'_id': None, 'count': 10352}) :
#     print(dic["_id"], ":", dic["count"])

# print(
#     tuple(client['mlspin']['RESI'].aggregate([
#         {"$group" : {'_id':"$COUNTY"}}
#     ])) # This takes 265 seconds
# )
# geolocator = Nominatim(user_agent="What County Is This City In?")

# citiesMA = ['Abington', 'Acton', 'Acushnet', 'Acworth', 'Adams', 'Addison', 'Agawam', 'Alabama', 'Albany', 'Alexander', 'Alexandria', 'Alford', 'Alfred', 'Allenstown', 'Alstead', 'Alton', 'Altona', 'Amesbury', 'Amherst', 'Andover', 'Ansonia', 'Antrim', 'Apollo Beach', 'Aquinnah', 'Arlington', 'Ashburnham', 'Ashby', 'Ashfield', 'Ashford', 'Ashland', 'Athens', 'Athol', 'Atkinson', 'Atlanta', 'Attleboro', 'Auburn', 'Augusta', 'Avon', 'Ayer', 'Baldwin', 'Bangor', 'Bar Harbor', 'Barberville', 'Barkhamsted', 'Barnet', 'Barnstable', 'Barnstead', 'Barre', 'Barrington', 'Bartlett', 'Becket', 'Bedford', 'Belchertown', 'Belfast', 'Belgrade', 'Bellingham', 'Bellows Falls', 'Belmont', 'Bennington', 'Benton', 'Berkley', 'Berlin', 'Bernardston', 'Berwick', 'Bethel', 'Bethlehem', 'Beverly', 'Beverly Hills', 'Biddeford', 'Billerica', 'Black Diamond', 'Blackstone', 'Blandford', 'Bloomfield', 'Boca Raton', 'Bolton', 'Bonita Springs', 'Boscawen', 'Boston', 'Bourne', 'Bow', 'Bowdoin', 'Boxborough', 'Boxford', 'Boylston', 'Boynton Beach', 'Bozrah', 'Bradenton', 'Bradford', 'Braintree', 'Brattleboro', 'Brentwood', 'Brewer', 'Brewster', 'Bridgeport', 'Bridgewater', 'Bridgton', 'Brighton', 'Brimfield', 'Bristol', 'Brockton', 'Brookfield', 'Brookhaven', 'Brooklin', 'Brookline', 'Brooklyn', 'Brownfield', 'Brownington', 'Brunswick', 'Buckland', 'Buffalo', 'Burlington', 'Burnham', 'Burrillville', 'Buxton', 'Cambridge', 'Campton', 'Canaan', 'Candia', 'Canterbury', 'Canton', 'Cape Coral', 'Cape Elizabeth', 'Carlisle', 'Carroll', 'Carver', 'Center Harbor', 'Central', 'Central Falls', 'Chaplin', 'Charlemont', 'Charlestown', 'Charlton', 'Chatham', 'Cheektowaga', 'Chelmsford', 'Chelsea', 'Cheshire', 'Chester', 'Chesterfield', 'Chichester', 'Chicopee', 'Chilmark', 'China', 'Citrus Springs', 'Claremont', 'Clarksburg', 'Clarksville', 'Claverack', 'Clearwater', 'Clermont', 'Clifton Park', 'Clinton', 'Cohasset', 'Cohocton', 'Colchester', 'Colebrook', 'Colonie', 'Colrain', 'Columbia', 'Concord', 'Conway', 'Cooper', 'Copake', 'Cornish', 'Cortlandt', 'Coventry', 'Cranston', 'Cromwell', 'Croydon', 'Crystal River', 'Cumberland', 'Cummington', 'Dalton', 'Danbury', 'Danvers', 'Danville', 'Dartmouth', 'Daytona Beach', 'Daytona Beach Shores', 'De Leon Springs', 'DeBary', 'DeLand', 'Dedham', 'Deerfield', 'Deering', 'Delray Beach', 'Deltona', 'Denmark', 'Dennis', 'Derry', 'Destin', 'Devens', 'Dighton', 'Dixmont', 'Dorchester', 'Douglas', 'Dover', 'Dover Foxcroft', 'Dracut', 'Dublin', 'Dudley', 'Dummer', 'Dunbarton', 'Dunnellon', 'Dunstable', 'Durham', 'Duxbury', 'East Bridgewater', 'East Brookfield', 'East Granby', 'East Greenwich', 'East Haddam', 'East Hampton', 'East Hartford', 'East Haven', 'East Kingston', 'East Longmeadow', 'East Lyme', 'East Providence', 'East Windsor', 'Eastford', 'Eastham', 'Easthampton', 'Easton', 'Eaton', 'Edgartown', 'Effingham', 'Egremont', 'Eliot', 'Ellington', 'Enfield', 'Englewood ', 'Epping', 'Epsom', 'Errol', 'Erving', 'Essex', 'Estero', 'Etna', 'Everett', 'Exeter', 'Fairfield', 'Fairhaven', 'Fall River', 'Falmouth', 'Farmington', 'Fitchburg', 'Fitzwilliam', 'Floral City', 'Florida', 'Fort Fairfield', 'Fort Lauderdale', 'Fort Myers', 'Fort Pierce', 'Foster', 'Foxboro', 'Framingham', 'Francestown', 'Franconia', 'Franklin', 'Freedom', 'Freeport', 'Freetown', 'Fremont', 'Frye Island', 'Gardiner', 'Gardner', 'Geneva', 'Georgetown', 'Gilford', 'Gill', 'Gilmanton', 'Gilsum', 'Glastonbury', 'Glenburn', 'Glocester', 'Gloucester', 'Goffstown', 'Gorham', 'Goshen', 'Gosnold', 'Grafton', 'Granby', 'Grantham', 'Granville', 'Gray', 'Great Barrington', 'Greenfield', 'Greenland', 'Greenville', 'Griswold', 'Groton', 'Groveland', 'Guildhall', 'Guilford', 'Hadley', "Hale's Location", 'Halifax', 'Hamden', 'Hamilton', 'Hammond', 'Hampden', 'Hampstead', 'Hampton', 'Hampton Falls', 'Hancock', 'Hanover', 'Hanson', 'Hardwick', 'Harmony', 'Harpswell', 'Harrisville', 'Hartford', 'Hartland', 'Harvard', 'Harwich', 'Harwinton', 'Hatfield', 'Haverhill', 'Haverstraw', 'Hawley', 'Heath', 'Hebron', 'Henniker', 'Hernando', 'Highgate', 'Hill', 'Hillsborough', 'Hingham', 'Hinsdale', 'Holbrook', 'Holden', 'Holderness', 'Holland', 'Hollis', 'Holliston', 'Holyoke', 'Hooksett', 'Hopedale', 'Hopkinton', 'Houlton', 'Howey in the Hills', 'Hubbardston', 'Hudson', 'Hull', 'Huntington', 'Inverness', 'Ipswich', 'Jackman', 'Jackson', 'Jacksonville', 'Jaffrey', 'Jamestown', 'Jefferson', 'Johnston', 'Keene', 'Kennebunk', 'Kennebunkport', 'Kensington', 'Killingly', 'Killington', 'Kingston', 'Kissimmee', 'Kittery', 'Laconia', 'Lake George', 'Lake Mary ', 'Lakeville', 'Lancaster', 'Lanesborough', 'Langdon', 'Lawrence', 'Lebanon', 'Lecanto', 'Ledyard', 'Lee', 'Leicester', 'Lempster', 'Lenox', 'Leominster', 'Leverett', 'Lewiston', 'Lexington', 'Leyden', 'Liberty', 'Limerick', 'Limington', 'Lincoln', 'Lisbon', 'Litchfield', 'Little Compton', 'Littleton', 'Londonderry', 'Longmeadow', 'Longwood', 'Loudon', 'Lowell', 'Lubec', 'Ludlow', 'Lunenburg', 'Lyman', 'Lyndeborough', 'Lynn', 'Lynnfield', 'Macon', 'Madbury', 'Madison', 'Magalloway Plt', 'Malden', 'Manchester', 'Mansfield', 'Marblehead', 'Marco Island', 'Marion', 'Marlborough', 'Marlow', 'Marshfield', 'Mashpee', 'Mason', 'Mattapoisett', 'Maynard', 'Medfield', 'Medford', 'Medway', 'Melbourne', 'Melrose', 'Mendon', 'Meredith', 'Meriden', 'Merrimac', 'Merrimack', 'Methuen', 'Miami', 'Middleboro', 'Middlebury', 'Middlefield', 'Middleton', 'Middletown', 'Milan', 'Milford', 'Millbury', 'Millis', 'Millville', 'Milton', 'Miramar Beach', 'Monroe', 'Monson', 'Mont Vernon', 'Montague', 'Monterey', 'Montgomery', 'Montville', 'Moultonborough', 'Mount Washington', 'Nahant', 'Nantucket', 'Naples', 'Narragansett', 'Nashua', 'Natick', 'Naugatuck', 'Needham', 'Nelson', 'New Ashford', 'New Bedford', 'New Boston', 'New Braintree', 'New Britain', 'New Castle', 'New Durham', 'New Gloucester', 'New Hampton', 'New Hartford', 'New Haven', 'New Ipswich', 'New Lebanon', 'New London', 'New Marlboro', 'New Rochelle', 'New Salem', 'New Shoreham', 'New York City', 'Newbury', 'Newburyport', 'Newfield', 'Newfields', 'Newington', 'Newmarket', 'Newport', 'Newry', 'Newstead', 'Newton', 'Norfolk', 'Norridgewock', 'North Adams', 'North Andover', 'North Attleboro', 'North Berwick', 'North Brookfield', 'North Hampton', 'North Haven', 'North Hempstead', 'North Kingstown', 'North Miami Beach', 'North Providence', 'North Reading', 'North Smithfield', 'North Stonington', 'Northampton', 'Northborough', 'Northbridge', 'Northeast', 'Northfield', 'Northumberland', 'Northwood', 'Norton', 'Norwalk', 'Norwell', 'Norwich', 'Norwood', 'Nottingham', 'Oak Bluffs', 'Oakfield', 'Oakham', 'Ocklawaha', 'Odessa', 'Ogunquit', 'Old Lyme', 'Old Orchard Beach', 'Old Saybrook', 'Orange', 'Orange Park', 'Orlando', 'Orleans', 'Ormond Beach', 'Orwell', 'Ossipee', 'Otego', 'Other', 'Otis', 'Otisfield', 'Out of Town', 'Oxford', 'Palermo', 'Palm Bay', 'Palm Beach Gardens', 'Palm Coast', 'Palm Harbor', 'Palmer', 'Parsonsfield', 'Patterson', 'Pawtucket', 'Paxton', 'Peabody', 'Pelham', 'Pembroke', 'Pembroke Pines', 'Penobscot', 'Pepperell', 'Perry', 'Peru', 'Peterborough', 'Petersham', 'Phillipston', 'Phippsburg', 'Piermont', 'Pine Ridge', 'Pittsburg', 'Pittsfield', 'Plainfield', 'Plainville', 'Plaistow', 'Plymouth', 'Plympton', 'Poland', 'Pomfret', 'Port Charlotte', 'Port Orange', 'Port St. Lucie', 'Portland', 'Portsmouth', 'Pownal', 'Preston', 'Princeton', 'Providence', 'Provincetown', 'Punta Gorda', 'Putnam', 'Quechee', 'Queensbury', 'Quincy', 'Randolph', 'Raymond', 'Raynham', 'Reading', 'Readsboro', 'Rehoboth', 'Revere', 'Richmond', 'Ridgefield', 'Rindge', 'Riverview', 'Rochester', 'Rockland', 'Rockport', 'Rockwood', 'Rocky Hill', 'Rollinsford', 'Rowe', 'Rowley', 'Roxbury', 'Royalston', 'Rumney', 'Russell', 'Rutland', 'Rye', 'Ryegate', 'Saco', 'Salem', 'Salisbury', 'Sanbornton', 'Sandisfield', 'Sandown', 'Sandwich', 'Sanford', 'Sangerville', 'Sarasota', 'Satellite Beach', 'Saugus', 'Savoy', 'Scarborough', 'Schuyler', 'Scituate', 'Scotland', 'Seabrook', 'Sebring', 'Seekonk', 'Seymour', 'Sharon', 'Sheffield', 'Shelburne', 'Shelton', 'Sherborn', 'Shirley', 'Shrewsbury', 'Shutesbury', 'Simsbury', 'Smithfield', 'Solon', 'Somers', 'Somerset', 'Somersworth', 'Somerville', 'South Berwick', 'South Burlington', 'South Hadley', 'South Hampton', 'South Kingstown', 'South Portland', 'South Windsor', 'Southampton', 'Southborough', 'Southbridge', 'Southington', 'Southwest Harbor', 'Southwick', 'Spencer', 'Sprague', 'Springfield', 'St. Petersburg', 'Stafford', 'Stamford', 'Standish', 'Stark', 'Stephentown', 'Sterling', 'Stewartstown', 'Stockbridge', 'Stoddard', 'Stoneham', 'Stonington', 'Stoughton', 'Stow', 'Strafford', 'Stratford', 'Stratham', 'Sturbridge', 'Sudbury', 'Suffield', 'Sugar Hill', 'Sullivan', 'Sun City Center', 'Sunapee', 'Sunderland', 'Sunrise', 'Surry', 'Sutton', 'Swampscott', 'Swans Island', 'Swansea', 'Swanzey', 'Taghkanic', 'Tampa', 'Tamworth', 'Tarpon Springs', 'Taunton', 'Temple', 'Templeton', 'Tewksbury', 'The Forks Plt', 'Thompson', 'Thonotosassa', 'Thornton', 'Tilton', 'Tisbury', 'Tiverton', 'Tolland', 'Topsfield', 'Topsham', 'Torrington', 'Townsend', 'Trinity', 'Troy', 'Trumbull', 'Truro', 'Tuftonboro', 'Tyngsborough', 'Tyringham', 'Ulster', 'Union', 'Unity', 'Upton', 'Uxbridge', 'Valrico', 'Vassalboro', 'Venice', 'Vernon', 'Vero Beach', 'Vershire', 'Vinalhaven', 'Voluntown', 'Wakefield', 'Wales', 'Wallingford', 'Walpole', 'Waltham', 'Wardsboro', 'Ware', 'Wareham', 'Warner', 'Warren', 'Warwick', 'Washington', 'Waterboro', 'Waterbury', 'Waterford', 'Watertown', 'Waterville Valley', 'Wayland', 'Weare', 'Webster', 'Wellesley', 'Wellfleet', 'Wells', 'Wendell', 'Wenham', 'Wentworth', 'Wentworth Location', 'West Boylston', 'West Bridgewater', 'West Brookfield', 'West Greenwich', 'West Halifax', 'West Hartford', 'West Haven', 'West Newbury', 'West Seneca', 'West Springfield', 'West Stockbridge', 'West Tisbury', 'West Warwick', 'Westborough', 'Westerly', 'Westfield', 'Westford', 'Westhampton', 'Westminster', 'Westmoreland', 'Weston', 'Westport', 'Westwood', 'Wethersfield', 'Weymouth', 'Whately', 'Whitefield', 'Whitestown', 'Whitingham', 'Whitman', 'Wilbraham', 'Williamsburg', 'Williamstown', 'Willington', 'Wilmington', 'Wilmot', 'Wilton', 'Winchendon', 'Winchester', 'Windham', 'Windsor', 'Windsor Locks', 'Winter Springs', 'Winthrop', 'Woburn', 'Wolcott', 'Wolfeboro', 'Woodbury', 'Woodstock', 'Woonsocket', 'Worcester', 'Worthington', 'Wrentham', 'Yarmouth', 'York']

# countiesMA = {' Albany': ['Colonie'],
#  ' Allegheny': ['Pittsburg'],
#  ' Barnstable': ['Alexander',
#                  'Augusta',
#                  'Barnstable',
#                  'Bourne',
#                  'Brewster',
#                  'Bridgeport',
#                  'Chatham',
#                  'Clearwater',
#                  'Coventry',
#                  'Dennis',
#                  'Eastham',
#                  'Englewood ',
#                  'Falmouth',
#                  'Gardiner',
#                  'Gray',
#                  'Greenland',
#                  'Harwich',
#                  'Limerick',
#                  'Mashpee',
#                  'Melbourne',
#                  'Miramar Beach',
#                  'Northwood',
#                  'Orleans',
#                  'Palm Coast',
#                  'Palm Harbor',
#                  'Penobscot',
#                  'Provincetown',
#                  'Sandwich',
#                  'Seabrook',
#                  'Sunrise',
#                  'Surry',
#                  'Thompson',
#                  'Truro',
#                  'Waterford',
#                  'Wellfleet',
#                  'Whitefield',
#                  'Willington',
#                  'Yarmouth'],
#  ' Berkshire': ['Adams',
#                 'Alford',
#                 'Ashford',
#                 'Barrington',
#                 'Becket',
#                 'Benton',
#                 'Bethlehem',
#                 'Cheshire',
#                 'Clarksburg',
#                 'Copake',
#                 'Dalton',
#                 'East Windsor',
#                 'Egremont',
#                 'Fairfield',
#                 'Florida',
#                 'Fort Myers',
#                 'Great Barrington',
#                 'Hancock',
#                 'Hinsdale',
#                 'Lanesborough',
#                 'Lee',
#                 'Lenox',
#                 'Marlow',
#                 'Monterey',
#                 'Montville',
#                 'Mount Washington',
#                 'New Ashford',
#                 'North Adams',
#                 'Norwalk',
#                 'Nottingham',
#                 'Otis',
#                 'Peru',
#                 'Pittsfield',
#                 'Richmond',
#                 'Sandisfield',
#                 'Savoy',
#                 'Sheffield',
#                 'Somers',
#                 'South Windsor',
#                 'Stafford',
#                 'Stockbridge',
#                 'Tyringham',
#                 'Washington',
#                 'West Stockbridge',
#                 'Williamstown',
#                 'Windsor',
#                 'York'],
#  ' Bristol': ['Acushnet',
#               'Attleboro',
#               'Barnet',
#               'Berkley',
#               'Bethel',
#               'Brooklyn',
#               'Cape Coral',
#               'Colebrook',
#               'Dartmouth',
#               'Dighton',
#               'Easton',
#               'Fairhaven',
#               'Fall River',
#               'Freetown',
#               'Hampton',
#               'Hebron',
#               'Lewiston',
#               'Lisbon',
#               'Mansfield',
#               'Marco Island',
#               'New Bedford',
#               'North Attleboro',
#               'Norton',
#               'Perry',
#               'Raynham',
#               'Rehoboth',
#               'Sanford',
#               'Seekonk',
#               'Somerset',
#               'Sullivan',
#               'Swansea',
#               'Taunton',
#               'Westport'],
#  ' Cumberland': ['South Portland'],
#  ' Dukes': ['Aquinnah',
#             'Cape Elizabeth',
#             'Chilmark',
#             'East Hartford',
#             'Edgartown',
#             'Gosnold',
#             'Hartford',
#             'New Hartford',
#             'Oak Bluffs',
#             'Other',
#             'Tisbury',
#             'West Hartford',
#             'West Haven',
#             'West Tisbury'],
#  ' Essex': ['Addison',
#             'Alton',
#             'Amesbury',
#             'Andover',
#             'Antrim',
#             'Belfast',
#             'Beverly',
#             'Beverly Hills',
#             'Boxford',
#             'Bradford',
#             'Brentwood',
#             'Burnham',
#             'Buxton',
#             'Candia',
#             'Claremont',
#             'Clifton Park',
#             'Colchester',
#             'Cromwell',
#             'Danbury',
#             'Danvers',
#             'Derry',
#             'Dublin',
#             'Durham',
#             'East Lyme',
#             'Enfield',
#             'Essex',
#             'Floral City',
#             'Fort Pierce',
#             'Foster',
#             'Freeport',
#             'Georgetown',
#             'Gilford',
#             'Gloucester',
#             'Griswold',
#             'Groveland',
#             'Guilford',
#             'Hamilton',
#             'Hammond',
#             'Haverhill',
#             'Ipswich',
#             'Jackman',
#             'Langdon',
#             'Lawrence',
#             'Lynn',
#             'Lynnfield',
#             'Macon',
#             'Manchester',
#             'Marblehead',
#             'Meredith',
#             'Merrimac',
#             'Merrimack',
#             'Methuen',
#             'Middlebury',
#             'Middleton',
#             'Nahant',
#             'Naples',
#             'Nelson',
#             'New Durham',
#             'New Gloucester',
#             'New Ipswich',
#             'New Marlboro',
#             'Newbury',
#             'Newburyport',
#             'North Andover',
#             'North Stonington',
#             'Ogunquit',
#             'Orlando',
#             'Palermo',
#             'Palm Bay',
#             'Peabody',
#             'Plaistow',
#             'Port Charlotte',
#             'Portland',
#             'Preston',
#             'Ridgefield',
#             'Rockport',
#             'Rocky Hill',
#             'Rowley',
#             'Salem',
#             'Salisbury',
#             'Saugus',
#             'Shelton',
#             'Stonington',
#             'Strafford',
#             'Swampscott',
#             'Tilton',
#             'Topsfield',
#             'Trumbull',
#             'Venice',
#             'Wallingford',
#             'Weare',
#             'Wenham',
#             'West Newbury',
#             'Wilmot',
#             'Windham',
#             'Wolcott',
#             'Woodbury'],
#  ' Franklin': ['Ashfield',
#                'Bernardston',
#                'Bozrah',
#                'Brattleboro',
#                'Buckland',
#                'Central Falls',
#                'Charlemont',
#                'Claverack',
#                'Colrain',
#                'Conway',
#                'Deerfield',
#                'Erving',
#                'Gill',
#                'Greenfield',
#                'Hawley',
#                'Heath',
#                'Jackson',
#                'Johnston',
#                'Lake Mary ',
#                'Leverett',
#                'Leyden',
#                'Monroe',
#                'Montague',
#                'New Salem',
#                'Northfield',
#                'Orange',
#                'Orange Park',
#                'Pine Ridge',
#                'Poland',
#                'Readsboro',
#                'Rowe',
#                'Schuyler',
#                'Seymour',
#                'Shelburne',
#                'Shutesbury',
#                'Stoddard',
#                'Sunderland',
#                'Unity',
#                'Warwick',
#                'Wells',
#                'Wendell',
#                'West Warwick',
#                'Whately'],
#  ' Hampden': ['Agawam',
#               'Blandford',
#               'Brimfield',
#               'Chester',
#               'Chicopee',
#               'Daytona Beach',
#               'East Longmeadow',
#               'Granville',
#               'Hampden',
#               'Holland',
#               'Holyoke',
#               'Lake George',
#               'Longmeadow',
#               'Ludlow',
#               'Monson',
#               'Montgomery',
#               'Northumberland',
#               'Odessa',
#               'Palmer',
#               'Putnam',
#               'Queensbury',
#               'Russell',
#               'Southwick',
#               'Springfield',
#               'Suffield',
#               'Tolland',
#               'Wales',
#               'West Springfield',
#               'Westfield',
#               'Wilbraham'],
#  ' Hampshire': ['Amherst',
#                 'Belchertown',
#                 'Central',
#                 'Chesterfield',
#                 'Cummington',
#                 'East Granby',
#                 'East Greenwich',
#                 'Easthampton',
#                 'Goshen',
#                 'Granby',
#                 'Hadley',
#                 'Hatfield',
#                 'Huntington',
#                 'Middlefield',
#                 'Northampton',
#                 'Norwich',
#                 'Pelham',
#                 'Plainfield',
#                 'Scarborough',
#                 'South Hadley',
#                 'Southampton',
#                 'Ware',
#                 'Warner',
#                 'Wentworth',
#                 'West Greenwich',
#                 'Westhampton',
#                 'Williamsburg',
#                 'Wilton',
#                 'Worthington'],
#  ' Hartford': ['Farmington', 'Hartland'],
#  ' Hillsborough': ['Hillsborough', 'Tampa'],
#  ' Litchfield': ['Canaan'],
#  ' Middlesex': ['Acton',
#                 'Alexandria',
#                 'Alfred',
#                 'Ansonia',
#                 'Arlington',
#                 'Ashby',
#                 'Ashland',
#                 'Ayer',
#                 'Baldwin',
#                 'Bedford',
#                 'Belmont',
#                 'Berwick',
#                 'Billerica',
#                 'Bloomfield',
#                 'Bow',
#                 'Boxborough',
#                 'Brookhaven',
#                 'Burlington',
#                 'Cambridge',
#                 'Canterbury',
#                 'Carlisle',
#                 'Chaplin',
#                 'Chelmsford',
#                 'Columbia',
#                 'Concord',
#                 'Croydon',
#                 'Crystal River',
#                 'Cumberland',
#                 'Deering',
#                 'Dracut',
#                 'Dummer',
#                 'Dunstable',
#                 'East Providence',
#                 'Eliot',
#                 'Ellington',
#                 'Epping',
#                 'Everett',
#                 'Fort Lauderdale',
#                 'Framingham',
#                 'Franconia',
#                 'Fremont',
#                 'Glenburn',
#                 'Groton',
#                 'Hampstead',
#                 'Hollis',
#                 'Holliston',
#                 'Hopkinton',
#                 'Houlton',
#                 'Hudson',
#                 'Laconia',
#                 'Lexington',
#                 'Lincoln',
#                 'Littleton',
#                 'Londonderry',
#                 'Lowell',
#                 'Lyman',
#                 'Malden',
#                 'Marlborough',
#                 'Maynard',
#                 'Medford',
#                 'Melrose',
#                 'Meriden',
#                 'Milan',
#                 'Nashua',
#                 'Natick',
#                 'Newfield',
#                 'Newton',
#                 'North Berwick',
#                 'North Providence',
#                 'North Reading',
#                 'Ossipee',
#                 'Patterson',
#                 'Pawtucket',
#                 'Pepperell',
#                 'Pomfret',
#                 'Providence',
#                 'Reading',
#                 'Rindge',
#                 'Riverview',
#                 'Saco',
#                 'Sherborn',
#                 'Shirley',
#                 'Solon',
#                 'Somerville',
#                 'South Berwick',
#                 'South Burlington',
#                 'Stark',
#                 'Stoneham',
#                 'Stow',
#                 'Stratham',
#                 'Sudbury',
#                 'Sugar Hill',
#                 'Tamworth',
#                 'Temple',
#                 'Tewksbury',
#                 'Thornton',
#                 'Torrington',
#                 'Townsend',
#                 'Trinity',
#                 'Troy',
#                 'Tyngsborough',
#                 'Union',
#                 'Wakefield',
#                 'Waltham',
#                 'Watertown',
#                 'Wayland',
#                 'Westford',
#                 'Westmoreland',
#                 'Weston',
#                 'Wilmington',
#                 'Winchester',
#                 'Woburn',
#                 'Woonsocket'],
#  ' Montgomery': ['Clarksville'],
#  ' Nantucket': ['Cornish', 'Nantucket'],
#  ' Nassau': ['North Hempstead'],
#  ' New Haven': ['New Haven', 'North Haven', 'Waterbury'],
#  ' New London': ['New London', 'Old Lyme'],
#  ' Newport': ['Little Compton', 'Middletown'],
#  ' Norfolk': ['Alstead',
#               'Athens',
#               'Avon',
#               'Bellingham',
#               'Braintree',
#               'Brookline',
#               'Canton',
#               'China',
#               'Cohasset',
#               'Dedham',
#               'Dover',
#               'Dunbarton',
#               'Foxboro',
#               'Franklin',
#               'Freedom',
#               'Hamden',
#               'Holbrook',
#               'Hooksett',
#               'Jaffrey',
#               'Ledyard',
#               'Longwood',
#               'Medfield',
#               'Medway',
#               'Millis',
#               'Milton',
#               'Narragansett',
#               'Naugatuck',
#               'Needham',
#               'New Shoreham',
#               'Newport',
#               'Norfolk',
#               'Norwood',
#               'Piermont',
#               'Plainville',
#               'Quincy',
#               'Randolph',
#               'Sharon',
#               'Sprague',
#               'Stoughton',
#               'Stratford',
#               'Walpole',
#               'Wellesley',
#               'West Seneca',
#               'Westerly',
#               'Westwood',
#               'Wethersfield',
#               'Weymouth',
#               'Wrentham'],
#  ' Onslow': ['Jacksonville'],
#  ' Palm Beach': ['Palm Beach Gardens'],
#  ' Pinellas': ['St. Petersburg'],
#  ' Plymouth': ['Abington',
#                'Atlanta',
#                'Bangor',
#                'Bartlett',
#                'Bridgewater',
#                'Brockton',
#                'Brownfield',
#                'Carver',
#                'Cooper',
#                'Duxbury',
#                'East Bridgewater',
#                'East Hampton',
#                'East Kingston',
#                'Glastonbury',
#                'Gorham',
#                'Halifax',
#                'Hanover',
#                'Hanson',
#                'Hingham',
#                'Hull',
#                'Inverness',
#                'Keene',
#                'Kensington',
#                'Kingston',
#                'Lakeville',
#                'Marion',
#                'Marshfield',
#                'Mattapoisett',
#                'Middleboro',
#                'New Hampton',
#                'New Rochelle',
#                'North Hampton',
#                'North Kingstown',
#                'Norwell',
#                'Pembroke',
#                'Pembroke Pines',
#                'Plymouth',
#                'Plympton',
#                'Pownal',
#                'Raymond',
#                'Rochester',
#                'Rockland',
#                'Rockwood',
#                'Rye',
#                'Ryegate',
#                'Scituate',
#                'Scotland',
#                'South Hampton',
#                'South Kingstown',
#                'Standish',
#                'Wareham',
#                'Waterville Valley',
#                'West Bridgewater',
#                'West Halifax',
#                'Whitman'],
#  ' Providence': ['North Smithfield'],
#  ' Richmond': ['New York City'],
#  ' Rockingham': ['Atkinson'],
#  ' Suffolk': ['Albany',
#               'Bar Harbor',
#               'Belgrade',
#               'Bennington',
#               'Boston',
#               'Bowdoin',
#               'Brighton',
#               'Brunswick',
#               'Center Harbor',
#               'Charlestown',
#               'Chelsea',
#               'Clermont',
#               'Cranston',
#               'Danville',
#               'Dorchester',
#               'Errol',
#               'Etna',
#               'Exeter',
#               'Fort Fairfield',
#               'Frye Island',
#               'Geneva',
#               'Harmony',
#               'Harvard',
#               'Highgate',
#               'Jamestown',
#               'Liberty',
#               'Lubec',
#               'Madison',
#               'Mason',
#               'Miami',
#               'Mont Vernon',
#               'New Castle',
#               'Newmarket',
#               'Old Saybrook',
#               'Ormond Beach',
#               'Otisfield',
#               'Peterborough',
#               'Phippsburg',
#               'Portsmouth',
#               'Revere',
#               'Roxbury',
#               'Rumney',
#               'Satellite Beach',
#               'Smithfield',
#               'Southwest Harbor',
#               'Sun City Center',
#               'Sunapee',
#               'Vernon',
#               'Vero Beach',
#               'Vershire',
#               'Winthrop'],
#  ' Volusia': ['DeLand'],
#  ' Walker': ['Alabama'],
#  ' Worcester': ['Apollo Beach',
#                 'Ashburnham',
#                 'Athol',
#                 'Auburn',
#                 'Barre',
#                 'Berlin',
#                 'Black Diamond',
#                 'Blackstone',
#                 'Bolton',
#                 'Boylston',
#                 'Boynton Beach',
#                 'Brewer',
#                 'Brookfield',
#                 'Buffalo',
#                 'Carroll',
#                 'Charlton',
#                 'Clinton',
#                 'Delray Beach',
#                 'Denmark',
#                 'Devens',
#                 'Douglas',
#                 'Dudley',
#                 'East Brookfield',
#                 'Eastford',
#                 'Eaton',
#                 'Fitchburg',
#                 'Fitzwilliam',
#                 'Gardner',
#                 'Grafton',
#                 'Greenville',
#                 'Hardwick',
#                 'Harrisville',
#                 'Hill',
#                 'Holden',
#                 'Hopedale',
#                 'Hubbardston',
#                 'Jefferson',
#                 'Kennebunk',
#                 'Kittery',
#                 'Lancaster',
#                 'Lebanon',
#                 'Leicester',
#                 'Leominster',
#                 'Litchfield',
#                 'Loudon',
#                 'Lunenburg',
#                 'Mendon',
#                 'Milford',
#                 'Millbury',
#                 'Millville',
#                 'New Boston',
#                 'New Braintree',
#                 'New Lebanon',
#                 'Newington',
#                 'North Brookfield',
#                 'Northborough',
#                 'Northbridge',
#                 'Oakham',
#                 'Oxford',
#                 'Paxton',
#                 'Petersham',
#                 'Phillipston',
#                 'Princeton',
#                 'Royalston',
#                 'Rutland',
#                 'Shrewsbury',
#                 'Southborough',
#                 'Southbridge',
#                 'Spencer',
#                 'Stamford',
#                 'Sterling',
#                 'Sturbridge',
#                 'Sutton',
#                 'Swans Island',
#                 'Swanzey',
#                 'Templeton',
#                 'Tiverton',
#                 'Upton',
#                 'Uxbridge',
#                 'Warren',
#                 'Webster',
#                 'West Boylston',
#                 'West Brookfield',
#                 'Westborough',
#                 'Westminster',
#                 'Winchendon',
#                 'Woodstock',
#                 'Worcester'],
#  ' York': ['Old Orchard Beach'],
#  'Bristol': ['Bristol'],
#  'None': ['Acworth',
#           'Allenstown',
#           'Altona',
#           'Barberville',
#           'Barkhamsted',
#           'Barnstead',
#           'Bellows Falls',
#           'Biddeford',
#           'Boca Raton',
#           'Bonita Springs',
#           'Boscawen',
#           'Bradenton',
#           'Bridgton',
#           'Brooklin',
#           'Brownington',
#           'Burrillville',
#           'Campton',
#           'Cheektowaga',
#           'Chichester',
#           'Citrus Springs',
#           'Cohocton',
#           'Cortlandt',
#           'Daytona Beach Shores',
#           'De Leon Springs',
#           'DeBary',
#           'Deltona',
#           'Destin',
#           'Dixmont',
#           'Dover Foxcroft',
#           'Dunnellon',
#           'East Haddam',
#           'East Haven',
#           'Effingham',
#           'Epsom',
#           'Estero',
#           'Francestown',
#           'Gilmanton',
#           'Gilsum',
#           'Glocester',
#           'Goffstown',
#           'Grantham',
#           'Guildhall',
#           "Hale's Location",
#           'Hampton Falls',
#           'Harpswell',
#           'Harwinton',
#           'Haverstraw',
#           'Henniker',
#           'Hernando',
#           'Holderness',
#           'Howey in the Hills',
#           'Kennebunkport',
#           'Killingly',
#           'Killington',
#           'Kissimmee',
#           'Lecanto',
#           'Lempster',
#           'Limington',
#           'Lyndeborough',
#           'Madbury',
#           'Magalloway Plt',
#           'Moultonborough',
#           'New Britain',
#           'Newfields',
#           'Newry',
#           'Newstead',
#           'Norridgewock',
#           'North Miami Beach',
#           'Northeast',
#           'Oakfield',
#           'Ocklawaha',
#           'Orwell',
#           'Otego',
#           'Out of Town',
#           'Parsonsfield',
#           'Port Orange',
#           'Port St. Lucie',
#           'Punta Gorda',
#           'Quechee',
#           'Rollinsford',
#           'Sanbornton',
#           'Sandown',
#           'Sangerville',
#           'Sarasota',
#           'Sebring',
#           'Simsbury',
#           'Somersworth',
#           'Southington',
#           'Stephentown',
#           'Stewartstown',
#           'Taghkanic',
#           'Tarpon Springs',
#           'The Forks Plt',
#           'Thonotosassa',
#           'Topsham',
#           'Tuftonboro',
#           'Ulster',
#           'Valrico',
#           'Vassalboro',
#           'Vinalhaven',
#           'Voluntown',
#           'Wardsboro',
#           'Waterboro',
#           'Wentworth Location',
#           'Whitestown',
#           'Whitingham',
#           'Windsor Locks',
#           'Winter Springs',
#           'Wolfeboro']}

# states = {'None': []}

# try:
#     for city in tqdm(citiesMA, ncols=150):
#         location = geolocator.geocode(city + ", Massachusetts")
#         countyFound = False
#         if location:
#             locationSplit = str(location).split(',')
#             state = locationSplit[-2].strip()
#             state = locationSplit[-3].strip() if state.replace('-', '').isnumeric() else st ate
#             countyContenders = list(filter(lambda x: "County" in x, locationSplit))
#             if state in states:
#                 countiesDict = states[state]
#             else:
#                 countiesDict = {}
#                 states[state] = countiesDict
#             if len(countyContenders) == 1:
#                 countyFound = True
#                 county = countyContenders[0][:-7].strip()
#                 if county in countiesDict:
#                     countiesDict[county].append(city)
#                 else:
#                     countiesDict[county] = [city]
#             else:
#                 if 'None' in countiesDict:
#                     countiesDict['None'].append(city)
#                 else:
#                     countiesDict['None'] = [city]
#         else:
#             states['None'].append(city)
# except Exception as exc:
#     print("failed,", exc)
#     traceback.print_exc()
#     print("")
# finally:
#     pprint(states)


# for i, county in enumerate(countiesMA):
#     print(i+1, county)

# location = geolocator.geocode("Colonie" + " United States")
# print(location.raw)
# print(str(location))
# state = list(str(location).split(','))[-2]
# print(state.strip())



# for county in countiesNJ:
#     print("\nCounty:", county)
#     cities = countiesNJ[county]
#     cities.sort()
#     print(cities)
    # for city in countiesNJ[county]:
    #     location = geolocator.geocode(city + ", New Jersey")
    #     if location:
    #         county = list(filter(lambda x: "County" in x, str(location).split(',')))[0]
    #     print(city, ":", county if county else "")


# try:
#     with open('listingsInEachCity.json') as json_file:
#         data = json.load(json_file)
#         print(data)
# except FileNotFoundError:
#     print("a")

# maxYears = 20

# dr = pd.date_range(
#     end=datetime.today(),
#     periods=12*maxYears,
#     freq="MS",
#     normalize=True,
#     name="Month"
# )
# months = pd.DataFrame([], index=dr)
# months["test"] = tuple(map(lambda x: x, months.index))

# print(months)
# %%
