# Author: Andrew Pantera for TLCengine
# Last Updated: 10/12/2024
# Streamlit app to serve as a front end for two housing prices databases (NJ and MA) to provide an interactive dashboard of MLS data

import json
import math
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pandas.tseries.offsets import MonthEnd
from pymongo import MongoClient
from scipy import stats

import MLS

load_dotenv(verbose=True) 
st.set_page_config(page_title='TLC Housing Prices Dashboard', page_icon ='https://pbs.twimg.com/profile_images/1068265299932114944/8Mvh266i.jpg', layout = 'wide')
st.sidebar.image("https://static1.squarespace.com/static/5bd66859c2ff616bbd26a33b/t/5bd66e6db208fc08df43271f/1610621824459/?format=1500w") # Display TLC logo
mongoString = (f'mongodb://{os.getenv("MONGODB_USERNAME")}:{os.getenv("MONGODB_PASSWORD")}@{os.getenv("MONGODB_URL")}/') # Assemble string used to connect to mongodb from geo2
client = MongoClient(mongoString)
pd.set_option('mode.chained_assignment', None) # Turn off a pandas warning 
#MLSDict = MLS.getMLSs()

months = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
maxYears = 20
listings_group = []
filteredListings_group = [None, None, None, None]
dataset_names = {}

def vaidateQueryParam(paramName: str, paramType: type, default, validationFunction: Callable[[Any], bool] = lambda x: True):
    """Validate a query paramater. Return it if it exists and is valid,
    return the default otherwise.

    Args:
        paramName (str): The name of the paramater in the query string
        paramType (type): The type to try to cast the paramater value too
        default ([paramType]): What to return when the paramater value is invalid or the paramater doesn't exist
        validationFunction (Callable[[Any], bool]): A function that returns whether the paramater valid.
            Defaults to a function that always returns true. 
    """
    # mabelo edit 2024: st.experimental_get_query_params depricated, replaced with st.query_params, no longer uses allow_output_mutation
    #params = st.experimental_get_query_params()
    params = st.query_params

    # Make sure the paramater we're after is in the url:
    if paramName not in params:
        return default
    valueList = params[paramName]
    # For some reason `st.experimental_set_query_params` always puts the paramater in a list, so here we assume the value we're after is the first element in a list

    # Check to see if we don't get a list, or of its empty:
    if type(valueList) != list or len(valueList) == 0:
        return default

    value = valueList[0] if paramType != list else valueList

    # Validate type
    if type(value) != paramType:
        try:
            value = paramType(value)
        except (ValueError, TypeError):
            return default

    # Validate using validation function
    if not validationFunction(value):
        return default
    return value

datasets = st.sidebar.slider('Number of datasets of data to compare', 1, 4, vaidateQueryParam("datasets", int, 1, lambda x: 1 <= x <= 4))

# mabelo edit 2024: st.cache depricated, replaced with st.cache_resource, no longer uses allow_output_mutation 
# @st.cache(allow_output_mutation=True, hash_funcs={pd.DataFrame: lambda _: None})
@st.cache_resource(hash_funcs={pd.DataFrame: lambda _: None})
def getMLSs():
    # Put this into a separate function in order to cache it, so streamlit doesn't request the MLSs again every time something is changed. This doesnt work
    return MLS.getMLSs()

MLSDict = getMLSs()

def makeChart(chart, title, data, rolling, description=None, sumTotalData=True, zeroScaleYAxis=True, percent=False):
    """Create a streamlit chart dropdown for the given data.

    Args:
        chart (string): This is the name of the chart as it appears in the 'charts' list and the views multiselector.
        title (string): This is the title of the graph, often the same as the chart name.
        data ([type]): [description]
        rolling (bool): Whether to chart a rolling average
        description (string, optional): A description of the chart. Defaults to None.
        sumTotalData (bool, optional): Whether to display the sum of all the datapoints along with the title. Sometimes it does not make sense to do this, like when the data is a rate. Defaults to True.
    """
    with st.expander(chart):
        if description:
            st.text(description)

        data["DatasetName"] = data.Label.apply(lambda x: dataset_names.get(x))
        rollingLabels = []
        if rolling:
            for label in map(lambda x: "Dataset " + str(x), range(1, datasets+1)):
                roll = data.loc[data["Label"] == label][chart].rolling(window=rollingMonths).mean()
                rollingDf = data[["Month"]]
                rollingDf[chart] = roll # This line sets off the pandas SettingWithCopyWarning
                rollingDf["DatasetName"] = "Rolling Average " + dataset_names.get(label)
                rollingDf["Label"] = "Rolling Average " + dataset_names.get(label)
                rollingLabels.append("Rolling Average " + dataset_names.get(label))
                print('testing', flush=True)# ---------------------remove
                print(str(type(rollingDf)), flush=True) # ---------------------remove
                # data = data.append(rollingDf)
                data = pd.concat([data, rollingDf])

        firstMonth = (datetime.today() - timedelta(days=365*years)).strftime("%Y-%m") + "-01"
        lastMonth = datetime.today().strftime("%Y-%m") + "-01"
        data["I"] = data["Month"]
        data.set_index('I', inplace=True)
        data.sort_index(inplace=True)

        # Display the title and the sum of the non-rolling average data points        
        st.subheader(f'{title}' + (': ' + str(int(data.loc[data["Label"] != "Rolling Average", chart].sum())) + ' total' if sumTotalData else '')) #  != "Rolling Average" is going to have to be changed to somthing like " ~.isin(["Rolling Aaverage", "Projected"])" once data projections are added

        data["year"] = data.Month.dt.year
        data["monthNum"] = data.Month.dt.month
        data["monthStr"] = data.monthNum.apply(lambda x: '0'*(2-len(str(x))) + str(x) + ": " + months[x-1])

        if "Time Series Line Chart" in chartViews:
            dataChart = data[firstMonth:lastMonth]
            altChart = alt.Chart(   # Actual Chart 
                    data=dataChart,
                    mark="line",
                    title=title
                ).mark_line(point=True
                ).encode(
                    alt.Y(chart,
                        scale=alt.Scale(zero=zeroScaleYAxis),
                        axis=alt.Axis(format='%' if percent else 'f', labels=True)
                    ),
                    x='Month',
                    tooltip=['Month', chart],
                    color=alt.Color('DatasetName:N', legend=alt.Legend(title="Dataset")),
                ).interactive(
                ).properties(
                    height=chartHeight
            )
            if confidenceRegion:
                confidenceRegionChart = alt.Chart(
                    data=dataChart,
                    mark="errorband",
                    title=title
                ).mark_errorband(
                    borders=True,
                    # extent="ci",
                ).encode(
                    alt.Y(
                        title=chart,
                        field="ci_hi",
                        type="quantitative",
                        scale=alt.Scale(zero=zeroScaleYAxis),
                        axis=alt.Axis(format='%' if percent else 'f', labels=True)
                    ),
                    alt.Y2(
                        field="ci_lo",
                    ),
                    x='Month',
                    tooltip=['Month', chart],
                    color=alt.Color('DatasetName:N', legend=alt.Legend(title="Dataset")),
                )
                
            st.altair_chart(
                altChart if not confidenceRegion else altChart+confidenceRegionChart,
                use_container_width=True
            )

        if "Stacked Line Chart" in chartViews:
            # To have altair plot different years as different lines we need to
            #     1) set the label as the year
            #     2) Set the year data field to be this year, so that they're all displayed
            #         ontop of eachother at the same x positions 
            # This will conflict with the rolling average, so we won't display the rolling 
            # average on stacked line charts. It would be too many lines anyways

            stackedData = data[(datetime.today() - timedelta(days=364.75*years)).strftime("%Y") + "-01-01":lastMonth] # Truncate the data to start at January 1st 'years' years ago
            stackedData = stackedData.loc[~stackedData["Label"].isin(rollingLabels)] # Get rid of rolling average data (If we get rid of the "~", it would keep only the rolling average data)

            for thisYear in range((datetime.today() - timedelta(days=364.75*years)).year, datetime.today().year + 1):
                stackedData.loc[stackedData["year"] == thisYear, "Label"] = str(thisYear) # Set the label to the year
                stackedData.loc[stackedData["year"] == thisYear, "DatasetName"] = stackedData.loc[stackedData["year"] == thisYear, "DatasetName"] + " " + str(thisYear) # Append the year to the dataset name

            stackedData["Month"] = stackedData["Month"].apply(lambda x: str(x.month).zfill(2) + ": " + months[x.month-1]) # Make sure the date in the 'Month' column is this year

            st.altair_chart(
                alt.Chart(
                    data=stackedData,
                    mark="line",
                    title=title
                ).mark_line(point=True
                ).encode(
                    alt.Y(chart,
                        scale=alt.Scale(zero=zeroScaleYAxis),
                        axis=alt.Axis(format='%' if percent else 'f', labels=True)
                    ),
                    x='Month',
                    tooltip=['Month', chart, 'Label'],
                    color=alt.Color('DatasetName:N', legend=alt.Legend(title="Dataset")),
                ).interactive(
                ).properties(
                    height=chartHeight
                ),
                use_container_width=True
            )

        if "Grouped Bar Chart" in chartViews:
            data = data[firstMonth:lastMonth]
            data = data.loc[~data["Label"].isin(rollingLabels)] # Get rid of rolling average data
            data["dataset"] = data.Label.apply(lambda x: int(x[8:]))
            if datasets == 1:
                monthly = st.checkbox("Separate By Month", value=True, key=title+"By Month")
                if monthly:
                    st.altair_chart(
                        alt.Chart(data).mark_bar(
                        ).encode(
                            alt.Y(str(chart)+":Q",
                                scale=alt.Scale(zero=zeroScaleYAxis),
                            ),
                            x='year:N',
                            tooltip=['Month', chart],
                            color='year:N',
                            column=alt.X('monthStr:N', title="Month")
                        )
                        .interactive(
                        ).properties(
                            height=chartHeight
                        )
                    )
            if datasets != 1 or not monthly:
                st.altair_chart(
                    alt.Chart(data).mark_bar(
                    ).encode(
                        alt.Y(str(chart)+":Q",
                            scale=alt.Scale(zero=zeroScaleYAxis),
                        ),
                        x=alt.X('DatasetName:N', axis=alt.Axis(labels=False)),
                        tooltip=['year', chart],
                        color=alt.Color('DatasetName:N', legend=alt.Legend(title="Datasets")),
                        column=alt.X('year:N', title="Year")
                    )
                    .interactive(
                    ).properties(
                        height=chartHeight
                    )
                )

def getListings(dataset):
    listings = None
    states = tuple(MLSDict)

    # load choices from query paramaters
    target_State = st.selectbox(
        "State", states, key="target_State"+str(dataset), index=states.index(vaidateQueryParam(f"d{dataset}state", str, states[0], lambda x: x in states)))
    unit = vaidateQueryParam(f"d{dataset}unit", str, None, lambda x: x in ('CountyOrParish', 'City', 'PostalCode'))
    target_counties = None if unit != 'CountyOrParish' else vaidateQueryParam(f"d{dataset}target_units", list, None, lambda x: all(map(lambda y: isinstance(y, str), x)))
    target_cities = None if unit != 'City' else vaidateQueryParam(f"d{dataset}target_units", list, None, lambda x: all(map(lambda y: isinstance(y, str), x)))
    target_zip_codes = None if unit != 'PostalCode' else vaidateQueryParam(f"d{dataset}target_units", list, None, lambda x: all(map(lambda y: isinstance(y, str), x)))
    target_units = None
    
    # Select by County
    if not target_cities and not target_zip_codes:
        # Get the names of the counties in the DB
        counties = MLSDict[target_State].counties # Get the list of all the counties in the state from the MLS
        target_counties = list(filter(lambda x: x in counties, target_counties)) if target_counties else None # Populate counties from the url paramater, but only if we have those counties in the MLS
        target_counties = st.multiselect("County", counties, key="target_counties"+str(dataset), default=target_counties)
        if target_counties:
            target_units = target_counties
            unit = "CountyOrParish"

    # Select by City
    if not target_counties and not target_zip_codes:
        # Get the names and listing counts of the cities in the DB
        cityToListingsCount = MLSDict[target_State].citiesCount
        cities = list(filter(bool, list(cityToListingsCount)))
        cities.sort()
        target_cities = list(filter(lambda x: x in cities, target_cities)) if target_cities else None
        target_cities = st.multiselect("City", cities, format_func=lambda x: ' '.join(i.capitalize() for i in x.split(' ')) + ": "+str(cityToListingsCount[x]), key="target_cities"+str(dataset), default=target_cities) # Put the city in sentince case, and list the number of listings in the city after the city
        if target_cities:
            target_units = target_cities
            unit = "City"

    # Select by Zip 
    if not target_counties and not target_cities:
        # Get the names of the zip codes in the DB
        zipCodes = MLSDict[target_State].zips
        target_zip_codes = list(filter(lambda x: x in zipCodes, target_zip_codes)) if target_zip_codes else None
        target_zip_codes = st.multiselect("Zip Code", zipCodes, key="target_zip_codes"+str(dataset), default=target_zip_codes)
        if target_zip_codes:
            target_units = target_zip_codes
            unit = "PostalCode"

    listings = MLSDict[target_State].getListings(unit, target_units)
    return target_State, unit, target_units, listings

def filterListings(listings, dataset):
    # Filter listings
    filtersPossibilities = ["Property Type", "Listing Status", "List Price", "Bathrooms Count", "Bedrooms Count", "Lot Size Square Feet", "Price Per Square Foot", "Outliers", "Year Built"]
    filteredListings = listings
    filtersDict = json.loads(vaidateQueryParam(f"d{dataset}filter", str, "{}"))
    filtersParamList = list(filter(lambda x: x in filtersPossibilities, filtersDict))
    filtersList = st.multiselect("Data Filters", filtersPossibilities, filtersParamList if filtersParamList else ["Property Type"], key="filtersList"+str(dataset))
    if listings is not None and not listings.empty:
        if filtersList:
            # This would be a good spot for another expander if nesting them was allowed, maybe revisit later if the feature changes
            if "Property Type" in filtersList:
                dictType = filtersDict.get("Property Type")
                typesPresent = list(filter(bool, set(listings.PropertyType.tolist()))) # The filter removes "None" from the list
                dictType = dictType if dictType and isinstance(dictType, list) and all(map(lambda x: x in typesPresent, dictType)) else None
                typesExcludingRent = list(filter(lambda x: "rent" not in x.lower() and "lease" not in x.lower(), typesPresent)) if typesPresent else []
                if 'S-Closed/Rented' in typesPresent: # This property type includes the word rent, but can also mean just any closed listing
                    typesExcludingRent.append('S-Closed/Rented')
                types = st.multiselect("Property Type", typesPresent, default=dictType if dictType else (None if filtersDict else typesExcludingRent), key="types"+str(dataset))
                filteredListings = filteredListings.loc[filteredListings["PropertyType"].isin(types)]
                filtersDict["Property Type"] = types

            if "List Price" in filtersList:
                dictPrice = filtersDict.get("List Price")
                dictPrice = dictPrice if dictPrice and isinstance(dictPrice, list) and len(dictPrice) == 2 and isinstance(dictPrice[0], int) and isinstance(dictPrice[1], int) and dictPrice[0] <= dictPrice[1] else None
                min_price = st.number_input(
                    "Min List Price", value=dictPrice[0] if dictPrice else 0, step=50000, key="min_price"+str(dataset))
                max_price = st.number_input(
                    'Max List Price', value=dictPrice[1] if dictPrice else int(max(1, filteredListings['ListPrice'].max())), step=50000, key="max_price"+str(dataset))
                filteredListings = filteredListings[filteredListings['ListPrice'] <= max_price][filteredListings['ListPrice'] >= min_price]
                filtersDict["List Price"] = (min_price, max_price)

            if "Bathrooms Count" in filtersList:
                dictBathCount = filtersDict.get("Bathrooms Count")
                dictBathCount = dictBathCount if dictBathCount and isinstance(dictBathCount, list) and len(dictBathCount) == 2 and isinstance(dictBathCount[0], int) and isinstance(dictBathCount[1], int) and 0 <= dictBathCount[0] <= dictBathCount[1] <= 10 else None
                (min_bathrooms, max_bathrooms) = st.slider("Bathrooms Count", 0.0, 10.0, dictBathCount if dictBathCount else (0.0, 10.0), 0.5, key="bathrooms"+str(dataset))
                filteredListings = filteredListings[filteredListings['BathroomsTotalDecimal'] <= max_bathrooms][filteredListings['BathroomsTotalDecimal'] >= min_bathrooms]
                filtersDict["Bathrooms Count"] = (min_bathrooms, max_bathrooms)

            if "Bedrooms Count" in filtersList:
                dictBedCount = filtersDict.get("Bedrooms Count")
                dictBedCount = dictBedCount if dictBedCount and isinstance(dictBedCount, list) and len(dictBedCount) == 2 and isinstance(dictBedCount[0], int) and isinstance(dictBedCount[1], int) and 0 <= dictBedCount[0] <= dictBedCount[1] <= 10 else None
                (min_bedrooms, max_bedrooms) = st.slider("Bedrooms Count", 0, 10, dictBedCount if dictBedCount else (0, 10), 1, key="bedrooms"+str(dataset))
                filteredListings = filteredListings[filteredListings['BedroomsTotal'] <= max_bedrooms][filteredListings['BedroomsTotal'] >= min_bedrooms]
                filtersDict["Bedrooms Count"] = (min_bedrooms, max_bedrooms)

            if "Lot Size Square Feet" in filtersList:
                dictLSSqft = filtersDict.get("Lot Size Square Feet")
                dictLSSqft = dictLSSqft if dictLSSqft and isinstance(dictLSSqft, list) and len(dictLSSqft) == 2 and isinstance(dictLSSqft[0], int) and isinstance(dictLSSqft[1], int) and dictLSSqft[0] <= dictLSSqft[1] else None
                min_sqft = st.number_input(
                    "Min Square Feet", value=dictLSSqft[0] if dictLSSqft else 0, step=50, key="min_sqft"+str(dataset))
                max_sqft = st.number_input(
                    'Max Square Feet', value=dictLSSqft[1] if dictLSSqft else int(10000 if str(filteredListings['LotSizeSquareFeet'].max()) == 'nan' else filteredListings['LotSizeSquareFeet'].max()), step=50, key="max_sqft"+str(dataset))
                filteredListings = filteredListings[filteredListings['LotSizeSquareFeet'] <= max_sqft][filteredListings['LotSizeSquareFeet'] >= min_sqft]
                filtersDict["Lot Size Square Feet"] = (min_sqft, max_sqft)

            if "Price Per Square Foot" in filtersList:
                dictPPSqft = filtersDict.get("Price Per Square Foot")
                dictPPSqft = dictPPSqft if dictPPSqft and isinstance(dictPPSqft, list) and len(dictPPSqft) == 2 and isinstance(dictPPSqft[0], float) and isinstance(dictPPSqft[1], float) and dictPPSqft[0] <= dictPPSqft[1] else None
                filteredListings = filteredListings[filteredListings['ListPricePerSQFT'] != np.Inf]
                min_ppsqft = st.number_input(
                    "Min Price Per Square Feet", value=dictPPSqft[0] if dictPPSqft else 0.0, step=0.01, key="min_ppsqft"+str(dataset))
                max_ppsqft = st.number_input(
                    'Max Price PerSquare Feet', value=dictPPSqft[1] if dictPPSqft else filteredListings['ListPricePerSQFT'].max(), step=0.01, key="max_ppsqft"+str(dataset)) # It is possible to get a value of infinity for the mmax ListPricePerSQFT, so we need to cap the max possible value. 
                filteredListings = filteredListings[filteredListings['ListPricePerSQFT'] <= max_ppsqft][filteredListings['ListPricePerSQFT'] >= min_ppsqft]
                filtersDict["Price Per Square Foot"] = (min_ppsqft, max_ppsqft)

            if "Year Built" in filtersList:
                dictYearBuilt = filtersDict.get("Year Built")
                dictYearBuilt = dictYearBuilt if dictYearBuilt and isinstance(dictYearBuilt, list) and len(dictYearBuilt) == 2 and isinstance(dictYearBuilt[0], float) and isinstance(dictYearBuilt[1], float) and dictYearBuilt[0] <= dictYearBuilt[1] else None
                min_yearBuilt = st.number_input(
                    "Min Year Built", value=dictYearBuilt[0] if dictYearBuilt else 1920, step=10, key="min_yearBuilt"+str(dataset))
                max_yearBuilt = st.number_input(
                    'Max Year Built', value=dictYearBuilt[1] if dictYearBuilt else int(2100 if str(filteredListings['YearBuilt'].max()) == 'nan' else filteredListings['YearBuilt'].max()), step=10, key="max_yearBuilt"+str(dataset))
                filteredListings = filteredListings[filteredListings['YearBuilt'] <= max_yearBuilt][filteredListings['YearBuilt'] >= min_yearBuilt]
                filtersDict["Year Built"] = (min_yearBuilt, max_yearBuilt)

            if "Listing Status" in filtersList:
                dictStatus = filtersDict.get("Listing Status")
                statusesPresent = list(set(listings.StandardStatus.tolist()))
                dictStatus = dictStatus if dictStatus and isinstance(dictStatus, list) and all(map(lambda x: x in statusesPresent, dictStatus)) else None
                status = st.multiselect("Listing Status", statusesPresent, key="status"+str(dataset), default=statusesPresent if statusesPresent else None)
                filteredListings = filteredListings.loc[filteredListings["StandardStatus"].isin(status)]
                filtersDict["Listing Status"] = status

            if "Outliers" in filtersList: 
                # This filters out univariate outliers from a single field. If a cell value in the selected field column is an outlier in that column, the whole row is removed. This does not remove outliers month to month 
                methods = ["IQRs", "Standard Deviations"]
                tails = ["Both", "Low Outliers", "High Outliers"]
                fields = list(set(filteredListings.columns)-set(("_id", "StandardStatus", "OffMarketDate", "City", "OnMarketDate", "YearBuilt", "CloseDate", "StateOrProvince", "PostalCode", "Latitude", "Longitude", "CountyOrParish", "PropertyType")))

                dictOutliers = filtersDict.get("Outliers") # Takes the form (method:str, tails: str, field: str, limit:float)
                dictOutliers = dictOutliers if dictOutliers and isinstance(dictOutliers, Sequence) and len(dictOutliers) == 4 and dictOutliers[0] in methods and dictOutliers[1] in tails and dictOutliers[2] in fields and isinstance(dictOutliers[3], float) else None


                method = st.selectbox("Filter out outliers by", methods, index=methods.index(dictOutliers[0]) if dictOutliers else 0)
                tails = st.selectbox("Remove", tails, index=tails.index(dictOutliers[1]) if dictOutliers else 0)
                field = st.selectbox("Filter all listings by field", fields, index=fields.index(dictOutliers[2]) if dictOutliers else 0) # There is probably a better way to do this instead of hardcoding, like excluding listings that aren't numerical by data type, or a mapping between fields and whether they're quantitative or categorical
                # The z score is a measure of how many standard deviations below or above the population mean a raw score is
                limit = st.number_input(method, value=dictOutliers[3] if dictOutliers else (1.5 if method == "IQRs" else 3.0))
                
                # filteredListings[field] = filteredListings[field].dropna()
                filteredListings = filteredListings.loc[filteredListings[field] != 0.0]
                iqr = stats.iqr(filteredListings[field])
                q25, q75 = np.percentile(filteredListings[field], 25), np.percentile(filteredListings[field], 75)
                lower, upper = q25 - limit*iqr, q75 + limit*iqr
  
                filteredListings = filteredListings.loc[
                    {
                        "Both": (lambda fl: (fl[field] > lower) & (fl[field] < upper)) if method == 'IQRs' else 
                            (lambda fl: ((stats.zscore(fl[field]) > -limit) & (stats.zscore(fl[field]) < limit))), 
                        "Low Outliers": (filteredListings[field] > lower) if method == 'IQRs' else 
                            (stats.zscore(filteredListings[field]) > -limit), 
                        "High Outliers": (filteredListings[field] < upper) if method == 'IQRs' else 
                            (stats.zscore(filteredListings[field]) < limit)
                    }[tails]
                ]
                filtersDict["Outliers"] = (method, tails, field, limit)


            # filterOnMarketDate
            # filterDaysOnMarket
    return json.dumps(filtersDict), filteredListings

def getConfidenceRegion(grouped: pd.core.groupby.generic.DataFrameGroupBy, interval: int) -> Sequence[Sequence[float]]:
    """Creates two lists, one for the low confidence region, and one for the high confidence region

    Args:
        grouped (pandas.core.groupby.generic.DataFrameGroupBy): [description]
        interval (int): The confidence interval, an int between 1 and 99

    Returns:
        Sequence[Sequence[float]]: returns a tuple containing 2 lists, the first of which is the High confidence region, the second is the low
    """
    # sourced from https://stackoverflow.com/questions/53519823/confidence-interval-in-python-dataframe
    stat = grouped.agg(['mean', 'count', 'std'])
    ci_hi = []
    ci_lo = []
    for i in stat.index:
        # This is never how this should be done, its so much slower than handing everything over to pandas or numpy to do it
        m, c, s = stat.loc[i]
        ci_hi.append(m + stats.norm.ppf(1-((100-confidenceInterval)/2)/100)*s/math.sqrt(c))
        ci_lo.append(m - stats.norm.ppf(1-((100-confidenceInterval)/2)/100)*s/math.sqrt(c))
    return (ci_hi, ci_lo)

def processAndLabel(function, df, label):
    processedDf = function(df)
    processedDf["Label"] = label
    return processedDf

with st.sidebar.expander("Dataset 1", expanded=True):
    listings = None
    filteredListings = None
    d1state, d1unit, d1target_units, listings = getListings(1)
    d1name = st.text_input("Dataset 1 Name", value=vaidateQueryParam("d1name", str, "Dataset 1", lambda x: 1 <= len(x)))
    dataset_names["Dataset 1"] = d1name
    if listings is not None:
        d1filter, filteredListings = filterListings(listings, 1)
        listings_group.append(listings)
        filteredListings_group[0] = filteredListings
        if datasets == 1:
            # mabelo: changed experimental_set_query_params to query_params
            # st.experimental_set_query_params(datasets=datasets, d1name=d1name, d1unit=d1unit, d1target_units=d1target_units, d1state=d1state, d1filter=d1filter)
            # st.query_params
            st.query_params.update(datasets=datasets, d1name=d1name, d1unit=d1unit, d1target_units=d1target_units, d1state=d1state, d1filter=d1filter)
    else:
        filteredListings_group[0] = None

if datasets >= 2: # I think i might benefit from implementing an object oriented structure here (update, I should make many things objects: the filters, the states, the datasets)
    with st.sidebar.expander("Dataset 2", expanded=True):
        listings2 = None
        filteredListings2 = None
        d2state, d2unit, d2target_units, listings2 = getListings(2)
        d2name = st.text_input("Dataset 2 Name", value=vaidateQueryParam("d2name", str, "Dataset 2", lambda x: 1 <= len(x)))
        dataset_names["Dataset 2"] = d2name
        if listings2 is not None:
            d2filter, filteredListings2 = filterListings(listings2, 2)
            listings_group.append(listings2)
            filteredListings_group[1] = filteredListings2
            if datasets == 2:
                st.experimental_set_query_params(datasets=datasets, d1name=d1name, d1unit=d1unit, d1target_units=d1target_units, d1state=d1state, d1filter=d1filter, d2name=d2name, d2unit=d2unit, d2target_units=d2target_units, d2state=d2state, d2filter=d2filter)
        else:
            filteredListings_group[1] = None

if datasets >= 3: 
    with st.sidebar.expander("Dataset 3", expanded=True):
        listings3 = None
        filteredListings3 = None
        d3state, d3unit, d3target_units, listings3 = getListings(3)
        d3name = st.text_input("Dataset 3 Name", value=vaidateQueryParam("d3name", str, "Dataset 3", lambda x: 1 <= len(x)))
        dataset_names["Dataset 3"] = d3name
        if listings3 is not None:
            d3filter, filteredListings3 = filterListings(listings3, 3)
            listings_group.append(listings3)
            filteredListings_group[2] = filteredListings3
            if datasets == 3:
                st.experimental_set_query_params(datasets=datasets, d1name=d1name, d1unit=d1unit, d1target_units=d1target_units, d1state=d1state, d1filter=d1filter, d2name=d2name, d2unit=d2unit, d2target_units=d2target_units, d2state=d2state, d2filter=d2filter, d3name=d3name, d3unit=d3unit, d3target_units=d3target_units, d3state=d3state, d3filter=d3filter)
        else:
            filteredListings_group[2] = None

if datasets >= 4: 
    with st.sidebar.expander("Dataset 4", expanded=True):
        listings4 = None
        filteredListings4 = None
        d4state, d4unit, d4target_units, listings4 = getListings(4)
        d4name = st.text_input("Dataset 4 Name", value=vaidateQueryParam("d4name", str, "Dataset 4", lambda x: 1 <= len(x)))
        dataset_names["Dataset 4"] = d4name
        if listings4 is not None:
            d4filter, filteredListings4 = filterListings(listings4, 4)
            listings_group.append(listings4)
            filteredListings_group[3] = filteredListings4
            if datasets == 4:
                st.experimental_set_query_params(datasets=datasets, d1name=d1name, d1unit=d1unit, d1target_units=d1target_units, d1state=d1state, d1filter=d1filter, d2name=d2name, d2unit=d2unit, d2target_units=d2target_units, d2state=d2state, d2filter=d2filter, d3name=d3name, d3unit=d3unit, d3target_units=d3target_units, d3state=d3state, d3filter=d3filter, d4name=d4name, d4unit=d4unit, d4target_units=d4target_units, d4state=d4state, d4filter=d4filter)
        else:
            filteredListings_group[3] = None

filteredListings_group = list(filter(lambda x: x is not None and not x.empty, filteredListings_group))

if filteredListings_group != [] and all(map(lambda x: x is not None and not x.empty, filteredListings_group)):
# if filteredListings is not None:
    # Select which types of charts to display
    chartViews = st.sidebar.multiselect("Chart Type Views", ["Time Series Line Chart", "Stacked Line Chart", "Grouped Bar Chart"], default=["Time Series Line Chart", "Stacked Line Chart", "Grouped Bar Chart"])
    chartHeight = st.sidebar.number_input("Chart Height", value=500, step=100)
    confidenceRegion = st.sidebar.checkbox("Confidence Region", value=False)
    if confidenceRegion:
        confidenceInterval = st.sidebar.number_input("Confidence Region", min_value=1, max_value=99, value=95, step=5)

    # Display Listings
    # st.title(f'Listings in {", ".join(target_counties)}. {functools.reduce(lambda x, y: x + y.shape[0], filteredListings_group, 0)} of {functools.reduce(lambda x, y: x + y.shape[0], listings_group, 0)} total')

    # Plot Listings
    # could use st.line_chart (simple) or st.write (pandas dataframe) (maybe select multiple cities with st.multiset? definitely select multiple something somehow. would that selection apply to the maps too?)
    # either way, do the logic in a dataframe, spend some time trying to make it leverage data frame functionalities as much as possible
    # Should use a plotly chart, see https://fullstackstation.com/streamlit-components-demo, show me: "Streamlit's components", select the component "charts.plotly_chart"
    # Why stop at a chart of monthly averages? What if we draw every point, cluster, draw confidence intervals, etc. 

    charts = ['New Listings', 'Homes for Sale', 'Pending Sales', 'Closed Sales', 'Days on Market', 'Absorption Rate', 'Sales Price', 'Price Per Sq Ft', 'Original List Price', 'Pct of Original Price', 'Pct of Last List Price', 'Dollar Volume', 'Sales Lead to Close Ratio', 'Months Supply'] # Excluded: 'Shows to Pending', 'Shows Per Listing', 
    # views = st.multiselect("Views", charts, default=["Days on Market", "Sales Price", 'Price Per Sq Ft', 'Pct of Last List Price', "Dollar Volume"])
    views = charts
    rolling = True
    if len(views) != 0:
        col1, space, col2 = st.columns([2, 1, 2])
        with col1:
            years = st.slider('Years of data to chart', 1, maxYears, 3)
        with col2:
            if "Time Series Line Chart" in chartViews:
                rollingMonths = st.slider('Months to include in the rolling Average', 0, 24, 0)
            else:
                rollingMonths = 0
        if rollingMonths <= 1:
            rolling = False

    # New Listings
    # A count of the properties that have been newly listed on the market in a given month.
    if "New Listings" in views:
        # Chart new listings
        chart = "New Listings"
        title = "New Listings Per Month"
        description = "A count of the properties that have been newly listed on the market in a given month."
        def getNewListings(filteredListings):
            newListings = filteredListings.groupby([filteredListings['OnMarketDate'].dt.year.rename('year'), filteredListings['OnMarketDate'].dt.month.rename('month')], as_index=False).agg({'count'})[["_id"]]
            newListings["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), newListings.index)))
            newListings.columns = ['New Listings', 'Month']
            return newListings
        newListings = pd.concat(map(
            lambda x, y: processAndLabel(getNewListings, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))
        ))
        makeChart(chart, title, newListings, rolling, description)

    # Homes for Sale
    # From infosparks: The number of properties available for sale in active status at the end of a given month. Also known as inventory. For this metric, the "12 months" calculation is the average of the most recent 12 months of data.
    if "Homes for Sale" in views or 'Sales Lead to Close Ratio' in views or 'Absorption Rate' in views:
        # Chart homes for sale
        chart = "Homes for Sale"
        title = chart
        description = "The number of homes that were for sale at at least one point during the given month."
        def getHomesForSale(filteredListings):
            dr = pd.date_range(
                end=datetime.today(),
                periods=12*maxYears+2, # + 2 because the max time for rolling average is 24 months
                freq="MS",
                normalize=True,
                name="Month"
            )
            homesForSale = pd.DataFrame(dr)
            def getHomesForSaleMonth(givenMonth):
                # return the number of listings that were put on market before the last day of the month, and that closed after the first day of the month
                # If a listing has not closed, its close date will be 1800-01-01, so we need to account for that too. 
                givenMonth = pd.to_datetime(givenMonth)
                allHomesForSale = filteredListings.loc[
                    lambda filteredListings: 
                        (filteredListings["OnMarketDate"] <= givenMonth + MonthEnd(1)) & 
                        ((filteredListings["CloseDate"] >= givenMonth) | (~filteredListings["StandardStatus"].isin(["Closed", "Sold", "Expired", "Canceled", "Cancelled", "Killed", "Under Agreement", "Rented", "Deposit", "S-Closed/Rented", 'T-Temp Off Market', 'X-Expired', 'Sold-REO', 'Rented-Leased', 'Sold-Short Sale', 'Withdrawn']))),
                    ["OnMarketDate", "CloseDate", "DaysOnMarket", "StandardStatus"]
                ]
                return allHomesForSale.shape[0]
            homesForSale["Homes for Sale"] = homesForSale["Month"].apply(getHomesForSaleMonth)
            emptyMonths = homesForSale.loc[homesForSale["Homes for Sale"] == 0].index
            homesForSale.drop(emptyMonths, axis=0, inplace=True)
            return homesForSale
        
        processHomesForSale = lambda x, y: processAndLabel(getHomesForSale, x, y) # more readable than processHomesForSale = functools.partial(processAndLabel, getHomesForSale)
        homesForSale = pd.concat(
            map(
                processHomesForSale,
                filteredListings_group,
                map(lambda x: "Dataset " + str(x), range(1,datasets+1)) #TODO
            )
        )
        if "Homes for Sale" in views:
            makeChart(chart, title, homesForSale, rolling, description)

    # Closed Sales
    # From infosparks: A count of the actual sales that have closed in a given month. For those familiar with NorthstarMLS data fields, this includes SOLD and COMP SOLD figures. Calculations are based on sold data.
    if "Closed Sales" in views or 'Sales Lead to Close Ratio' in views or 'Absorption Rate' in views:
        chart = "Closed Sales"
        title = chart
        description = "The number of homes that have closed during the given month"
        def getClosedSales(filteredListings):
            closedSales = filteredListings.groupby([filteredListings['CloseDate'].dt.year.rename('year'), filteredListings['CloseDate'].dt.month.rename('month')], as_index=False).agg({'count'})[["_id"]]
            closedSales["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), closedSales.index)))
            closedSales.columns = ['Closed Sales', 'Month']
            return closedSales
        closedSales = pd.concat(map(
            lambda x, y: processAndLabel(getClosedSales, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1)) #TODO
        ))
        if "Closed Sales" in views:
            makeChart(chart, title, closedSales, rolling, description)

    # Absorption Rate (formerly Sales Lead to Close Ratio)
    # From mike: Closed listings to total inventory https://www.investopedia.com/terms/a/absorption-rate.asp#:~:text=If%20buyers%20snap%20up%20100,100%20homes%20sold%2Fmonth
    if 'Absorption Rate' in views or 'Absorption Rate' in views:
        # Take the total number of homes on the market during a given month as the number of sales leads, 
        # and the total number of homes sold during that month the number of successfully closed sales. 
        # Essentially: total number of homes sold this month / total number of homes on market this month
        # total number of homes sold this month: "Closed Sales"
        # total number of homes on market this month: "Homes for Sale"
        chart = 'Absorption Rate'
        title = chart
        description = 'The ratio of the total number of homes sold in a month to the total number of homes on market that month. AKA Sales Lead to Close Ratio'
        def getAbsorptionRate(filteredListings):
            homesForSale = getHomesForSale(filteredListings)
            closedSales = getClosedSales(filteredListings)
            closeRatio = pd.DataFrame(homesForSale['Month'])
            homesForSale.index = homesForSale["Month"]
            closedSales.index = closedSales["Month"]
            def getAbsorptionRateMonth(month):
                try:
                    return closedSales.loc[month, 'Closed Sales'] / homesForSale.loc[month, 'Homes for Sale']
                except:
                    # There can be indices present in closeRatio that arent present in either closedSales or homesForSale
                    return None
            closeRatio[chart] = closeRatio["Month"].apply(getAbsorptionRateMonth).dropna()
            return closeRatio
        closeRatio = pd.concat(map(
            lambda x, y: processAndLabel(getAbsorptionRate, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))
        ))
        if chart in views:
            makeChart(chart, title, closeRatio, rolling, description, sumTotalData=False, percent=True)

    # Months Supply
    # From infosparks (Absorption Rate): The inventory of homes for sale at the end of a given month, divided by the average monthly Pending Sales from the last 12 months. Also known as absorption rate.
    if 'Months Supply' in views:
        # for each month the months supply would be:
        # (the number of homes for sale that month / total sales in the last 12 months) * 12 months
        # The unit on the y axis of the graph would be months because the absorption rate is measured in months
        # the number of homes for sale that month = homesForSale
        # total sales in the last 12 months = rolling average of closedSales
        chart = 'Months Supply'
        title = chart
        description = 'The rate at which the market eliminates inventory measured in months to absorb current inventory. Rate based on yearly average sales. For a given month, the months supply is the listings for sale that month dividied by the yearly average closed sales.'
        def getMonthsSupply(filteredListings):
            homesForSale = getHomesForSale(filteredListings)
            homesForSale.columns = ['Month', 'Homes for Sale']
            homesForSale.index = homesForSale.Month
            closedSales = getClosedSales(filteredListings)
            absorptionRate = pd.DataFrame(homesForSale['Month'])
            closedSalesRolling = closedSales['Closed Sales'].rolling(window=12).mean()
            closedSalesRolling.index = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), closedSalesRolling.index)))
            def getMonthsSupplyMonth(month):
                try:
                    return homesForSale.loc[month, 'Homes for Sale'] / closedSalesRolling.loc[month]
                except:
                    return None

            absorptionRate[chart] = absorptionRate["Month"].apply(getMonthsSupplyMonth).dropna(how='any')
            return absorptionRate
        absorptionRate = pd.concat(map(
            lambda x, y: processAndLabel(getMonthsSupply, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))# TODO
        ))
        makeChart(chart, title, absorptionRate, rolling, description, sumTotalData=False)

    # Pending Sales 
    # From infosparks: A count of the properties on which contracts have been accepted in a given month
    # This will be the same chart as New Listings Per Month filtered to only include listings with a status of pending
    if "Pending Sales" in views:
        chart = "Pending Sales "
        title = "Monthly Total Pending Sales"
        description = "The number of properties put on market each month with a current status of Pending"
        def getPendingSales(filteredListings):
            pendingSales = filteredListings.loc[filteredListings["StandardStatus"].isin(["Pending", 'P-Pending Sale'])]
            pendingSales = pendingSales.groupby([pendingSales['OnMarketDate'].dt.year.rename('year'), pendingSales['OnMarketDate'].dt.month.rename('month')], as_index=True).agg({'count'})[['OnMarketDate']]
            pendingSales["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), pendingSales.index)))
            pendingSales.columns = [chart, 'Month']
            return pendingSales
        pendingSales = pd.concat(map(
            lambda x, y: processAndLabel(getPendingSales, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
        ))
        makeChart(chart, title, pendingSales, rolling, description, sumTotalData=True)

    # Days on Market
    # From infosparks: Median or average number of days between when a property is listed and when an offer is accepted in a given month. Calculations are based on sold data.*
    # The median number of days on market for listings closed this month
    if "Days on Market" in views:
        chart = "Days on Market"
        title = "Median Days on Market per Month" if not confidenceRegion else "Mean Days on Market per Month"
        description = "The median number of days on market for listings closed each month" if not confidenceRegion else "The mean number of days on market for listings closed each month"
        def getDaysOnMarket(filteredListings):
            grouped = filteredListings.groupby([filteredListings['CloseDate'].dt.year.rename('year'), filteredListings['CloseDate'].dt.month.rename('month')], as_index=True)[['DaysOnMarket']]
            if confidenceRegion:
                daysOnMarket = grouped.mean()
                ci_hi, ci_lo = getConfidenceRegion(grouped, confidenceInterval)
            else:
                daysOnMarket = grouped.median()
            daysOnMarket["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), daysOnMarket.index)))
            daysOnMarket.columns = [chart, 'Month']
            if confidenceRegion:
                daysOnMarket['ci_hi'] = ci_hi
                daysOnMarket['ci_lo'] = ci_lo
            return daysOnMarket
        daysOnMarket = pd.concat(map(
            lambda x, y: processAndLabel(getDaysOnMarket, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
        ))
        makeChart(chart, title, daysOnMarket, rolling, description, sumTotalData=False)

    # Sales Price
    # From infosparks: Calculations are based on sold data. Prices do not account for seller concessions. Median represents the point at which half of the homes that sold in a given month were priced higher and half were priced lower. Average is the mean sales price for all closed sales in a given month.*
    if "Sales Price" in views or 'Pct of Original Price' in views:
        chart = "Sales Price"  
        title = "Median Sales Price" if not confidenceRegion else "Mean Sales Price"
        description = "The median close price for listings closed each month" if not confidenceRegion else "The mean close price for listings closed each month"
        def getSalesPrice(filteredListings):
            grouped = filteredListings.groupby([filteredListings['CloseDate'].dt.year.rename('year'), filteredListings['CloseDate'].dt.month.rename('month')], as_index=True)[['ClosePrice']]
            if confidenceRegion:
                salesPrice = grouped.mean(numeric_only=True)
                ci_hi, ci_lo = getConfidenceRegion(grouped, confidenceInterval)
            else:
                salesPrice = grouped.median(numeric_only=True)
            salesPrice["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), salesPrice.index)))
            salesPrice.columns = [chart, 'Month']
            if confidenceRegion:
                salesPrice['ci_hi'] = ci_hi
                salesPrice['ci_lo'] = ci_lo
            return salesPrice
        salesPrice = pd.concat(map(
            lambda x, y: processAndLabel(getSalesPrice, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
        ))
        if "Sales Price" in views:
            makeChart(chart, title, salesPrice, rolling, description, sumTotalData=False)

    # Price Per Sq Ft
    # From infosparks: Calculated by taking an average or a median of closed sales price divided by square footage for each individual listing in the current period. Calculations are based on sold data. Prices do not account for seller concessions.*
    if "Price Per Sq Ft" in views:
        chart = "Price Per Sq Ft"
        title = "Median List Price Per Building Sq Ft"
        description = "The median list price per square foot of building area for listings closed each month"
        def getPpsqft(filteredListings):
            ppsqft = filteredListings[filteredListings["ListPricePerSQFT"] != np.Inf]
            ppsqft = ppsqft.groupby([ppsqft['CloseDate'].dt.year.rename('year'), ppsqft['CloseDate'].dt.month.rename('month')], as_index=True)[['ListPricePerSQFT']].median(numeric_only=True)
            ppsqft["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), ppsqft.index)))
            ppsqft.columns = [chart, 'Month']
            ppsqft = ppsqft.replace(0.000000, np.nan).dropna(axis=0, how="any")
            return ppsqft
        ppsqft = pd.concat(map(
            lambda x, y: processAndLabel(getPpsqft, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
        ))
        makeChart(chart, title, ppsqft, rolling, description=description, sumTotalData=False)

    # Original List Price
    # From infosparks: Median or average of the first price of a home listing.
    if "Original List Price" in views or 'Pct of Original Price' in views:
        chart = "Original List Price"
        title = "Median Original List Price"
        description = "The median original list price for listings closed each month"
        origPrice = None
        def getOrigPrice(filteredListings):
            origPrice = filteredListings.groupby([filteredListings['CloseDate'].dt.year.rename('year'), filteredListings['CloseDate'].dt.month.rename('month')], as_index=True)[['OriginalListPrice']].median(numeric_only=True)
            origPrice["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), origPrice.index)))
            origPrice.columns = [chart, 'Month']
            return origPrice
        try:
            origPrice = pd.concat(map(
                lambda x, y: processAndLabel(getOrigPrice, x, y),
                filteredListings_group,
                map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
            ))
            if "Original List Price" in views:
                makeChart(chart, title, origPrice, rolling, description, sumTotalData=False)
        except KeyError:
            if "Original List Price" in views:
                with st.expander(chart):
                    st.text("No original list prices found for selected listings.")

    # Pct of Original Price
    # On infosparks: Shown as an actual percentange, from 0% to 100%
    # From infosparks: Percentage found when dividing a listing’s sales price by its original list price, then taking the average for all sold listings in a given month, not accounting for seller concessions. Calculations are based on sold data. Example:
    #     Example: A property is listed at $200,000, reduced to $190,000 and taken off the market. Then the same property is listed again at $180,000 a few months later and is further reduced to $175,000. It closes for $160,000. We take the ratio of $160,000:$180,000 or 88.9% instead of $160,000:$200,000 because a new listing ID was issued.
    if 'Pct of Original Price' in views:
        chart = "Percent of Original List Price"
        title = "Median Sale Percent of Original List Price"
        description = "Percentage found when dividing a listing’s sales price by its original list price, then taking the average for all sold listings in a given month. \nSales with a Percent of Original List Price that is less than 50 percent or more than 200 percent are added to the sales count but are not factored into Average Sales Price Percent of Original List Price"
        def getPop(filteredListings):
            pop = filteredListings[['CloseDate', 'ClosePrice', 'OriginalListPrice']].replace(0.0, np.nan).dropna(axis=0, how="any") 
            pop[chart] = (pop.ClosePrice / pop.OriginalListPrice) 
            pop = pop.loc[ lambda pop: (pop[chart] >= .5) & (pop[chart] <= 2.0) ]
            pop = pop.groupby([pop['CloseDate'].dt.year.rename('year'), pop['CloseDate'].dt.month.rename('month')], as_index=True)[[chart]].mean(numeric_only=True)
            pop["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), pop.index)))
            pop.columns = [chart, 'Month']
            return pop
        try:
            pop = pd.concat(map(
                lambda x, y: processAndLabel(getPop, x, y),
                filteredListings_group,
                map(lambda x: "Dataset " + str(x), range(1,datasets+1))
            ))
            makeChart(chart, title, pop, rolling, description, sumTotalData=False, zeroScaleYAxis=False, percent=True)
        except KeyError:
            with st.expander(chart):
                st.text("No original list prices found for selected listings.")

    # Pct of Last List Price
    # From infosparks: Percentage found when dividing a listing's sales price by its last listed price, then taking the average for all properties sold in a given month, not accounting for seller concessions. Example:
    #     So using the example from Percent of Original List Price, that same property that was originally listed at $200,000 taken off the market, relisted at $180,000 a few months later, reduced to $175,000 and sold for $160,000 would have a ratio of $160,000:$175,000 or 91.4%.
    if 'Pct of Last List Price' in views:
        chart = 'Percent of Last List Price'
        title = "Average Sales Price Percent of Last List Price"
        description = "Percentage found when dividing a listing’s sales price by its last list price, then taking the average for all sold listings in a given month. \nSales with a Percent of Last List Price that is less than 50 percent or more than 200 percent are added to the sales count but are not factored into Average Sales Price Percent of Last List Price"
        def getPllp(filteredListings):
            pllp = filteredListings[['CloseDate', 'ClosePrice', 'ListPrice']].replace(0.0, np.nan).dropna(axis=0, how="any") # Get rid of listings that haven't closed yet (close price will be 0)
            pllp[chart] = (pllp.ClosePrice / pllp.ListPrice)
            pllp = pllp.loc[ lambda pllp: (pllp[chart] >= .5) & (pllp[chart] <= 2.0) ]     # * Sales with a Percent of Original Price that is less than 50 percent or more than 200 percent, are added to the sales count but are not factored into price, price per square foot or days on market.   
            pllp = pllp.groupby([pllp['CloseDate'].dt.year.rename('year'), pllp['CloseDate'].dt.month.rename('month')], as_index=True)[[chart]].mean(numeric_only=True)
            pllp["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), pllp.index)))
            pllp.columns = [chart, 'Month']
            return pllp
        pllp = pd.concat(map(
            lambda x, y: processAndLabel(getPllp, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
        ))
        makeChart(chart, title, pllp, rolling, description, sumTotalData=False, zeroScaleYAxis=False, percent=True)

    # Dollar Volume
    # On infosparks: Still grouped per month
    # From infosparks: The total dollar amount of all sales for the selected criteria.
    if "Dollar Volume" in views:
        chart = "Dollar Volume"
        title = "Monthly Total Dollar Volume"
        description = "The total dollar amount of all sales for listings closed each month"
        def getDollarVol(filteredListings):
            dollarVol = filteredListings.groupby([filteredListings['CloseDate'].dt.year.rename('year'), filteredListings['CloseDate'].dt.month.rename('month')], as_index=True)[['ClosePrice']].sum(numeric_only=True)
            dollarVol["Month"] = pd.to_datetime(tuple(map(lambda x: str(int(x[0])) + '-' + str(int(x[1])), dollarVol.index)))
            dollarVol.columns = [chart, 'Month']
            return dollarVol
        dollarVol = pd.concat(map(
            lambda x, y: processAndLabel(getDollarVol, x, y),
            filteredListings_group,
            map(lambda x: "Dataset " + str(x), range(1,datasets+1))#TODO
        ))
        makeChart(chart, title, dollarVol, rolling, description, sumTotalData=True)


    # Shows to Pending
    # From infosparks: The number of showings scheduled per listing that went into pending status during the selected reporting period. Data begins in January 2012.

    # Shows Per Listing
    # From infosparks: The average number of showings scheduled on active listings per month. Data begins in January 2012.
    #     Example: There are 100 active listings during a given month for a given geography. Between those 100 active listings, there were 1,000 active days. Divide 1,000 by 30 days (for 1 month) to get 33.3. There are 500 showings during the month, so 500 divided by 33.3 equals 15.0 Shows Per Listing for the month.


    # * Sales with a Days on Market of less than 0, or a Percent of Original Price that is less than 50 percent or more than 200 percent, are added to the sales count but are not factored into price, price per square foot or days on market.
else:
    st.text("No listings match selected filters. Please use the sidebar to select datasets.")
