from pymongo import MongoClient
from flask import Flask, request, jsonify, make_response
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pandas as pd
from flask_cors import CORS
import numpy as np
import statsmodels.api as sm
import pickle
from sklearn import linear_model
from pandas import DataFrame


client = MongoClient('mongodb://krish:kNKhNwjQEDXZ@172.26.1.20:27017/')
db = client["housing-tax-prices"]
bridgedb = client["housing-prices"]
app = Flask(__name__)
CORS(app)


X = []
y = []
for collection in ["morris", "atlantic", "hudson", "essex"]:
    col = db[collection]
    query = {"SALE-PRICE": {'$gt':100}, "year": 2016, "PROPERTY-CLASS":2}
    print(collection)

    #Querying MongoDB
for out in col.find(query):
    X.append([out['LAND-VALUE'], out["SALE-PRICE"], out['CALCULATED-ACREAGE'], out['NET-VALUE']])
    y.append(out['LAST-YEAR-TAX'])


#Querying MongoDB done

X = DataFrame(X, columns=['LAND-VALUE', "SALE-PRICE", "CALCULATED-ACREAGE", "NET-VALUE"])
y = np.array(y)

X['LAST-YEAR-TAX'] = y
print(X.head())
cols = ['LAND-VALUE', "SALE-PRICE", "CALCULATED-ACREAGE", "NET-VALUE"]
X[cols] = X[cols].apply(pd.to_numeric, errors='coerce')
X = X.dropna()
X[cols] = X[cols].astype('int')
print(X.shape)

y = X['LAST-YEAR-TAX']
X = X.drop(columns = ['LAST-YEAR-TAX'])
print(X.shape)

print(f"original X {X.shape}")

print(f"new X {X.shape}")
X.astype(np.float64)
y.astype(np.float64)

#Data splitting
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

X_train = X_train.to_numpy()
X_test = X_test.to_numpy()
y_train = y_train.to_numpy()
y_test = y_test.to_numpy()

# Uncomment this to repickle
# pkl_filename = "tax_prediction_model.pkl"
# tax_prediction_model = linear_model.Lasso(alpha=0.1).fit(X_train, y_train)
# with open(pkl_filename, 'wb') as file:
#     pickle.dump(tax_prediction_model, file)
#     print("dumped the pickle")


def predict_prices(land_val, sale_price, acreage, net_value):
    with open(pkl_filename, 'rb') as file:
        tax_prediction_model = pickle.load(file)
    
    tax_prediction = tax_prediction_model.predict([[land_val, sale_price, acreage, net_value]])
    asdf = tax_prediction_model.predict(X_train)
    asdf -= y_train
    res = np.mean(np.abs(asdf)).tolist()

    # -/+ 2*res centered around the tax_prediction
    return(tax_prediction[0], res)

def get_predictions(x):
    new_res = []
    for house in x:
        tup = predict_prices(house["LAND-VALUE"], house["SALE-PRICE"],house['CALCULATED-ACREAGE'], house['NET-VALUE'])
        obj = {}
        obj["prediction"] = tup[0]
        obj["variance"] = tup[1]
        new_res.append(obj)
    return new_res

def get_features(x):
    new_res = []
    for house in x:
        obj = {}
        obj["baths"] = house["BathroomsTotalInteger"]
        obj["beds"] = house["BedroomsTotal"]
        obj["floorSpace"] = house["CALCULATED-ACREAGE"]
        obj["landSize"] = house["LotSizeSquareFeet"]
        obj["built"] = house["YearBuilt"]
        obj["transactionDate"] = house["TRANSACTION-DATE-MMDDYY"]
        obj["transactionAmount"] = house["SALE-PRICE"]
        obj["longitude"] = house["Longitude"]
        obj["latitude"] = house["Latitude"]
        new_res.append(obj)
    return new_res

def get_tax_number(s):
    tax_amt = ""
    numbers = list(map(lambda x: str(x), range(0,10)))
    if isinstance(s, int):
        return s
    for c in s:
        if c in numbers:
            tax_amt += c
    return int(tax_amt) 

def get_tax_info(x):
    new_info = []
    years = set()
    for info in x:
        if info['year'] not in years:
            obj = {}
            obj["year"] = info["year"]
            obj["taxPrice"] = get_tax_number(info["LAST-YEAR-TAX"])
            new_info.append(obj)
            years.add(info['year'])

    # ian comment the below stuff out if you need stuff to not be bad i went to bed while this is running
    # pred = x[0]
    # obj = {}
    # obj["year"] = 2018
    # obj["taxPrice"] = predict_prices(pred["LAND-VALUE"],pred["SALE-PRICE"])[0]
    # new_info.append(obj)
        
    return sorted(new_info, key=lambda x: x['year'])

@app.route('/prediction/<county>/<address>/<zip>')
def predictions(county, address, zip):
    address = " ".join(address.strip().split("_"))
    county = county.strip().lower()
    col = db[county]
    res = col.find({ "STREET-ADDRESS": address, "ZIP-CODE": int(zip)})
    res = [x for x in res]
    res = get_predictions(res)
    return jsonify({"predictions":res})
    

@app.route('/data/<county>/<address>/<zip>')
def data2(county, address, zip):
    address = " ".join(address.strip().split("_"))
    county = county.strip().lower()
    col = db[county]
    res = col.find({ "STREET-ADDRESS": address, "ZIP-CODE": int(zip)})
    res = [x for x in res]
    res = get_features(res)
    print(len(res))
    return jsonify({"details":res})

@app.route('/coords/<county>/<address>/<zip>')
def coords(county, address, zip):
    address = address.strip().split("_")
    address[1] = address[1][0] + address[1][1:].lower()
    county = county.strip().lower()
    county = county[0].upper() + county[1:]
    col = bridgedb["bridge"]
    print({ "CountyOrParish": county, "PostalCode": zip, "StreetNumber": address[0], "StreetName": address[1]})
    res = col.find({ "CountyOrParish": county, "PostalCode": zip, "StreetNumber": address[0], "StreetName": address[1]})
    res = [x for x in res]
    # print(res)
    if res:
        res = res[0]
        del res["_id"]
    return jsonify({"coords":res})

@app.route('/tax-data/<county>/<address>/<zip>')
def tax_data(county, address, zip):
    address = " ".join(address.strip().split("_"))
    county = county.strip().lower()
    col = db[county]
    res = col.find({ "STREET-ADDRESS": address, "ZIP-CODE": int(zip)})
    res = [x for x in res]
    res = get_tax_info(res)
    print(len(res))
    return jsonify({"taxes":res})

@app.route('/data_csv/<county>/<address>')
def data3(county, address):
    # Serves as route for delivering csv from database
    print("CSV route has been called")
    address = " ".join(address.strip().split("_"))
    county = county.strip().lower()
    col = db[county]
    cursor = col.find({ "STREET-ADDRESS": address })
    print("Found objects")
    df = pd.DataFrame(list(cursor))
    print("Created CSV")
    res = make_response(df.to_csv())
    print("Created Response")
    res.headers["Content-Disposition"] = "attachment; filename=export.csv"
    res.headers["Content-Type"] = "text/csv"
    print("CSV returned")
    return res

if __name__ == "__main__":
    app.run(debug=True, threaded=True)