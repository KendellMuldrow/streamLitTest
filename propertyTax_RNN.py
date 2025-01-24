import numpy as np
import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient, ASCENDING
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from keras import Sequential
from keras.layers import LSTM, Dense, Dropout
from keras.callbacks import EarlyStopping

# MongoDB config
client = MongoClient('mongodb://krish:kNKhNwjQEDXZ@172.26.1.20:27017/')
db = client["housing-tax-prices"]

def init_model(county, epochs, evaluate_model = True):
    collection = db[county]
        
    # independent variables
    cols = ["LAND-VALUE", "SALE-PRICE", "CALCULATED-ACREAGE", "NET-VALUE"]
        
    X = []
    y = []
    # query MongoDB
    for property in collection.distinct('RECORD-KEY'):
        seq_X = []
        seq_y = []
        query = {"RECORD-KEY": property, "SALE-PRICE": {'$gt':100}, "year": {'$gt': 2018}, "PROPERTY-CLASS":2}
        for out in collection.find(query).sort('YEAR', ASCENDING):
            seq_X.extend([[out[col] for col in cols]])
            seq_y.append(out['LAST-YEAR-TAX'])
        X.append(seq_X)
        y.append(seq_y)
        
    print(X.shape)
    print(y.shape)
    
    X = DataFrame(X, columns=cols)
    y = np.array(y)

    # clean data
    X = X.apply(pd.to_numeric, errors='coerce')
    X = X.dropna()
    X = X.astype('int')
    X.astype(np.float64)
    y.astype(np.float64)
    X = X.to_numpy()

    #Data splitting
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)
    
    # Scale the data
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train.reshape(-1, X_train.shape[-1])).reshape(X_train.shape)
    X_test = scaler.transform(X_test.reshape(-1, X_test.shape[-1])).reshape(X_test.shape)
    
    print(X_train.shape)
    
    # set up RNN-LSTM model
    model = Sequential()
    model.add(LSTM(50, activation='tanh', input_shape=(X_train.shape[0], X_train.shape[1])))
    model.add(Dropout(0.2))
    model.add(Dense(1, activation='linear'))

    # Compile the model
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])

    # Train the model
    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
    # Early stopping to avoid overfitting
    trained_Model = model.fit(X_train, y_train, validation_data=(X_test, y_test), epochs=epochs, batch_size=32, callbacks=[early_stopping], verbose=1)
    
    if (evaluate_model):
        test_loss, test_mae = model.evaluate(X_test, y_test, verbose=0)
        print(f"Test Loss (MSE): {test_loss}")
        print(f"Test MAE: {test_mae}")
    
    return trained_Model
        
if __name__ == "__main__":
    hudson_model = init_model('hudson', epochs=10)
    
    # testing model >> from 2017
    land_val, sale_price, acreage, net_value = 322600, 409000, 7230, 434900
    test_values = [[land_val, sale_price, acreage, net_value]]
    test_values = test_values.astype(np.float64)

    tax_prediction = hudson_model.predict(test_values)
    print(f"tax_prediction: {tax_prediction}")