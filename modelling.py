from datetime import datetime
import dask.dataframe as dd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor
import dask.dataframe as dd # Import dask.dataframe

# Change the base URL to an S3 URI format
base_s3_uri = "s3://608-cmb-pub/Combined_Data/run-1774211589769-part-block-0-r-"

runs = [f"{i:05d}" for i in range(117)]

# Construct the list of S3 URIs for all files
s3_urls = [f"{base_s3_uri}{r}-uncompressed.parquet" for r in runs]

# Use dd.read_parquet to read directly into a Dask DataFrame
df = dd.read_parquet(s3_urls, storage_options={'anon': True})

# converting date column to datetime
df["fl_date"] = dd.to_datetime(df["fl_date"], format='%m/%d/%Y %I:%M:%S %p')

# creating a new month column
df["month"] = df["fl_date"].dt.month

# dropping flights that were cancelled
# these flights do not have a value for arrival delay
df = df[df["cancelled"] != "1.00"]

# drops rows with arrival delay missing
df = df[df["arr_delay"] != ""]

# converts columns with numeric variables to numeric data type
columns_to_convert = ['crs_dep_time','dep_time', 'dep_delay', 'crs_arr_time', 'arr_time', 'arr_delay', 'air_time', 'distance', 'origin_temp', 'origin_precip', 'origin_rain', 'origin_snow', 'origin_wind', 'origin_wind_gusts', 'dest_temp', 'dest_precip', 'dest_rain', 'dest_snow', 'dest_wind', 'dest_wind_gusts']
for col in columns_to_convert:
    df[col] = dd.to_numeric(df[col], errors='coerce')

# converts columns with categorical variables to category data type
df[["day_of_week", 'op_carrier', 'origin', 'dest', 'origin_weather_code', 'dest_weather_code', 'month']] = df[["day_of_week", 'op_carrier', 'origin', 'dest', 'origin_weather_code', 'dest_weather_code', 'month']].astype("category")

# drops columns that will not be used in the model
df = df.drop(columns = ["fl_date", "op_unique_carrier", "op_carrier_airline_id", "tail_num", "op_carrier_fl_num", "origin_airport_id", "origin_airport_seq_id", "origin_city_market_id", "origin_city_name", "origin_state_abr", "dest_airport_id", "dest_airport_seq_id", "dest_city_market_id", "dest_city_name", "dest_state_abr", "dep_time", "dep_delay", "arr_time", "cancelled", "cancellation_code", "diverted", "air_time", "carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay", "fl_date_key", "dep_hour_key", "arr_hour_key"])

# drops any remaing missing values
df = df.dropna()

# splits the dask dataframe into X (predictor variables) and y (the target variable)
y = df["arr_delay"]
X = df.drop(columns = "arr_delay")

# splits the data into a train and test set
X_train, X_test, y_train, y_test = train_test_split(X.compute(), y.compute(), test_size = 0.2, random_state = 42)

# fits the train set to an XGBoost model
xg_model = XGBRegressor(enable_categorical = True)
xg_model.fit(X_train, y_train)

# uses the trained model to make predictions on the test set
y_pred = xg_model.predict(X_test)
# evaluates the performance of the model on making predictions on the test set using mean absolute error
mae = mean_absolute_error(y_test, y_pred) #25.56243324279785
print(mae)

def prediction(data):
    delay = xg_model.predict(data)
    return delay


