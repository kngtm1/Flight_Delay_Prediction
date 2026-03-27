import os
import pickle
import datetime
import dask.dataframe as dd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

MODEL_FILE = "model.pkl"

# Check if model already exists
if os.path.exists(MODEL_FILE):
    print("Model already trained. Loading model.pkl...")
    with open(MODEL_FILE, "rb") as f:
        xg_model = pickle.load(f)
else:
    print("Training model...")

    # ----------- Load and preprocess data -----------
    base_s3_uri = "s3://608-cmb-pub/Combined_Data/run-1774211589769-part-block-0-r-"
    runs = [f"{i:05d}" for i in range(117)]
    s3_urls = [f"{base_s3_uri}{r}-uncompressed.parquet" for r in runs]

    df = dd.read_parquet(s3_urls, storage_options={'anon': True})

    df["fl_date"] = dd.to_datetime(df["fl_date"], format='%m/%d/%Y %I:%M:%S %p')
    df["month"] = df["fl_date"].dt.month
    df = df[df["cancelled"] != "1.00"]
    df = df[df["arr_delay"] != ""]

    numeric_cols = ['crs_dep_time','dep_time', 'dep_delay', 'crs_arr_time', 'arr_time', 'arr_delay',
                    'air_time', 'distance', 'origin_temp', 'origin_precip', 'origin_rain', 'origin_snow',
                    'origin_wind', 'origin_wind_gusts', 'dest_temp', 'dest_precip', 'dest_rain',
                    'dest_snow', 'dest_wind', 'dest_wind_gusts']
    for col in numeric_cols:
        df[col] = dd.to_numeric(df[col], errors='coerce')

    cat_cols = ["day_of_week", 'op_carrier', 'origin', 'dest', 'origin_weather_code',
                'dest_weather_code', 'month']
    df[cat_cols] = df[cat_cols].astype("category")

    drop_cols = ["fl_date", "op_unique_carrier", "op_carrier_airline_id", "tail_num",
                 "op_carrier_fl_num", "origin_airport_id", "origin_airport_seq_id",
                 "origin_city_market_id", "origin_city_name", "origin_state_abr",
                 "dest_airport_id", "dest_airport_seq_id", "dest_city_market_id",
                 "dest_city_name", "dest_state_abr", "dep_time", "dep_delay", "arr_time",
                 "cancelled", "cancellation_code", "diverted", "air_time", "carrier_delay",
                 "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay",
                 "fl_date_key", "dep_hour_key", "arr_hour_key"]
    df = df.drop(columns=drop_cols).dropna()

    # Split features and target
    y = df["arr_delay"]
    X = df.drop(columns="arr_delay")

    # Compute Dask dataframe to Pandas
    X_train, X_test, y_train, y_test = train_test_split(X.compute(), y.compute(), test_size=0.2, random_state=42)

    # Train XGBoost
    xg_model = XGBRegressor(enable_categorical=True)
    xg_model.fit(X_train, y_train)

    # Evaluate
    y_pred = xg_model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    print(f"Training done. MAE on test set: {mae}")

    # Save model
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(xg_model, f)
    print("Model saved to model.pkl")

# Prediction helper
def prediction(data):
    return xg_model.predict(data)
