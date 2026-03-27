import os
import pickle
import gc
import datetime
import dask.dataframe as dd
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

MODEL_FILE = "model.pkl"

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
    print("Loaded Data....")

    # --- Preprocessing (stays lazy in Dask) ---
    df["fl_date"] = dd.to_datetime(df["fl_date"], format='%m/%d/%Y %I:%M:%S %p')
    df["month"] = df["fl_date"].dt.month
    df = df[df["cancelled"] != "1.00"]
    df = df[df["arr_delay"] != ""]

    numeric_cols = ['crs_dep_time', 'crs_arr_time', 'arr_delay',
                    'distance', 'origin_temp', 'origin_precip', 'origin_rain', 'origin_snow',
                    'origin_wind', 'origin_wind_gusts', 'dest_temp', 'dest_precip', 'dest_rain',
                    'dest_snow', 'dest_wind', 'dest_wind_gusts']
    for col in numeric_cols:
        df[col] = dd.to_numeric(df[col], errors='coerce')

    cat_cols = ["day_of_week", 'op_carrier', 'origin', 'dest',
                'origin_weather_code', 'dest_weather_code', 'month']
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
    print("Cleaned Dataframe...")

    # --- Train/test split by partition (never loads all data at once) ---
    partitions = df.npartitions
    test_cutoff = int(partitions * 0.8)  # 80% train, 20% test

    train_ddf = df.get_partition(slice(0, test_cutoff))
    test_ddf  = df.get_partition(slice(test_cutoff, partitions))

    # --- XGBoost incremental training via batches ---
    xg_model = XGBRegressor(
        enable_categorical=True,
        tree_method="hist",       # much more memory efficient
        device="cuda",            # remove this line if not using GPU
        n_estimators=100,
    )

    print("Training in batches...")
    fitted = False
    for i in range(train_ddf.npartitions):
        partition = train_ddf.get_partition(i).compute()
        X_batch = partition.drop(columns="arr_delay")
        y_batch = partition["arr_delay"]

        if not fitted:
            xg_model.fit(X_batch, y_batch)
            fitted = True
        else:
            # Incrementally update the model with each new batch
            xg_model.fit(X_batch, y_batch, xgb_model=xg_model)

        del partition, X_batch, y_batch
        gc.collect()  # free memory after each batch
        print(f"  Trained on partition {i+1}/{train_ddf.npartitions}")

    # --- Evaluate in batches too ---
    print("Evaluating...")
    all_preds, all_actuals = [], []
    for i in range(test_ddf.npartitions):
        partition = test_ddf.get_partition(i).compute()
        X_batch = partition.drop(columns="arr_delay")
        y_batch = partition["arr_delay"]
        all_preds.append(xg_model.predict(X_batch))
        all_actuals.append(y_batch.values)
        del partition, X_batch, y_batch
        gc.collect()

    mae = mean_absolute_error(np.concatenate(all_actuals), np.concatenate(all_preds))
    print(f"Training done. MAE on test set: {mae}")

    # --- Save model ---
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(xg_model, f)
    print("Model saved to model.pkl")


def prediction(data):
    return xg_model.predict(data)
