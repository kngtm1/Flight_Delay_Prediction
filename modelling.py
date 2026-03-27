import os
import datetime
import dask.dataframe as dd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

MODEL_FILE = "model.json"

# Check if model already exists
if os.path.exists(MODEL_FILE):
    print("Model already trained. Skipping training.")
else:
    print("Training model...")

    # ----------- Load and preprocess data -----------
    base_s3_uri = "s3://608-cmb-pub/Combined_Data/run-1774211589769-part-block-0-r-"
    runs = [f"{i:05d}" for i in range(117)]
    s3_urls = [f"{base_s3_uri}{r}-uncompressed.parquet" for r in runs]

    df = dd.read_parquet(s3_urls, storage_options={'anon': True})

    # Create month from date (produces int32 — keep as-is, do not cast to string)
    df["fl_date"] = dd.to_datetime(df["fl_date"], format='%m/%d/%Y %I:%M:%S %p')
    df["month"] = df["fl_date"].dt.month

    # Filter out cancelled flights and missing arrival delays
    df = df[df["cancelled"] != "1.00"]
    df = df[df["arr_delay"] != ""]

    # Convert numeric columns
    numeric_cols = [
        'crs_dep_time', 'dep_time', 'dep_delay', 'crs_arr_time', 'arr_time', 'arr_delay',
        'air_time', 'distance', 'origin_temp', 'origin_precip', 'origin_rain', 'origin_snow',
        'origin_wind', 'origin_wind_gusts', 'dest_temp', 'dest_precip', 'dest_rain',
        'dest_snow', 'dest_wind', 'dest_wind_gusts'
    ]
    for col in numeric_cols:
        df[col] = dd.to_numeric(df[col], errors='coerce')

    # Convert categorical columns
    # Note: day_of_week, op_carrier, origin, dest, origin_weather_code, dest_weather_code
    #       are string categories (from raw parquet strings)
    #       month is int32 category (from .dt.month)
    cat_cols = ["day_of_week", 'op_carrier', 'origin', 'dest', 'origin_weather_code',
                'dest_weather_code', 'month']
    df[cat_cols] = df[cat_cols].astype("category")

    # Drop unused columns
    drop_cols = [
        "fl_date", "op_unique_carrier", "op_carrier_airline_id", "tail_num",
        "op_carrier_fl_num", "origin_airport_id", "origin_airport_seq_id",
        "origin_city_market_id", "origin_city_name", "origin_state_abr",
        "dest_airport_id", "dest_airport_seq_id", "dest_city_market_id",
        "dest_city_name", "dest_state_abr", "dep_time", "dep_delay", "arr_time",
        "cancelled", "cancellation_code", "diverted", "air_time", "carrier_delay",
        "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay",
        "fl_date_key", "dep_hour_key", "arr_hour_key"
    ]
    df = df.drop(columns=drop_cols).dropna()

    # Split features and target
    y = df["arr_delay"]
    X = df.drop(columns="arr_delay")

    # Compute Dask dataframe to Pandas
    print("Computing Dask dataframe (this may take a while)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X.compute(), y.compute(), test_size=0.2, random_state=42
    )

    # Print dtypes for verification before training
    print("\nFeature dtypes going into model:")
    for col in X_train.columns:
        if hasattr(X_train[col], 'cat'):
            print(f"  {col}: {X_train[col].dtype} (categories dtype: {X_train[col].cat.categories.dtype})")
        else:
            print(f"  {col}: {X_train[col].dtype}")

    # Train XGBoost
    xg_model = XGBRegressor(enable_categorical=True)
    xg_model.fit(X_train, y_train)

    # Evaluate
    y_pred = xg_model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    print(f"\nTraining done. MAE on test set: {mae:.2f} minutes")

    # Save as JSON (not pickle) so it can be loaded with xgboost.Booster()
    xg_model.save_model(MODEL_FILE)
    print(f"Model saved to {MODEL_FILE}")
