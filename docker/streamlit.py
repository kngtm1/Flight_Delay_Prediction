%%writefile streamlit.py
import datetime
import streamlit as st
from geopy.distance import geodesic
import requests
import openmeteo_requests
import pandas as pd
import numpy as np
import requests_cache
from retry_requests import retry
import xgboost

# ================================== LOGIC =====================================

# Calculate Day of the Week
def day_of_week(date):
    return date.weekday()+1
  # return date.weekday()+1

# Calculate Month
def month(date):
  return date.month

# Convert time into an integer
def time_to_int(time):
  time_int = time.hour * 100 + time.minute
  return time_int

# Get coordinates
AIRPORT_COORDS = pd.read_csv(
    "https://data604-g4-final.s3.us-east-1.amazonaws.com/Aiport_Coordinates/2a905675-33b7-48c9-9c3e-9decf6a88f21.csv"
)

def get_lon_lat(airport):
    row = AIRPORT_COORDS[AIRPORT_COORDS["airport"] == airport]

    if row.empty:
        return None

    lat = float(row["latitude"].values[0])
    lon = float(row["longitude"].values[0])

    return (lat, lon)

# Calculate distance
def distance(dep_airport, dest_airport):
    dep_loc = get_lon_lat(dep_airport)
    dest_loc = get_lon_lat(dest_airport)

    if not dep_loc or not dest_loc:
        return "Invalid airport code"

    return round(geodesic(dep_loc, dest_loc).kilometers, 2)

# Get weather data
def get_weather(lat, lon, dep_date, dep_time):

  target_time = pd.Timestamp(datetime.datetime.combine(dep_date, dep_time), tz="UTC")

  cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
  retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
  openmeteo = openmeteo_requests.Client(session = retry_session)

  url = "https://api.open-meteo.com/v1/forecast"
  params = {
    "latitude": lat,
    "longitude": lon,
    "hourly": ["temperature_2m", "precipitation", "rain", "snowfall", "weather_code", "wind_speed_10m", "wind_gusts_10m"],
  }
  response = openmeteo.weather_api(url, params = params)[0]

  # Process hourly data. The order of variables needs to be the same as requested.
  hourly = response.Hourly()
  hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
  hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
  hourly_rain = hourly.Variables(2).ValuesAsNumpy()
  hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()
  hourly_weather_code = hourly.Variables(4).ValuesAsNumpy()
  hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
  hourly_wind_gusts_10m = hourly.Variables(6).ValuesAsNumpy()

  hourly_data = {"date": pd.date_range(
    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    freq = pd.Timedelta(seconds = hourly.Interval()),
    inclusive = "left"
  )}

  hourly_data["temperature_2m"] = hourly_temperature_2m
  hourly_data["precipitation"] = hourly_precipitation
  hourly_data["rain"] = hourly_rain
  hourly_data["snowfall"] = hourly_snowfall
  hourly_data["weather_code"] = hourly_weather_code
  hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
  hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

  hourly_dataframe = pd.DataFrame(data = hourly_data)

  closest = hourly_dataframe.iloc[(hourly_dataframe["date"] - target_time).abs().argsort()[:1]]

  return {
  "temperature": np.round(closest["temperature_2m"].values[0], 1),
  "precipitation": np.round(closest["precipitation"].values[0], 1),
  "rain": np.round(closest["rain"].values[0], 1),
  "snow": np.round(closest["snowfall"].values[0], 1),
  "wind_speed": np.round(closest["wind_speed_10m"].values[0], 1),
  "wind_gusts": np.round(closest["wind_gusts_10m"].values[0], 1),
  "weather_code": closest["weather_code"].values[0]
  }

FEATURE_NAMES = ['day_of_week', 'op_carrier', 'origin', 'dest', 'crs_dep_time', 'crs_arr_time',
                 'distance', 'origin_temperature_2m', 'origin_precipitation', 'origin_rain', 'origin_snowfall',
                 'origin_weather_code', 'origin_wind_speed_10m', 'origin_wind_gusts_10m', 'dest_temperature_2m',
                 'dest_precipitation', 'dest_rain', 'dest_snowfall', 'dest_weather_code', 'dest_wind_speed_10m',
                 'dest_wind_gusts_10m', 'month']

# Integer categoricals (came from numeric columns in training)
INT_CATEGORICALS = ["month", "day_of_week", "origin_weather_code", "dest_weather_code"]

# String categoricals (came from string columns in training)
STR_CATEGORICALS = ["op_carrier", "origin", "dest"]

CATEGORICAL_COLS = INT_CATEGORICALS + STR_CATEGORICALS

def preprocess_input(data_dict):
    df = pd.DataFrame([data_dict])

    for col in CATEGORICAL_COLS:
        if col in df.columns:
            if col == "month":
                # int32 categories — matches .dt.month from training
                df[col] = pd.Categorical(
                    np.array([df[col].iloc[0]], dtype=np.int32),
                    categories=np.arange(1, 13, dtype=np.int32)
                )
            elif col in ["origin_weather_code", "dest_weather_code"]:
                # string categories, but value comes in as float32 from weather API
                # must convert to int first to strip decimal, then string
                df[col] = pd.Categorical(
                    [str(int(float(df[col].iloc[0])))],
                )
            else:
                # all others: plain string categories
                df[col] = df[col].astype(str).astype("category")

    for col in df.columns:
        if col not in CATEGORICAL_COLS:
            df[col] = pd.to_numeric(df[col])

    df = df[FEATURE_NAMES]
    return df

model = xgboost.Booster()
model.load_model("model.json")

# ================================== FRONT END INPUT ===================================

st.title("Will your flight be delayed? ✈️")

# Airport list
airport_list = sorted(AIRPORT_COORDS["airport"].tolist())
# -------- Departure --------
st.header("Departure")

# Departure Date
dep_date = st.date_input("Departure Date", datetime.date.today())

# Departure Time
dep_time = st.time_input("Departure Time (24 Hr Time)")

# Depature Airport
dep_query = st.text_input("Type Departure Airport (e.g. LAX, JFK):")
st.subheader("↓")
dep_filtered = [
    code for code in airport_list
    if dep_query.upper() in code
][:10]

dep_airport = None
if dep_filtered:
    dep_airport = st.selectbox(
        "Departure Airports Search Results",
        dep_filtered,
        key="dep_airport"
    )
else:
    if dep_query:
        st.warning("No matching airports found")

# Origin Carrier
carriers = ['MQ', 'NK', 'YV', 'YX', 'WN', '9E', 'B6', 'DL', 'QX', 'UA', 'G4', 'AS', 'F9', 'OO', 'HA', 'OH', 'AA']
carrier = st.selectbox("Origin Carrier", carriers)

st.header("Destination")

# Destination Time
dest_time = st.time_input("Destination Time (24 Hr Time)")

# Destination Airport
dest_query = st.text_input("Type Destination Airport (e.g. LAX, JFK):")
st.subheader("↓")
dest_filtered = [
    code for code in airport_list
    if dest_query.upper() in code
][:10]

dest_airport = None
if dest_filtered:
    dest_airport = st.selectbox(
        "Destination Airports Search Results",
        dest_filtered,
        key="dest_airport"
    )
else:
    if dest_query:
        st.warning("No matching airports found")

# ================================== FRONT END INPUT ===================================

if st.button("Get Estimated Time"):

  # Get coordinates
  dep_lat, dep_lon = get_lon_lat(dep_airport)
  dest_lat, dest_lon = get_lon_lat(dest_airport)

  if not dep_lon or not dep_lat or not dest_lon or not dest_lat:
      st.warning("Invalid airport code")
      st.stop()

  # Get weather
  dep_weather = get_weather(dep_lat, dep_lon, dep_date, dep_time)
  dest_weather = get_weather(dest_lat, dest_lon, dep_date, dest_time)

  # Round values
  dep_temp = round(dep_weather["temperature"], 1)
  dep_precipitation = round(dep_weather["precipitation"], 1)
  dep_rain = round(dep_weather["rain"], 1)
  dep_snow = round(dep_weather["snow"], 1)
  dep_wind_speed = round(dep_weather["wind_speed"], 1)
  dep_wind_gusts = round(dep_weather["wind_gusts"], 1)
  dep_weather_code = dep_weather["weather_code"]

  dest_temp = round(dest_weather["temperature"], 1)
  dest_precipitation = round(dest_weather["precipitation"], 1)
  dest_rain = round(dest_weather["rain"], 1)
  dest_snow = round(dest_weather["snow"], 1)
  dest_wind_speed = round(dest_weather["wind_speed"], 1)
  dest_wind_gusts = round(dest_weather["wind_gusts"], 1)
  dest_weather_code = dest_weather["weather_code"]

  airports_distance = round(distance(dep_airport, dest_airport), 2)


  # Flight summary

  st.markdown("---")

  st.markdown(f"""
  <div style="border:2px solid #4CAF50; padding: 10px; border-radius: 10px;">
      <h3>Flight Summary</h3>
      <b>Route:</b> {dep_airport} → {dest_airport}<br>
      <b>Departure:</b> {dep_date}, {dep_time}<br>
      <b>Distance:</b> {airports_distance} km<br>
      <b>Carrier:</b> {carrier}
  </div>
  """, unsafe_allow_html=True)

  # Weather metrics in columns
  col1, col2 = st.columns(2)

  with col1:
      st.subheader(f"Departure ({dep_airport})")
      st.metric("Temperature", f"{dep_temp:.0f} °C")
      st.metric("Precipitation", f"{dep_precipitation:.2f} mm")
      st.metric("Wind Speed", f"{dep_wind_speed:.2f} km/h")

  with col2:
      st.subheader(f"Destination ({dest_airport})")
      st.metric("Temperature", f"{dest_temp:.0f} °C")
      st.metric("Precipitation", f"{dest_precipitation:.2f} mm")
      st.metric("Wind Speed", f"{dest_wind_speed:.2f} km/h")

# ========================== DATA PROCESSING AND PREDICTION ===============================

  data = {
      "day_of_week": day_of_week(dep_date),
      "op_carrier": carrier,
      "origin": dep_airport,
      "dest": dest_airport,
      "crs_dep_time": time_to_int(dep_time),
      "crs_arr_time": time_to_int(dest_time),
      "distance": airports_distance,
      "origin_temperature_2m": dep_temp,
      "origin_precipitation": dep_precipitation,
      "origin_rain": dep_rain,
      "origin_snowfall": dep_snow,
      "origin_weather_code": dep_weather_code,
      "origin_wind_speed_10m": dep_wind_speed,
      "origin_wind_gusts_10m": dep_wind_gusts,
      "dest_temperature_2m": dest_temp,
      "dest_precipitation": dest_precipitation,
      "dest_rain": dest_rain,
      "dest_snowfall": dest_snow,
      "dest_weather_code": dest_weather_code,
      "dest_wind_speed_10m": dest_wind_speed,
      "dest_wind_gusts_10m": dest_wind_gusts,
      "month": month(dep_date)
  }

  input_df = preprocess_input(data)

  dmat = xgboost.DMatrix(input_df, enable_categorical=True)
  predicted_delay = model.predict(dmat)

  st.markdown("---")

  st.header("Predicted Delayed Time:")

  blank_col1, blank_col2, blank_col3 = st.columns(3)

  with blank_col1:
    st.header("")

  with blank_col2:
    st.metric("Time Delay:", f"{predicted_delay[0]:.0f} minutes")

  with blank_col3:
    st.header("")
