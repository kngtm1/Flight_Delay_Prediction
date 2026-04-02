WITH flights_clean AS (
    SELECT
        f.*,

        -- Convert flight date to YYYY-MM-DD so it matches weather datetime_local_str date format
        format_datetime(
            date_parse(f.fl_date, '%c/%e/%Y %r'),
            'yyyy-MM-dd'
        ) AS fl_date_key,

        -- Extract departure hour from military time after left-padding to 4 digits
        substr(
            lpad(cast(try_cast(f.crs_dep_time AS integer) AS varchar), 4, '0'),
            1,
            2
        ) AS dep_hour_key,

        -- Extract arrival hour from military time after left-padding to 4 digits
        substr(
            lpad(cast(try_cast(f.crs_arr_time AS integer) AS varchar), 4, '0'),
            1,
            2
        ) AS arr_hour_key
    FROM flights f
),

weather_clean AS (
    SELECT
        w.airport,

        w.temperature_2m,
        w.precipitation,
        w.rain,
        w.snowfall,
        w.weather_code,
        w.wind_speed_10m,
        w.wind_gusts_10m,

        -- Date portion from weather local datetime string: YYYY-MM-DD
        substr(w.datetime_local_str, 1, 10) AS weather_date_key,

        -- Hour portion from weather local datetime string: HH
        substr(w.datetime_local_str, 12, 2) AS weather_hour_key
    FROM weather w
)

SELECT
    f.*,

    -- Origin weather
    wo.temperature_2m    AS origin_temperature_2m,
    wo.precipitation     AS origin_precipitation,
    wo.rain              AS origin_rain,
    wo.snowfall          AS origin_snowfall,
    wo.weather_code      AS origin_weather_code,
    wo.wind_speed_10m    AS origin_wind_speed_10m,
    wo.wind_gusts_10m    AS origin_wind_gusts_10m,

    -- Destination weather
    wd.temperature_2m    AS dest_temperature_2m,
    wd.precipitation     AS dest_precipitation,
    wd.rain              AS dest_rain,
    wd.snowfall          AS dest_snowfall,
    wd.weather_code      AS dest_weather_code,
    wd.wind_speed_10m    AS dest_wind_speed_10m,
    wd.wind_gusts_10m    AS dest_wind_gusts_10m

FROM flights_clean f

LEFT JOIN weather_clean wo
    ON f.origin = wo.airport
   AND f.fl_date_key = wo.weather_date_key
   AND f.dep_hour_key = wo.weather_hour_key

LEFT JOIN weather_clean wd
    ON f.dest = wd.airport
   AND f.fl_date_key = wd.weather_date_key
   AND f.arr_hour_key = wd.weather_hour_key;