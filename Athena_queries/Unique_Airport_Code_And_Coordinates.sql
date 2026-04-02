WITH unique_airports AS (
    SELECT origin AS airport
    FROM flights
    
    UNION
    
    SELECT dest AS airport
    FROM flights
),
latest_airports AS (
    SELECT
        airport,
        display_airport_name,
        display_city_market_name_full,
        airport_country_name,
        latitude,
        longitude,
        utc_local_time_variation,
        airport_is_latest,
        ROW_NUMBER() OVER (
            PARTITION BY airport
            ORDER BY display_airport_name
        ) AS rn
    FROM airports
    WHERE airport_is_latest = '1'
)

SELECT
    ua.airport,
    la.display_airport_name,
    la.display_city_market_name_full,
    la.airport_country_name,
    la.latitude,
    la.longitude,
    la.utc_local_time_variation,
    la.airport_is_latest
FROM unique_airports ua
LEFT JOIN latest_airports la
    ON ua.airport = la.airport
    AND la.rn = 1
ORDER BY ua.airport;