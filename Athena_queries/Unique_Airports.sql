SELECT origin AS airport
FROM flights
    
  UNION
    
SELECT dest AS airport
FROM flights