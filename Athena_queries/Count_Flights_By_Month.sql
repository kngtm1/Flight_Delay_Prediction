SELECT
    CASE WHEN grouping(year_val)=1 THEN 'TOTAL'
         ELSE CAST(year_val AS VARCHAR) END AS year,
    CASE WHEN grouping(month_val)=1 THEN ''
         ELSE CAST(month_val AS VARCHAR) END AS month,
    COUNT(*) AS total_records
FROM (
    SELECT
        year(date_parse(fl_date, '%c/%e/%Y %r'))  AS year_val,
        month(date_parse(fl_date, '%c/%e/%Y %r')) AS month_val
    FROM flights
) t
GROUP BY GROUPING SETS ((year_val, month_val), ())
ORDER BY year_val NULLS LAST, month_val NULLS LAST;