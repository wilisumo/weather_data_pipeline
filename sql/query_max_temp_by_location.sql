SELECT t.dt as date,t.location as location,t.temp as max_temperature
FROM
(SELECT dt,location,month,temp FROM $table_name) as t,
(SELECT MAX(temp) as max_temp, month, location from $table_name GROUP BY month,location) as agg
WHERE 1=1
AND t.location = agg.location
AND t.temp = agg.max_temp