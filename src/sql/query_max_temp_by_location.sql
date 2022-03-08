SELECT t.locationtime as date,t.timezoneloc as timezoneloc,t.temp as max_temperature FROM
(SELECT locationtime,timezoneloc,month,temp FROM df) as t,
(SELECT MAX(temp) as max_temp, month, timezoneloc from df GROUP BY month,timezoneloc) as agg 
 WHERE 1=1
 AND t.timezoneloc = agg.timezoneloc
 AND t.temp = agg.max_temp