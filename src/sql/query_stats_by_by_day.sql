SELECT df1.locationtime,agg3.avg,agg3.min_tmp,agg1.timezoneloc as max_location,agg2.timezoneloc as min_loc  
FROM df df1
 INNER JOIN(
 SELECT timezoneloc,max(temp) as max_temp,locationtime FROM df group by timezoneloc,locationtime
 )agg1 ON df1.locationtime = agg1.locationtime AND df1.timezoneloc = agg1.timezoneloc
 INNER JOIN(
 SELECT timezoneloc,min(temp) as min_temp,locationtime FROM df group by timezoneloc,locationtime
 )agg2 ON df1.locationtime = agg2.locationtime AND df1.timezoneloc= agg2.timezoneloc
 INNER JOIN(
 SELECT AVG(temp) as avg,MIN(temp) as min_tmp, locationtime,timezoneloc FROM df GROUP BY locationtime,timezoneloc
 )agg3 ON df1.locationtime = agg3.locationtime and df1.timezoneloc = agg3.timezoneloc
