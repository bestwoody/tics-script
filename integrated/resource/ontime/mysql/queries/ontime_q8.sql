select destcityname, count(distinct origincityname) as u  from ontime where year >= 2000 and year <= 2010 group by destcityname order by u desc limit 10;
