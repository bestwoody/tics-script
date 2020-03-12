select dayofweek, count(*) as c from ontime where year >= 2000 and year <= 2008 group by dayofweek order by c desc;
