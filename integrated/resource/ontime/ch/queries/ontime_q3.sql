select origin, count(*) as c from ontime where depdelay>10 and year>=2000 and year<=2008 group by origin order by c desc limit 10;
