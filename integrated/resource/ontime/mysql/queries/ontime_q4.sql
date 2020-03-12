select carrier, count(*) from ontime where depdelay>10 and year=2007 group by carrier order by count(*) desc;
