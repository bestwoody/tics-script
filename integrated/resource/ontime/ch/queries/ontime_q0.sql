select avg(c1) from ( select year, month, count(*) as c1 from ontime group by year, month ) A;
