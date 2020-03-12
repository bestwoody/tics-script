select year, c1/c2 from ( select year, count(*)*100 as c1 from ontime where depdelay>10 group by year ) A any inner join ( select year, count(*) as c2 from ontime group by year ) B using (year) order by year;

