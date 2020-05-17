DROP TABLE IF EXISTS ticker_sector;

CREATE TEMPORARY TABLE ticker_sector
AS
SELECT hs.ticker as ticker, 
	   hs.sector as sector, 
	   hsp.volume as volume,
	   hsp.close as close,
	   hsp.day as day
FROM historical_stocks hs JOIN historical_stock_prices hsp 
	 ON hs.ticker = hsp.ticker
WHERE year(hsp.day) between '2008' and '2018';



DROP TABLE IF EXISTS ticker_year;

CREATE TEMPORARY TABLE ticker_year
AS
SELECT ticker,
	   sector,
       year(day) as year,
       SUM(volume) as volume, 
       SUM(close) as close,
       MIN(day) as first_date,
       MAX(day) as last_date,
       COUNT(*) as num
FROM ticker_sector
GROUP BY ticker, sector, year(day);



DROP TABLE IF EXISTS job2basic_hive;

CREATE TABLE job2basic_hive 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
AS
SELECT s as sector, 
       y as year, 
       ROUND(AVG(v)) as avgVolume, 
       ROUND(AVG(((ts.close - tmp.c) / tmp.c) * 100), 2),
       ROUND(SUM(c) / SUM(n), 2)
FROM
	 ( SELECT ty.sector as s,
	   		  ty.year as y,
	   		  ty.ticker as t,
	   		  ty.volume as v,
	   		  ts.close,
	   		  last_date as ld,
	   		  ty.close as c,
	   		  num as n
	   FROM ticker_year ty JOIN ticker_sector ts
		 	ON ty.sector = ts.sector 
     	 	and ty.year = year(ts.day) 
     	 	and ty.ticker = ts.ticker
     	 	and ty.first_date = ts.day ) tmp
JOIN ticker_sector ts ON ts.sector = s 
       AND year(ts.day) = y 
       AND ts.ticker = t 
       AND ts.day = ld
GROUP BY s, y;