DROP TABLE if exists ticker_sector;

CREATE TEMPORARY TABLE ticker_sector
AS
SELECT hs.ticker as ticker, 
	   sector, 
	   volume,
	   close,
	   day
FROM historical_stocks hs JOIN historical_stock_prices hsp 
	 ON hs.ticker = hsp.ticker
WHERE year(day) between '2008' and '2018'
	  and sector != 'N/A';



DROP TABLE if exists ticker_sector_year;

CREATE TEMPORARY TABLE ticker_sector_year
AS
SELECT ticker,
	   sector,
       year(day) as year,
       SUM(volume) as volumes, 
       SUM(close) as closes,
       MIN(day) as first_date,
       MAX(day) as last_date,
       COUNT(*) as num
FROM ticker_sector
GROUP BY ticker, sector, year(day);



DROP TABLE if exists ex2basic_hive;

CREATE TABLE ex2basic_hive 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
AS
SELECT tmp.sector as sector, 
       tmp.year as year,
       cast(AVG(tmp.volumes) as BIGINT) as avgVolume, 
       ROUND(AVG(((ts.close - first_close) / first_close) * 100), 2),
       ROUND(SUM(tmp.closes) / SUM(num), 2)
FROM
	 ( SELECT ty.ticker,
	 		  ty.sector,
	   		  ty.year,
	   		  ty.volumes,
	   		  ty.closes,
	   		  ts.close as first_close,
	   		  last_date,
	   		  num
	   FROM ticker_sector_year ty JOIN ticker_sector ts
		 	ON  ty.ticker = ts.ticker 
     	 	and ty.sector = ts.sector
     	 	and ty.year = year(ts.day) 
     	 	and ty.first_date = ts.day ) tmp
JOIN ticker_sector ts 
	 ON  tmp.ticker = ts.ticker 
     and tmp.sector = ts.sector
     and tmp.year = year(ts.day)
     and last_date = ts.day
GROUP BY tmp.sector, tmp.year;



