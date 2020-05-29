DROP TABLE if exists ticker_firstlastvalues;

CREATE TEMPORARY TABLE ticker_firstlastvalues
AS	 
SELECT ticker as ticker,
	   MIN(day) as first_date,
	   MAX(day) as last_date,
	   MIN(close) as min_close,
       MAX(close) as max_close,
       FLOOR(AVG(volume)) as avg_volume
FROM historical_stock_prices
WHERE year(day) between '2008' and '2018'
GROUP BY ticker;



---------- ALTERNATIVE VERSION: WITH TEMPORARY TABLES ----------
DROP TABLE if exists ticker_firstclose;

CREATE TEMPORARY TABLE ticker_firstclose
AS
SELECT hsp.ticker as ticker,
	   hsp.close as first_close,
	   min_close,
	   max_close,
	   avg_volume
FROM ticker_firstlastvalues tfd JOIN historical_stock_prices hsp
	 ON (tfd.ticker = hsp.ticker and tfd.first_date = hsp.day);



DROP TABLE if exists ticker_lastclose;

CREATE TEMPORARY TABLE ticker_lastclose
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT hsp.ticker AS ticker,
	   hsp.close AS last_close
FROM ticker_firstlastvalues tld JOIN historical_stock_prices hsp
	 ON (tld.ticker = hsp.ticker and tld.last_date = hsp.day);



DROP TABLE if exists ex1_hive;

CREATE TABLE ex1_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT tfc.ticker as ticker,
       ROUND(((last_close - first_close)/first_close)*100, 0) as d_quot,
	   min_close,
	   max_close,
	   avg_volume
FROM ticker_firstclose tfc 
     JOIN ticker_lastclose tlc
	 ON (tfc.ticker = tlc.ticker)
ORDER BY d_quot desc;






