DROP TABLE if exists ticker_firstlastvalues;

CREATE TABLE ticker_firstlastvalues
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT ticker as ticker,
	   MIN(date_ticker) as first_date,
	   MAX(date_ticker) as last_date,
	   MIN(close) as min_close, 
       MAX(close) as max_close,
       AVG(volume) as avg_volume	   
FROM historical_stock_prices
WHERE year(date_ticker) between '2008' and '2018'
GROUP BY ticker;


DROP TABLE if exists ticker_firstclose;

CREATE TABLE ticker_firstclose
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT hsp.ticker as ticker,
	   hsp.close as first_close
FROM historical_stock_prices hsp JOIN ticker_firstlastvalues tfd
	 ON (hsp.ticker = tfd.ticker and hsp.date_ticker = tfd.first_date);


DROP TABLE if exists ticker_lastclose;

CREATE TABLE ticker_lastclose
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT hsp.ticker AS ticker,
	   hsp.close AS last_close
FROM historical_stock_prices hsp JOIN ticker_firstlastvalues tld
	 ON (hsp.ticker = tld.ticker and hsp.date_ticker = tld.last_date);


DROP TABLE if exists job1_hive;

CREATE TABLE job1_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT tfc.ticker as ticker,
       ((tlc.last_close - tfc.first_close)/tfc.first_close)*100 as delta_quot,
	   tflv.min_close as min_close,
	   tflv.max_close as max_close,
	   tflv.avg_volume as avg_volume
FROM ticker_firstclose tfc JOIN ticker_lastclose tlc
	ON (tfc.ticker = tlc.ticker)
							JOIN ticker_firstlastvalues tflv
	ON (tfc.ticker = tflv.ticker)
ORDER BY delta_quot desc;



DROP TABLE if exists ticker_firstlastvalues;
DROP TABLE if exists ticker_firstclose;
DROP TABLE if exists ticker_lastclose;



