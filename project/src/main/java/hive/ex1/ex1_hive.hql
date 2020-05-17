DROP TABLE if exists ticker_firstlastvalues;

CREATE TEMPORARY TABLE ticker_firstlastvalues
AS
SELECT ticker as ticker,
	   MIN(day) as first_date,
	   MAX(day) as last_date,
	   MIN(close) as min_close, 
       MAX(close) as max_close,
       AVG(volume) as avg_volume	   
FROM historical_stock_prices
WHERE year(day) between '2008' and '2018'
GROUP BY ticker;



---------- VERSION WITHOUT TEMPORARY TABLES ----------

DROP TABLE if exists ex1_hive;

CREATE TABLE ex1_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT tfc.ticker as ticker,
       ((tlc.last_close - tfc.first_close)/tfc.first_close)*100 as delta_quot,
	   tflv.min_close as min_close,
	   tflv.max_close as max_close,
	   tflv.avg_volume as avg_volume
FROM   ( SELECT hsp.ticker as ticker,
	   			hsp.close as first_close
		 FROM historical_stock_prices hsp JOIN ticker_firstlastvalues tfd
	 		  ON (hsp.ticker = tfd.ticker and hsp.day = tfd.first_date) 
	   ) tfc
JOIN   ( SELECT hsp.ticker AS ticker,
	   			hsp.close AS last_close
		 FROM historical_stock_prices hsp JOIN ticker_firstlastvalues tld
	 	 	  ON (hsp.ticker = tld.ticker and hsp.day = tld.last_date)
       ) tlc
	   ON (tfc.ticker = tlc.ticker)
JOIN ticker_firstlastvalues tflv
	   ON (tfc.ticker = tflv.ticker)
ORDER BY delta_quot desc;



---------- VERSION WITH TEMPORARY TABLES: PROBABLY WILL BE REMOVED ----------

DROP TABLE if exists ticker_firstclose;

CREATE TEMPORARY TABLE ticker_firstclose
AS
SELECT hsp.ticker as ticker,
	   hsp.close as first_close
FROM historical_stock_prices hsp JOIN ticker_firstlastvalues tfd
	 ON (hsp.ticker = tfd.ticker and hsp.day = tfd.first_date);


DROP TABLE if exists ticker_lastclose;

CREATE TEMPORARY TABLE ticker_lastclose
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT hsp.ticker AS ticker,
	   hsp.close AS last_close
FROM historical_stock_prices hsp JOIN ticker_firstlastvalues tld
	 ON (hsp.ticker = tld.ticker and hsp.day = tld.last_date);



DROP TABLE if exists ex1_hive;

CREATE TABLE ex1_hive
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



