DROP TABLE if exists historical_stock_prices;

CREATE TABLE historical_stock_prices (
        ticker STRING,
        open FLOAT,
        close FLOAT,
        adj_close FLOAT,
        low_the FLOAT,
        high_the FLOAT,
        volume BIGINT,
        date_ticker DATE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';



--CREATE TABLE historical_stock_prices_row (hspRow STRING)
--	ROW FORMAT DELIMITED 
--  FIELDS TERMINATED BY '\n';



-- add jar ../../git/BigData/project/target/DeltaQuotation.jar;

-- CREATE TEMPORARY FUNCTION delta_quotation AS 'hive.job1.DeltaQuotation';

-- add jar ../../git/BigData/project/target/DataParser.jar;

-- CREATE TEMPORARY FUNCTION delta_quotation AS 'hive.job1.DataParser';



LOAD DATA LOCAL INPATH '../BigData/Project/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

-- LOAD DATA INPATH '/user/teodoro/input/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;



-- CREATE TABLE historical_stock_prices 
--  ROW FORMAT DELIMITED 
--  FIELDS TERMINATED BY ',';
--AS SELECT Parser(hspRow) AS 
--	   (ticker,open,close,adj_close,low_the,high_the,volume,date_ticker)
--FROM historical_stock_prices_row;



DROP TABLE if exists ticker_firstlast_values;

CREATE TABLE ticker_firstlast_values
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT ticker AS ticker_firstlast,
	   MIN(date_ticker) AS first_date,
	   MAX(date_ticker) AS last_date,
	   MIN(close) AS min_close, 
       MAX(close) AS max_close,
       AVG(volume) AS avg_volume	   
FROM historical_stock_prices
WHERE year(date_ticker) BETWEEN '2008' AND '2018'
GROUP BY ticker;


DROP TABLE if exists ticker_first_close;

CREATE TABLE ticker_first_close
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT hsp.ticker as ticker_first,
	   hsp.close as first_close
FROM historical_stock_prices hsp JOIN ticker_firstlast_values tfd
	 ON (hsp.ticker = tfd.ticker_firstlast AND hsp.date_ticker = tfd.first_date);


DROP TABLE if exists ticker_last_close;

CREATE TABLE ticker_last_close
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT hsp.ticker AS ticker_last,
	   hsp.close AS last_close
FROM historical_stock_prices hsp JOIN ticker_firstlast_values tld
	 ON (hsp.ticker = tld.ticker_firstlast AND hsp.date_ticker = tld.last_date);


DROP TABLE if exists job1_hive;

CREATE TABLE job1_hive
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT tfc.ticker_first AS ticker,
       ((tlc.last_close - tfc.first_close)/tfc.first_close)*100 AS delta_quot,
	   tflv.min_close AS min_close,
	   tflv.max_close AS max_close,
	   tflv.avg_volume AS avg_volume
FROM ticker_first_close tfc JOIN ticker_last_close tlc
	ON (tfc.ticker_first = tlc.ticker_last)
							JOIN ticker_firstlast_values tflv
	ON (tfc.ticker_first = tflv.ticker_firstlast)
ORDER BY delta_quot DESC;



DROP TABLE if exists ticker_firstlast_values;
DROP TABLE if exists ticker_first_close;
DROP TABLE if exists ticker_last_close;



