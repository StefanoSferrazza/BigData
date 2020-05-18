DROP TABLE if exists historical_stock_prices;

CREATE TABLE historical_stock_prices (
        ticker STRING,
        open DECIMAL(10,5),
        close DECIMAL(10,5),
        adj_close DECIMAL(10,5),
        low_the DECIMAL(10,5),
        high_the DECIMAL(10,5),
        volume BIGINT,
        day DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
    


DROP TABLE if exists historical_stocks;

CREATE TABLE historical_stocks (
        ticker STRING,
        exchange_type STRING,
        company STRING,
        sector STRING,
        industry STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';



LOAD DATA LOCAL INPATH '../BigData/Project/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;
-- LOAD DATA INPATH '/user/bigdata/input/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '../BigData/Project/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;
-- LOAD DATA INPATH '/user/bigdata/input/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;



-- CREATE TABLE historical_stock_prices 
--  ROW FORMAT DELIMITED 
--  FIELDS TERMINATED BY ',';
--AS SELECT Parser(hspRow) AS 
--	   (ticker,open,close,adj_close,low_the,high_the,volume,date_ticker)
--FROM historical_stock_prices_row;
