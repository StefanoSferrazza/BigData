DROP TABLE if exists historical_stock_prices;

CREATE TABLE historical_stock_prices (
        ticker STRING,
        open FLOAT,
        close FLOAT,
        adj_close FLOAT,
        low_the FLOAT,
        high_the FLOAT,
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
