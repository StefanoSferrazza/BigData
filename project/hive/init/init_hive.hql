
---------- INITIALIZATION: it creates tables for input datasets ----------

DROP TABLE if exists historical_stock_prices;

CREATE TABLE historical_stock_prices (
        ticker STRING,
        open FLOAT, --DECIMAL(15,5),	also tested with decimals
        close FLOAT, --DECIMAL(15,5),
        adj_close FLOAT, --DECIMAL(15,5),
        low_the FLOAT, --DECIMAL(15,5),
        high_the FLOAT, --DECIMAL(15,5),
        volume BIGINT,
        day DATE)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS textfile;
    


DROP TABLE if exists historical_stocks;

CREATE TABLE historical_stocks (
        ticker STRING,
        exchange_type STRING,
        company STRING,
        sector STRING,
        industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
		with serdeproperties (
								"separatorChar" = ",",
								"quoteChar" = "\""
							 )
STORED AS textfile;



LOAD DATA LOCAL INPATH '../BigData/Project/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;
-- LOAD DATA INPATH '/user/bigdata/input/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '../BigData/Project/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;
-- LOAD DATA INPATH '/user/bigdata/input/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;

