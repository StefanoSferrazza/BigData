DROP TABLE if exists historical_stock_prices;

CREATE TABLE historical_stock_prices (
        ticker STRING,
        open DECIMAL(15,5),
        close DECIMAL(15,5),
        adj_close DECIMAL(15,5),
        low_the DECIMAL(15,5),
        high_the DECIMAL(15,5),
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

