CREATE TABLE IF NOT EXISTS historical_stock_prices (
        ticker STRING,
        open DECIMAL,
        close DECIMAL,
        adj_close DECIMAL,
        low_the DECIMAL,
        high_the DECIMAL,
        volume BIGINT,
        date_ticker DATE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';



-- add jar ../../git/BigData/project/target/DeltaQuotation.jar;

-- CREATE TEMPORARY FUNCTION delta_quotation AS 'hive.job1.DeltaQuotation';



LOAD DATA LOCAL INPATH '../BigData/Project/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

-- LOAD DATA INPATH '/user/teodoro/input/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;



--(SELECT DISTINCT ticker,
--       close as fin_close
--FROM historical_stock_prices
--WHERE year(date_ticker) BETWEEN '2008' AND '2018'
--GROUP BY ticker
--ORDER BY date_ticker DESC)
--
--UNION ALL
--
--(SELECT DISTINCT ticker,
--       close as init_close
--FROM historical_stock_prices
--WHERE year(date_ticker) BETWEEN '2008' AND '2018'
--GROUP BY ticker
--ORDER BY date_ticker ASC)



-- COME dire che ticker sia una chiave??????????????????

CREATE TABLE job1Hive 
--      	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT  t.ticker AS ticker, 
--      delta_quotation(t.first_close,t.last_close) AS delta_quot,
        ((t.last_close - t.first_close)/t.first_close)*100 AS delta_quot,
        t.min_close AS min_close,
        t.max_close AS max_close,
        t.avg_volume AS avg_volume
FROM ( 
	SELECT ticker,
     	    MIN(date_ticker) AS first_close,
     		MAX(date_ticker) AS last_close,
	 		MIN(close) AS min_close, 
     		MAX(close) AS max_close,
     		AVG(volume) AS avg_volume
    FROM historical_stock_prices       
	WHERE year(date_ticker) BETWEEN '2008' AND '2018'
	GROUP BY ticker
	) t
ORDER BY delta_quot DESC;


      

