CREATE TABLE IF NOT EXISTS historical_stock_prices (
        ticker STRING,
        open DECIMAL,
        close DECIMAL,
        adj_close DECIMAL,
        low_the DECIMAL,
        high_the DECIMAL,
        volume BIGINT,
        date_ticker DATE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';			--stabilisce il separatore



-- add jar ../../git/BigData/project/target/DeltaQuotation.jar;					--per importare il jar che rappresenta la UDF

-- CREATE TEMPORARY FUNCTION delta_quotation AS 'hive.job1.DeltaQuotation';		--crea una funzione basata sulla classe definita nel jar
																				-- il secondo è il percorso nel targer per arrivare al .class della UDF definita



LOAD DATA LOCAL INPATH '../BigData/Project/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

-- LOAD DATA INPATH '/user/teodoro/input/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;


-- metodo alternativo per calcolare primo e ultimo close, probabilmente verrà eliminato
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
--      	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','			--sembra non serva ed anzi potrebbe dare problemi
AS
SELECT  t.ticker AS ticker, 
--      delta_quotation(t.first_close,t.last_close) AS delta_quot,
        ((t.last_close - t.first_close)/t.first_close)*100 AS delta_quot,
        t.min_close AS min_close,
        t.max_close AS max_close,
        t.avg_volume AS avg_volume
FROM ( 	
	SELECT ticker,
			(
			SELECT close as first
			FROM historical_stock_prices
			WHERE date_ticker == (
								 SELECT MIN(date_ticker) AS first_date
								 FROM historical_stock_prices
								 WHERE year(date_ticker)>='2008'
								 )
			) as first_close,
			(
			SELECT close as last
			FROM historical_stock_prices
			WHERE date_ticker == (
								 SELECT MAX(date_ticker) AS last_date
								 FROM historical_stock_prices
								 WHERE year(date_ticker)<='2018'
								 )
			) as last_close,
			MIN(close) AS min_close, 
     		MAX(close) AS max_close,
     		AVG(volume) AS avg_volume
    FROM historical_stock_prices       
	WHERE year(date_ticker) BETWEEN '2008' AND '2018'
	GROUP BY ticker
	) t
ORDER BY delta_quot DESC;


SELECT close where date==first_close

