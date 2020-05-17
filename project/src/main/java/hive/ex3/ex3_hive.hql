DROP TABLE if exists ticker_firstlastdateyear;

CREATE TEMPORARY TABLE ticker_firstlastdateyear
AS
SELECT ticker,
	   year(day) as year,
	   MIN(day) as first_date,
	   MAX(day) as last_date
FROM historical_stock_prices
WHERE year(day) between '2016' and '2018'	 
GROUP BY ticker, year(day);



---------- quotation - FIRST VERSION: first delta quot per ticker, then avg for each company ----------

DROP TABLE if exists ticker_quotationyear;

CREATE TEMPORARY TABLE ticker_quotationyear
AS
SELECT first.ticker as ticker,
	   first.year as year,     
       cast(((last.last_close - first.first_close)/first.first_close)*100 as BIGINT) as delta_quot
FROM ( SELECT tfldy.ticker as ticker,
	   		  tfldy.year as year,
	   		  hsp.close as first_close
	    FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	   	    ON (tfldy.ticker = hsp.ticker and tfldy.first_date = hsp.day)
	 ) first
JOIN ( SELECT tfldy.ticker as ticker,
	   		  tfldy.year as year,
	   		  hsp.close as last_close
	   FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	        ON (tfldy.ticker = hsp.ticker and tfldy.last_date = hsp.day)
	 ) last
ON (first.ticker = last.ticker and first.year = last.year);



DROP TABLE if exists company_quotationyear;

CREATE TEMPORARY TABLE company_quotationyear
AS
SELECT company,
	   year,
       cast (AVG(delta_quot) as INT) as delta_quot		 -- maybe change with FLOOR
FROM historical_stocks hs JOIN ticker_quotationyear tqy
	 ON hs.ticker = tqy.ticker
GROUP BY company, year;



---------- quotation - SECOND VERSION: sum first_close and sum last_close, then delta_quot ----------

DROP TABLE if exists ticker_quotationyear;

CREATE TEMPORARY TABLE ticker_quotationyear
AS
SELECT first.ticker as ticker,
	   first.year as year,     
	   first_close,
	   last_close
FROM ( SELECT tfldy.ticker as ticker,
	   		  tfldy.year as year,
	   		  hsp.close as first_close
	    FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	   	    ON (tfldy.ticker = hsp.ticker and tfldy.first_date = hsp.day)
	 ) first
JOIN ( SELECT tfldy.ticker as ticker,
	   		  tfldy.year as year,
	   		  hsp.close as last_close
	   FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	        ON (tfldy.ticker = hsp.ticker and tfldy.last_date = hsp.day)
	 ) last
ON (first.ticker = last.ticker and first.year = last.year);



DROP TABLE if exists company_quotationyear;

CREATE TEMPORARY TABLE company_quotationyear
AS
SELECT company,
	   year,
       cast(((last_close - first_close)/first_close)*100 as BIGINT) delta_quot
FROM historical_stocks hs JOIN ticker_quotationyear tqy
	 ON hs.ticker = tqy.ticker
GROUP BY company, year;



---------- VERSION WITH YEARS AS DISTINCT PARAMETERS ----------

DROP TABLE if exists ex3_hive;

CREATE TABLE ex3_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT concat_ws(';', COLLECT_SET(c1.company)),
	   c1.delta_quot as quot2016,
	   c2.delta_quot as quot2017,
	   c3.delta_quot as quot2018
FROM company_quotationyear c1 JOIN company_quotationyear c2
	 ON (c1.company = c2.company and c1.year != c2.year)
	 						  JOIN company_quotationyear c3
	 ON (c1.company = c3.company and c1.year != c3.year and c2.year != c3.year)
WHERE c1.year = '2016' and c2.year = '2017' and c3.year = '2018'
GROUP BY c1.delta_quot, c2.delta_quot, c3.delta_quot
HAVING count(*) > 1;



---------- VERSION WITH SINGLE STRING CONCAT ----------

DROP TABLE if exists company_quotationyear_collected;

CREATE TEMPORARY TABLE company_quotationyear_collected 
AS
SELECT company,
	   COLLECT_SET( CONCAT( cast (year as STRING), ":",
	   			  			cast (delta_quot as STRING), "%"))
	   as quot_years
FROM company_quotationyear
GROUP BY company;



DROP TABLE if exists ex3_hive;

CREATE TABLE ex3_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT *
FROM ( SELECT COLLECT_SET(company) as companies,
	          quot_years
	   FROM ( SELECT company,
	   				 concat_ws(';', quot_years) as quot_years
			  FROM company_quotationyear_collected
			  WHERE size(quot_years)==3
	   		) tmp
	   GROUP BY quot_years
	 ) ex3_hive_tot
WHERE size(companies)>1
ORDER BY size(companies) desc;



	