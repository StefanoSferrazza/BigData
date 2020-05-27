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



---------- MAIN PERSONAL VERSION: quotation => sum first_close and sum last_close, then delta_quot ----------
DROP TABLE if exists ticker_quotationyear;

CREATE TEMPORARY TABLE ticker_quotationyear
AS
SELECT first.ticker as ticker,
	   first.year as year,     
	   firstclose_ticker,
	   lastclose_ticker
FROM ( SELECT tfldy.ticker as ticker,				--
	   		  tfldy.year as year,
	   		  hsp.close as firstclose_ticker
	    FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	   	     ON (tfldy.ticker = hsp.ticker and tfldy.first_date = hsp.day)
	 ) first
JOIN ( SELECT tfldy.ticker as ticker,
	   		  tfldy.year as year,
	   		  hsp.close as lastclose_ticker
	   FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	        ON (tfldy.ticker = hsp.ticker and tfldy.last_date = hsp.day)
	 ) last
ON (first.ticker = last.ticker and first.year = last.year);



DROP TABLE if exists company_quotationyear;

CREATE TEMPORARY TABLE company_quotationyear 
AS
SELECT company,
	   COLLECT_SET( CONCAT( cast(year as STRING), ":",
	   			  			cast(cast(((lastcloses_company - firstcloses_company)/
	   			  						firstcloses_company)*100
	   			  				 as BIGINT)
	   			  			as STRING), "%"))
	   as quot_years
FROM (
	   SELECT company,
	   		  year,
       		  SUM(firstclose_ticker) as firstcloses_company,
	   		  SUM(lastclose_ticker) as lastcloses_company
	   FROM historical_stocks hs 
	   		JOIN ticker_quotationyear tqy
	 		ON hs.ticker = tqy.ticker
	   GROUP BY company, year
	 ) tmp
GROUP BY company;



DROP TABLE if exists ex3_hive;

CREATE TABLE ex3_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT CONCAT ("{", CONCAT_WS(';', comp_list), "}") as companies,
	   quot_years
FROM ( SELECT COLLECT_SET(company) as comp_list,
	          quot_years
	   FROM ( SELECT company,
	   				 CONCAT_WS(';', quot_years) as quot_years
			  FROM company_quotationyear
			  WHERE size(quot_years)==3
	   		) comp_totquot
	   GROUP BY quot_years
	 ) ex3_hive_tot
WHERE size(comp_list)>1
ORDER BY size(comp_list) desc;

