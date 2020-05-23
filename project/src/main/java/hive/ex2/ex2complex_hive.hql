DROP TABLE if exists ticker_firstlastdateyear;

CREATE TEMPORARY TABLE ticker_firstlastdateyear
AS
SELECT hs.ticker,
	   company,
	   sector,
	   year(day) as year,
	   SUM(volume) as totvolume_ticker,
	   MIN(day) as firstdate_ticker,
	   MAX(day) as lastdate_ticker,
	   SUM(close) as totclose_ticker,
	   COUNT(*) as totcount_ticker
FROM historical_stocks hs JOIN historical_stock_prices hsp
	 ON hs.ticker = hsp.ticker
WHERE year(day) between '2008' and '2018'
	  and sector != 'N/A'
	  and company != 'N/A'
GROUP BY hs.ticker, company, sector, year(day);



DROP TABLE if exists ticker_firstlastcloseyear;

CREATE TEMPORARY TABLE ticker_firstlastcloseyear
AS
SELECT first.ticker as ticker,
	   company,
	   sector,
	   first.year as year,
	   totvolume_ticker,
	   firstclose_ticker,
	   lastclose_ticker,
	   totclose_ticker,
	   totcount_ticker
FROM ( SELECT tfldy.ticker as ticker,
              company,
              sector,
	   		  year,
	   		  totvolume_ticker,
	   		  hsp.close as firstclose_ticker,
	   		  totclose_ticker,
	   		  totcount_ticker
	   FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	   	    ON (tfldy.ticker = hsp.ticker and tfldy.firstdate_ticker = hsp.day)
	 ) first
JOIN ( SELECT tfldy.ticker as ticker,
	   		  year as year,
	   		  hsp.close as lastclose_ticker
	   FROM ticker_firstlastdateyear tfldy JOIN historical_stock_prices hsp
	        ON (tfldy.ticker = hsp.ticker and tfldy.lastdate_ticker = hsp.day)
	 ) last
ON (first.ticker = last.ticker and first.year = last.year);



DROP TABLE if exists company_quotationyear;

CREATE TEMPORARY TABLE company_quotationyear
AS
SELECT company,
	   sector,
	   year,
	   totvolume_company,
	   (((lastcloses_company - firstcloses_company) / firstcloses_company) * 100) as delta_quot,
	   (totclose_company / totcount_company) as avg_dailyquot
FROM ( SELECT company,
	          sector,
	   		  year,
	   		  SUM(totvolume_ticker) as totvolume_company,
			  SUM(firstclose_ticker) as firstcloses_company,
	   		  SUM(lastclose_ticker) as lastcloses_company,
			  SUM(totclose_ticker) as totclose_company,
			  SUM(totcount_ticker) as totcount_company 		 
	   FROM ticker_firstlastcloseyear
	   GROUP BY company, sector, year
	 ) tmp;



DROP TABLE if exists ex2complex_hive;

CREATE TABLE ex2complex_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT sector,
	   year,
       cast(AVG(totvolume_company) as BIGINT) as avg_volume,
       ROUND(AVG(delta_quot), 2) as delta_quot,
       ROUND(AVG(avg_dailyquot), 2) as avg_dailyquot
FROM company_quotationyear
GROUP BY sector, year;


