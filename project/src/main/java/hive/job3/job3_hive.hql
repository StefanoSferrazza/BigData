DROP TABLE if exists ticker_company;

CREATE TABLE ticker_company
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT hsp.ticker as ticker,
	   hs.company as company,
	   hsp.close as close,
	   hsp.date_ticker as date_ticker
FROM historical_stocks hs JOIN historical_stock_prices hsp 
	 ON hsp.ticker = hs.ticker
WHERE year(date_ticker) between '2016' and '2018';



DROP TABLE if exists ticker_firstlastdateyear;

CREATE TABLE ticker_firstlastdateyear
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT ticker,
	   company,
	   year(date_ticker) as year,
	   MIN(date_ticker) as first_date,
	   MAX(date_ticker) as last_date
FROM ticker_company
GROUP BY ticker, company, year(date_ticker);



DROP TABLE if exists ticker_firstclose_year;

CREATE TABLE ticker_firstclose_year
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT tfldy.ticker as ticker,
	   tfldy.company as company,
	   tfldy.year as year,
	   tc.close as first_close
FROM ticker_firstlastdateyear tfldy JOIN ticker_company tc
	 ON (tfldy.ticker = tc.ticker and tfldy.first_date = tc.date_ticker);



DROP TABLE if exists ticker_lastclose_year;

CREATE TABLE ticker_lastclose_year
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT tfldy.ticker as ticker,
	   tfldy.company as company,
	   tfldy.year as year,
	   tc.close as last_close
FROM ticker_firstlastdateyear tfldy JOIN ticker_company tc
	 ON (tfldy.ticker = tc.ticker and tfldy.last_date = tc.date_ticker);



DROP TABLE if exists ticker_quotationyear;

CREATE TABLE ticker_quotationyear
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT tfcy.ticker as ticker,
	   tfcy.company as company,
	   tfcy.year as year,
       cast(((tlcy.last_close - tfcy.first_close)/tfcy.first_close)*100 as BIGINT) as delta_quot
FROM ticker_firstclose_year tfcy JOIN ticker_lastclose_year tlcy
	ON (tfcy.ticker = tlcy.ticker and tfcy.year = tlcy.year);



DROP TABLE if exists company_quotationyear;

CREATE TABLE company_quotationyear
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT company,
	   year,
       cast (AVG(delta_quot) as INT) as delta_quot		-- maybe change
FROM ticker_quotationyear
GROUP BY company, year
ORDER BY company, year;



DROP TABLE if exists company_quotationyear_collected;

CREATE TABLE company_quotationyear_collected 
(
		company STRING,
        quot_years ARRAY < STRING >
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ',';

INSERT INTO TABLE company_quotationyear_collected
SELECT company,
	   COLLECT_SET( CONCAT( cast (year as STRING), ":",
	   			  			cast (delta_quot as STRING), "%"))
	   as quot_years
FROM company_quotationyear
GROUP BY company;



DROP TABLE if exists company_quotationyear_string;		-- probably useless

CREATE TABLE company_quotationyear_string 
(
		company STRING,
		quot_years STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ',';

INSERT INTO TABLE company_quotationyear_string
SELECT company,
	   concat_ws(';', quot_years) as quot_years
FROM company_quotationyear_collected
WHERE size(quot_years)==3;



DROP TABLE if exists job3_hive_tmp;

CREATE TABLE job3_hive_tmp 
(
		companies ARRAY < STRING >,
		quot_years STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ',';

INSERT INTO TABLE job3_hive_tmp
SELECT COLLECT_SET(company) as companies,
	   quot_years
FROM company_quotationyear_string
GROUP BY quot_years;



DROP TABLE if exists job3_hive;

CREATE TABLE job3_hive
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
AS
SELECT *
FROM job3_hive_tmp
WHERE size(companies)>1
ORDER BY size(companies) desc;



DROP TABLE if exists ticker_company;
DROP TABLE if exists ticker_firstlastdateyear;
DROP TABLE if exists ticker_firstclose_year;
DROP TABLE if exists ticker_lastclose_year;
DROP TABLE if exists ticker_quotationyear;
DROP TABLE if exists company_quotationyear;
DROP TABLE if exists company_quotationyear_collected;
DROP TABLE if exists company_quotationyear_string;		-- probably useless
DROP TABLE if exists job3_hive_tmp;

	