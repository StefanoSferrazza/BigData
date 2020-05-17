DROP TABLE if exists ticker_company_sector;

CREATE TABLE ticker_company_sector
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT hsp.ticker AS ticker,
	   hs.company AS company,
	   hs.sector AS sector,
	   hsp.close AS close,
	   hsp.volume AS volume,
	   hsp.date_ticker AS date_ticker
FROM historical_stocks hs JOIN historical_stock_prices hsp 
	 ON hsp.ticker = hs.ticker
WHERE year(date_ticker) BETWEEN '2008' AND '2018';



DROP TABLE if exists company_year_avgvolume;

CREATE TABLE company_year_avgvolume
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS
SELECT company,
       year(date_ticker) AS year,
       AVG(volume),
       sector
FROM ticker_company_sector
GROUP BY company, year(date_ticker)





