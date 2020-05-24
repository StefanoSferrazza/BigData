
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
       cast (AVG(delta_quot) as INT) as delta_quot
FROM historical_stocks hs JOIN ticker_quotationyear tqy
	 ON hs.ticker = tqy.ticker
GROUP BY company, year;



DROP TABLE if exists ex3_hive_SameQuots;

CREATE TEMPORARY TABLE ex3_hive_SameQuots
AS
SELECT companies,
	   CONCAT( "2016", ":", cast(tmp.quot2016 as STRING), "%",
	   			";", "2017", ":", cast(tmp.quot2017 as STRING), "%",
	   			";", "2018", ":", cast(tmp.quot2018 as STRING), "%") as deltaQuot
FROM ( SELECT CONCAT ("{", CONCAT_WS(';', COLLECT_SET(c1.company)), "}") as companies,
	   		  c1.delta_quot as quot2016,
	          c2.delta_quot as quot2017,
	          c3.delta_quot as quot2018
       FROM company_quotationyear c1 JOIN company_quotationyear c2
	        ON (c1.company = c2.company and c1.year != c2.year)
	 						  JOIN company_quotationyear c3
	        ON (c1.company = c3.company and c1.year != c3.year and c2.year != c3.year)
       WHERE c1.year = '2016' and c2.year = '2017' and c3.year = '2018'
       GROUP BY c1.delta_quot, c2.delta_quot, c3.delta_quot
       HAVING count(*) > 1
      ) tmp;



DROP TABLE if exists ex3_hive_Singles;

CREATE TEMPORARY TABLE ex3_hive_Singles
AS
SELECT cast(tmp.companies as STRING) as company,
       tmp.quot2016 as quot2016,
	   tmp.quot2017 as quot2017,
	   tmp.quot2018 as quot2018
FROM ( SELECT CONCAT ("{", CONCAT_WS(';', COLLECT_SET(c1.company)), "}") as companies,
	   		  c1.delta_quot as quot2016,
	          c2.delta_quot as quot2017,
	          c3.delta_quot as quot2018
       FROM company_quotationyear c1 JOIN company_quotationyear c2
	        ON (c1.company = c2.company and c1.year != c2.year)
	 						  JOIN company_quotationyear c3
	        ON (c1.company = c3.company and c1.year != c3.year and c2.year != c3.year)
       WHERE c1.year = '2016' and c2.year = '2017' and c3.year = '2018'
       GROUP BY c1.delta_quot, c2.delta_quot, c3.delta_quot
       HAVING count(*) == 1
      ) tmp;



DROP TABLE if exists ex3_hive_SimilQuots;

CREATE TEMPORARY TABLE ex3_hive_SimilQuots
AS
SELECT CONCAT ("{", CONCAT_WS(';', COLLECT_SET(tmp.company)), "}") as companies,
	   cast(tmp.similQuot as STRING) as similQuot
FROM (SELECT t1.company,
	         SQRT(pow((t1.quot2016-t2.quot2016),2) + 
	   		      pow((t1.quot2017-t2.quot2017),2) +
	   		      pow((t1.quot2018-t2.quot2018),2)) as similQuot
	  FROM ex3_hive_Singles t1 JOIN ex3_hive_Singles t2
	  	   ON t1.company != t2.company
      WHERE SQRT(pow((t1.quot2016-t2.quot2016),2)  + 
	   		      pow((t1.quot2017-t2.quot2017),2) +
	   		      pow((t1.quot2018-t2.quot2018),2)) >= 25
	  ) as tmp
GROUP BY tmp.similQuot;



DROP TABLE if exists ex3_hive;

CREATE TABLE ex3_hive
AS
SELECT *
FROM ex3_hive_SameQuots
UNION
SELECT *
FROM ex3_hive_SimilQuots;





	