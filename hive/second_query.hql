CREATE TABLE IF NOT EXISTS stock_prices (ticker STRING, open FLOAT, close FLOAT,adjustedThe FLOAT,low FLOAT,high FLOAT,volume FLOAT,tickerdate DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION  'historical_stock_prices.csv';

LOAD DATA LOCAL INPATH 'historical_stock_prices.csv'
OVERWRITE INTO TABLE stock_prices;

CREATE TABLE IF NOT EXISTS stocks (ticker STRING, exc STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "skip.header.line.count"="1")
   STORED AS TEXTFILE
LOCATION  'historical_stocks.csv';
LOAD DATA LOCAL INPATH 'historical_stocks.csv'
OVERWRITE INTO TABLE stocks;


CREATE TABLE IF NOT EXISTS minmaxdate AS
SELECT s.sector,
       YEAR(sp.tickerdate) as tickeryear,
       s.name,
       sp.ticker,
       MIN(tickerdate) AS mindate,
       MAX(tickerdate) AS maxdate
FROM stock_prices sp
JOIN stocks s ON sp.ticker = s.ticker
WHERE tickerdate >= '2008-01-01'
GROUP BY s.sector, YEAR(sp.tickerdate), s.name, sp.ticker;


CREATE TABLE IF NOT EXISTS minmaxclose AS
SELECT mm.sector,
       mm.tickeryear,
       mm.name,
       AVG((((sp2.close - sp1.close) / sp1.close) * 100)) AS percentile
FROM minmaxdate mm
JOIN stock_prices sp1 ON mm.ticker = sp1.ticker AND mindate = sp1.tickerdate
JOIN stock_prices sp2 ON mm.ticker = sp2.ticker AND maxdate = sp2.tickerdate
GROUP BY mm.sector, mm.tickeryear, mm.name;


CREATE TABLE IF NOT EXISTS statsperticker AS
SELECT stocks.sector AS sector,
       YEAR(stock_prices.tickerdate) AS tickeryear,
       stocks.name,
       stock_prices.ticker AS ticker,
       SUM(volume) AS sumvolume,
       AVG(close) AS avgclose
FROM stock_prices
JOIN stocks ON stock_prices.ticker = stocks.ticker
WHERE tickerdate > '2008-01-01'
GROUP BY stocks.sector, YEAR(stock_prices.tickerdate), stocks.name, stock_prices.ticker;


CREATE TABLE IF NOT EXISTS statspername AS
SELECT sector,
       tickeryear,
       name,
       AVG(sumvolume) AS avgvolume,
       AVG(avgclose) AS avgclose
FROM statsperticker
GROUP BY sector, tickeryear, name;

CREATE TABLE IF NOT EXISTS results AS
SELECT m.sector,
       m.tickeryear,
       AVG(s.avgvolume),
       AVG(m.percentile),
       AVG(s.avgclose)
FROM minmaxclose m
JOIN statspername s on m.name = s.name and m.tickeryear = s.tickeryear
GROUP BY m.sector, m.tickeryear;