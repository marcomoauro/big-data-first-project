CREATE TABLE IF NOT EXISTS stock_prices
(
    ticker      STRING,
    open        FLOAT,
    close       FLOAT,
    adjustedThe FLOAT,
    low         FLOAT,
    high        FLOAT,
    volume      FLOAT,
    tickerdate  DATE
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'historical_stock_prices.csv';

LOAD DATA LOCAL INPATH 'historical_stock_prices.csv'
    OVERWRITE INTO TABLE stock_prices;

CREATE TABLE IF NOT EXISTS stocks
(
    ticker   STRING,
    exc      STRING,
    name     STRING,
    sector   STRING,
    industry STRING
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"",
        "skip.header.line.count" = "1")
    STORED AS TEXTFILE
    LOCATION 'historical_stocks.csv';
LOAD DATA LOCAL INPATH 'historical_stocks.csv'
    OVERWRITE INTO TABLE stocks;


CREATE TABLE IF NOT EXISTS minmaxdates AS
SELECT YEAR(tickerdate) AS tickeryear, ticker, MIN(tickerdate) AS mindate, MAX(tickerdate) AS maxdate
FROM stock_prices
WHERE stock_prices.tickerdate >= '2016-01-01'
GROUP BY YEAR(tickerdate), ticker;

CREATE TABLE IF NOT EXISTS minmaxdates_filtered AS
SELECT m.ticker, m.tickeryear, m.mindate, m.maxdate
FROM minmaxdates m
JOIN
     (
         SELECT ticker, count(*) as count
         FROM minmaxdates
         GROUP BY ticker
     ) mc
     ON m.ticker = mc.ticker
WHERE mc.count = 3;

CREATE TABLE IF NOT EXISTS minmaxclose AS
SELECT m.tickeryear,
       m.ticker,
       (((sp2.close - sp1.close) / sp1.close) * 100) AS percentile
FROM minmaxdates_filtered m
         JOIN stock_prices sp1 on m.ticker = sp1.ticker and m.mindate = sp1.tickerdate
         JOIN stock_prices sp2 on m.ticker = sp2.ticker and m.maxdate = sp2.tickerdate;

CREATE TABLE IF NOT EXISTS percentiles_by_company AS
SELECT s.name, m.tickeryear, ROUND(AVG(m.percentile)) AS percentile
FROM minmaxclose m
JOIN stocks s on m.ticker = s.ticker
GROUP BY s.name, m.tickeryear
ORDER BY s.name desc, m.tickeryear asc;

CREATE TABLE IF NOT EXISTS triplets AS
SELECT name, collect_list(percentile) AS triplet
FROM percentiles_by_company
GROUP BY name;

CREATE TABLE IF NOT EXISTS similars AS
SELECT triplet, collect_list(name) AS companies
FROM triplets
WHERE size(triplet) = 3
GROUP BY triplet;

select companies, triplet
from similars
WHERE size(companies) > 1
ORDER BY triplet asc;
