set hive.auto.convert.join = false;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS tickers (ticker STRING, open FLOAT, close FLOAT, adjusted_close FLOAT, low FLOAT, high FLOAT, volume FLOAT, data DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION  '/home/marco/Documenti/apache-hive-3.1.2-bin/data/historical_stock_prices.csv'
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH '/home/marco/Documenti/apache-hive-3.1.2-bin/data/historical_stock_prices.csv'
OVERWRITE INTO TABLE tickers;

CREATE TABLE IF NOT EXISTS tickers_year AS
SELECT *
FROM tickers
WHERE year(data) >= 2008;

CREATE TABLE IF NOT EXISTS tickers_grouped AS
SELECT ticker, min(data) AS start_date, max(data) AS end_date, min(close) as minor_price, max(close) as major_price, avg(volume) as average_volume
FROM tickers_year
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS tickers_start_date AS
SELECT tg.ticker, t.close
FROM tickers_grouped tg, tickers t
WHERE tg.ticker = t.ticker and tg.start_date = t.data;

CREATE TABLE IF NOT EXISTS tickers_end_date AS
SELECT tg.ticker, t.close
FROM tickers_grouped tg, tickers t
WHERE tg.ticker = t.ticker and tg.end_date = t.data;

CREATE TABLE IF NOT EXISTS tickers_results AS
SELECT tg.ticker, (((te.close - ts.close) / ts.close) * 100) as percentage_variation, tg.minor_price, tg.major_price, tg.average_volume
FROM tickers_grouped tg, tickers_start_date ts, tickers_end_date te
WHERE tg.ticker = ts.ticker and tg.ticker = te.ticker
ORDER BY percentage_variation DESC;
