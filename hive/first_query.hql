CREATE TABLE IF NOT EXISTS tickers (ticker STRING, open FLOAT, close FLOAT, adjusted_close FLOAT, low FLOAT, high FLOAT, volume FLOAT, data DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION  'historical_stock_prices.csv'
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH 'historical_stock_prices.csv'
OVERWRITE INTO TABLE tickers;

CREATE TABLE IF NOT EXISTS tickers_grouped AS
SELECT ticker, min(data) AS start_date, max(data) AS end_date, min(close) as minor_price, max(close) as major_price, avg(volume) as average_volume
FROM tickers
WHERE year(data) >= 2008
GROUP BY ticker;

SELECT tg.ticker, (((t2.close - t1.close) / t1.close) * 100) as percentage_variation, tg.minor_price, tg.major_price, tg.average_volume
FROM tickers_grouped tg
JOIN tickers t1 ON tg.ticker = t1.ticker and tg.start_date = t1.data
JOIN tickers t2 ON tg.ticker = t2.ticker and tg.end_date = t2.data
ORDER BY percentage_variation DESC;
