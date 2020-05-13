import pyspark
import sys
from operator import itemgetter
import time
from operator import lt
from operator import gt


def get_date_close(start_close, start_date, end_close, end_date, op):
    return (start_close, start_date) if op(start_date, end_date) else (end_close, end_date)


def get_name_year_key(x):
    ticker, year = x[0].split('-')
    return '{0}sep{1}'.format(stocks.get(ticker, 'GneGne'), year)


def increase(start, end):
    return ((end - start) / start) * 100


def percentiles_per_years(x):
    sorted_years = sorted(x[1], key=itemgetter(0))
    return '_'.join([str(sorted_years[0][1]), str(sorted_years[1][1]), str(sorted_years[2][1])])


t0 = time.time()
session = pyspark.sql.SparkSession.builder.appName('third_query').getOrCreate()
input1 = '/home/marco/Documenti/daily-historical-stock-prices-1970-2018/small4_historical_stock_prices.csv'
input2 = '/home/marco/Documenti/daily-historical-stock-prices-1970-2018/historical_stocks.csv'
rows = session.read.csv(sys.argv[1], header=True).rdd.cache()
stocks = session \
    .read.csv(sys.argv[2], header=True) \
    .rdd.cache() \
    .map(lambda x: (x['ticker'], x['name'])) \
    .collectAsMap()
groups = rows.filter(lambda x: x['date'].split('-')[0] >= '2016') \
    .map(lambda x: ('-'.join([x['ticker'], x['date'].split('-')[0]]),
                    ((float(x['close']), x['date']), (float(x['close']), x['date'])))) \
    .reduceByKey(lambda x, y: (get_date_close(x[0][0], x[0][1], y[0][0], y[0][1], lt),
                               get_date_close(x[1][0], x[1][1], y[1][0], y[1][1], gt))) \
    .map(lambda x: (get_name_year_key(x),
                    (increase(x[1][0][0], x[1][1][0]), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0].split('sep')[0], [(x[0].split('sep')[1], round(x[1][0] / x[1][1]))])) \
    .reduceByKey(lambda x, y: x + y) \
    .filter(lambda x: len(x[1]) == 3) \
    .map(lambda x: [percentiles_per_years(x), x[0]]) \
    .reduceByKey(lambda x, y: '{0}--{1}'.format(x, y)) \
    .sortBy(lambda x: x[0], True).collect()

for i in range(0, len(groups)):
    if len(groups[i][1].split('--')) < 2:
        continue
    lllperc, llperc, lperc = groups[i][0].split('_')
    print('({0}): 2016:{1}%, 2017:{2}%, 2018:{3}%'.format('; '.join(groups[i][1].split('--')), lllperc, llperc, lperc))
session.stop()
print('Execution took: ' + str((time.time() - t0)) + ' seconds')
