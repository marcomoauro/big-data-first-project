import pyspark
import sys
import time
from operator import lt
from operator import gt


def get_date_close(start_close, start_date, end_close, end_date, op):
    return (start_close, start_date) if op(start_date, end_date) else (end_close, end_date)


def get_name_year_key(x):
    ticker, year = x[0].split('-')
    return '{0}-{1}'.format(stocks.get(ticker, 'DN'), year)


def percentile_increase(start, end):
    return ((end[0] - start[0]) / start[0]) * 100


t0 = time.time()
session = pyspark.sql.SparkSession.builder.appName('second_query').getOrCreate()
rows = session.read.csv(sys.argv[1], header=True).rdd.cache()
stocks = session \
    .read.csv(sys.argv[2], header=True) \
    .rdd.cache() \
    .map(lambda x: (x['ticker'], x['sector'])) \
    .collectAsMap()
rows = rows.filter(lambda x: x['date'].split('-')[0] >= '2008') \
    .map(lambda x: ('{0}-{1}'.format(x['ticker'], x['date'].split('-')[0]),
                    (float(x['volume']), (float(x['close']), x['date']), (float(x['close']), x['date']), float(x['close']), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0],
                               get_date_close(x[1][0], x[1][1], y[1][0], y[1][1], lt),
                               get_date_close(x[2][0], x[2][1], y[2][0], y[2][1], gt),
                               x[3] + y[3], x[4] + y[4])) \
    .map(lambda x: (get_name_year_key(x),
                    (x[1][0],
                     percentile_increase(x[1][1], x[1][2]),
                     x[1][3] / x[1][4], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3])) \
    .map(lambda x: (x[0], x[1][0] / x[1][3], x[1][1] / x[1][3], x[1][2] / x[1][3])) \
    .sortBy(lambda x: x[0], True).collect()
for i in range(0, len(rows)):
    print('{0}\t{1}\t{2}%\t{3}'.format(rows[i][0], rows[i][1], round(rows[i][2], 2), rows[i][3]))
session.stop()
print('Execution took: ' + str((time.time() - t0)) + ' seconds')
