import pyspark
import sys
import time
from operator import lt
from operator import gt


def get_date_close(start_close, start_date, end_close, end_date, op):
    return (start_close, start_date) if op(start_date, end_date) else (end_close, end_date)


def percentual_increase(start, end):
    return round(((end[0] - start[0]) / start[0]) * 100, 2)


t0 = time.time()
session = pyspark.sql.SparkSession.builder.appName('first_query').getOrCreate()
input = '/home/marco/Documenti/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
rows = session.read.csv(sys.argv[1], header=True).rdd.cache() \
    .filter(lambda x: x['date'].split('-')[0] >= '2008') \
    .map(lambda x: (x['ticker'], ((float(x['close']), x['date']), (float(x['close']), x['date']),
                                  float(x['close']), float(x['close']), (float(x['volume']), 1)))) \
    .reduceByKey(lambda x, y: (get_date_close(x[0][0], x[0][1], y[0][0], y[0][1], lt),
                               get_date_close(x[1][0], x[1][1], y[1][0], y[1][1], gt),
                               min(x[2], y[2]), max(x[3], y[3]), (x[4][0] + y[4][0], x[4][1] + y[4][1]))) \
    .map(lambda x: (x[0], percentual_increase(x[1][0], x[1][1]), x[1][2], x[1][3], x[1][4][0] / x[1][4][1])) \
    .sortBy(lambda x: x[1], False).collect()
for i in range(0, len(rows)):
    print('{0}\t{1}%\t{2}\t{3}\t{4}'.format(rows[i][0], rows[i][1], rows[i][2], rows[i][3], rows[i][4]))
session.stop()
print('Execution took: ' + str((time.time() - t0)) + ' seconds')
