#!/usr/bin/env python
"""reducer.py"""

import subprocess
import csv
from operator import itemgetter
import sys
import time

DEFAULT_DATE_MAX = '2050-01-01'
DEFAULT_DATE_MIN = '1950-01-01'

t0 = time.time()
# create dict with ticker like key and sector like value
dict_stocks = {}

cat = subprocess.Popen(["/home/federico/Programmi/hadoop/bin/hdfs", "dfs", "-cat", "/data/historical_stocks.csv"], stdout=subprocess.PIPE)
reader = csv.DictReader(cat.stdout)
for row in reader:
    dict_stocks[row['ticker']] = row['sector']

dict = {}
for line in sys.stdin:
    ticker, _, close, _, low, high, volume, date = line.strip().split(',')

    dict_key = '{0}-{1}'.format(dict_stocks[ticker], date.split('-')[0])
    ticker_dict = dict.setdefault(dict_key, {})

    # (a)
    ticker_dict['volumes'] = ticker_dict.get('volumes', 0) + float(volume)
    ticker_dict['day_counter'] = ticker_dict.get('day_counter', 0) + 1

    # (b)
    quotations_dict = ticker_dict.setdefault('tickers', {})
    quotations_ticker_dict = quotations_dict.setdefault(ticker, {})

    daily_quotation = float(close)
    if date < quotations_ticker_dict.get('first_quotation_day', DEFAULT_DATE_MAX):
        quotations_ticker_dict['first_quotation_day'] = date
        quotations_ticker_dict['first_quotation'] = daily_quotation
    if date > quotations_ticker_dict.get('last_quotation_day', DEFAULT_DATE_MIN):
        quotations_ticker_dict['last_quotation_day'] = date
        quotations_ticker_dict['last_quotation'] = daily_quotation

    # (c)
    quotations_ticker_dict['quotations'] = quotations_ticker_dict.get('quotations', 0) + daily_quotation
    quotations_ticker_dict['day_counter'] = quotations_ticker_dict.get('day_counter', 0) + 1

for key in dict.keys():
    for ticker in dict[key]['tickers'].keys():
        dict_ticker = dict[key]['tickers'][ticker]
        percentage_increment = ((dict_ticker['last_quotation'] - dict_ticker['first_quotation']) / dict_ticker['first_quotation']) * 100
        dict[key]['percentage_increments'] = dict[key].get('percentage_increments', 0) + percentage_increment
        dict[key]['quotations_mean_sum'] = dict[key].get('quotations_mean_sum', 0) + (dict_ticker['quotations'] / dict_ticker['day_counter'])
    dict[key]['percentage_increments_mean'] = round(dict[key]['percentage_increments'] / len(dict[key]['tickers'].keys()), 2)
    dict[key]['mean_quotations_mean_sum'] = dict[key]['quotations_mean_sum'] / len(dict[key]['tickers'].keys())

array = []
for key in dict.keys():
    mean_volume = dict[key]['volumes'] / len(dict[key]['tickers'].keys())
    array += [[key, mean_volume, dict[key]['percentage_increments_mean'], dict[key]['mean_quotations_mean_sum']]]

for couple in sorted(array, key=itemgetter(0), reverse=True):
    print('{0}\t{1}\t{2}%\t{3}'.format(couple[0], couple[1], couple[2], couple[3]))


print('Execution took: ' + str((time.time() - t0)) + ' seconds')
