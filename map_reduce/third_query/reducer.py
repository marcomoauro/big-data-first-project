#!/usr/bin/env python
"""reducer.py"""

import subprocess
from operator import itemgetter
import time
import sys
import csv
from itertools import groupby

DEFAULT_DATE_MAX = '2050-01-01'
DEFAULT_DATE_MIN = '1950-01-01'

def percentage_variation(final, initial):
    return round(((final - initial) / initial) * 100)

t0 = time.time()
dict_stocks = {}
cat = subprocess.Popen(["/home/marco/Documenti/hadoop/bin/hdfs", "dfs", "-cat", "/user/marco/input/historical_stocks.csv"], stdout=subprocess.PIPE)
reader = csv.DictReader(cat.stdout)
for row in reader:
    dict_stocks[row['ticker']] = row['name']

dict = {}
for line in sys.stdin:
    ticker, _, close, _, _, _, _, date = line.strip().split(',')

    ticker_dict = dict.setdefault(ticker, {})
    ticker_year = ticker_dict.setdefault(date.split('-')[0], {})
    daily_quotation = float(close)
    if date < ticker_year.get('first_quotation_day', DEFAULT_DATE_MAX):
        ticker_year['first_quotation_day'] = date
        ticker_year['first_quotation'] = daily_quotation
    if date > ticker_year.get('last_quotation_day', DEFAULT_DATE_MIN):
        ticker_year['last_quotation_day'] = date
        ticker_year['last_quotation'] = daily_quotation

array = []
for key in dict.keys():
    ticker_dict = dict[key]
    if len(ticker_dict.keys()) < 3:
        continue
    lll_inc = percentage_variation(ticker_dict['2016']['last_quotation'], ticker_dict['2016']['first_quotation'])
    ll_inc = percentage_variation(ticker_dict['2017']['last_quotation'], ticker_dict['2017']['first_quotation'])
    l_inc = percentage_variation(ticker_dict['2018']['last_quotation'], ticker_dict['2018']['first_quotation'])
    array += [[dict_stocks[key], [lll_inc, ll_inc, l_inc]]]

companies_array = []
for name, increments in groupby(sorted(array, key=itemgetter(0)), lambda x: x[0]):
    increments = list(increments)
    lll_inc_acc = 0
    ll_inc_acc = 0
    l_inc_acc = 0
    for increment in increments:
        lll_inc_acc += increment[1][0]
        ll_inc_acc += increment[1][1]
        l_inc_acc += increment[1][2]
    increments_years = '{}_{}_{}'.format(round(lll_inc_acc / len(increments)), round(ll_inc_acc / len(increments)), round(l_inc_acc / len(increments)))
    companies_array += [[name, increments_years]]

for key, companies in groupby(sorted(companies_array, key=itemgetter(1)), lambda x: x[1]):
    companies = list(companies)
    if len(companies) < 2:
        continue
    lll_inc, ll_inc, l_inc = key.split('_')
    companies_string = '(' + '; '.join([company[0] for company in companies]) + ')'
    print('{0}: 2016:{1}%, 2017:{2}%, 2018:{3}%'.format(companies_string, lll_inc, ll_inc, l_inc))

print('Execution took: ' + str((time.time() - t0)) + ' seconds')
