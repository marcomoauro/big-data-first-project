#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import time

DEFAULT_DATE_MAX = '2050-01-01'
DEFAULT_DATE_MIN = '1950-01-01'

def percentage_variation(close_final, close_initial):
    return round(((close_final - close_initial) / close_initial) * 100)

t0 = time.time()
dict = {}
for line in sys.stdin:
    ticker, _, close, _, low, high, volume, date = line.strip().split(',')
    ticker_dict = dict.setdefault(ticker, {})
    ticker_dict['min_list'] = ticker_dict.get('min_list', []) + [float(close)]
    ticker_dict['max_list'] = ticker_dict.get('max_list', []) + [float(close)]
    ticker_dict['volumes'] = ticker_dict.get('volumes', []) + [float(volume)]

    close_initial_range_dict = ticker_dict.setdefault('close_initial_range', {})
    close_final_range_dict = ticker_dict.setdefault('close_final_range', {})
    if date < close_initial_range_dict.get('date', DEFAULT_DATE_MAX):
        close_initial_range_dict['date'] = date
        close_initial_range_dict['close'] = float(close)
    if date > close_final_range_dict.get('date', DEFAULT_DATE_MIN):
        close_final_range_dict['date'] = date
        close_final_range_dict['close'] = float(close)

array = []
for key in dict.keys():
    ticker_dict = dict[key]
    ticker_dict['min'] = min(ticker_dict['min_list'])
    ticker_dict['max'] = max(ticker_dict['max_list'])
    ticker_dict['percentage_variation'] = percentage_variation(ticker_dict['close_final_range']['close'], ticker_dict['close_initial_range']['close'])
    ticker_dict['volume'] = sum(ticker_dict['volumes']) / len((ticker_dict['volumes']))
    array += [[key, ticker_dict['percentage_variation'], ticker_dict['min'], ticker_dict['max'], ticker_dict['volume']]]

for couple in sorted(array, key=itemgetter(1), reverse=True):
    print('{0}\t{1}%\t{2}\t{3}\t{4}'.format(couple[0], couple[1], couple[2], couple[3], couple[4]))

print('Execution took: ' + str((time.time() - t0)) + ' seconds')
