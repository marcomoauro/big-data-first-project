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

# tramite questo comando, senza l'ausilio di librerie esterne, leggiamo il file historical_stocks.csv da hdfs
# e raccogliamo l'output in un dizionario legando il settore al ticker.
cat = subprocess.Popen(["hdfs", "dfs", "-cat", "input/historical_stocks.csv"], stdout=subprocess.PIPE)
reader = csv.DictReader(cat.stdout)
for row in reader:
    dict_stocks[row['ticker']] = row['sector']

# creo un dizionario contenente le informazioni utili per calcolare l'output con la seguente struttura:
# {
#   settore-anno: {
#    volume: # somma dei volumi
#    tickers: {
#      ticker1: {
#        ...
#      },
#      ...
#    }
#   }
# }
sector_years = {}
for line in sys.stdin:
    ticker, _, close, _, low, high, volume, date = line.strip().split(',')

    dict_key = '{0}-{1}'.format(dict_stocks.get(ticker, 'DS'), date.split('-')[0])
    ticker_dict = sector_years.setdefault(dict_key, {})

    # (a)
    # aggiornamento del volume complessivo
    ticker_dict['volumes'] = ticker_dict.get('volumes', 0) + float(volume)
    ticker_dict['day_counter'] = ticker_dict.get('day_counter', 0) + 1

    # (b)
    quotations_dict = ticker_dict.setdefault('tickers', {})
    quotations_ticker_dict = quotations_dict.setdefault(ticker, {})

    # aggiornamento della prima e ultima quotazione
    daily_quotation = float(close)
    if date < quotations_ticker_dict.get('first_quotation_day', DEFAULT_DATE_MAX):
        quotations_ticker_dict['first_quotation_day'] = date
        quotations_ticker_dict['first_quotation'] = daily_quotation
    if date > quotations_ticker_dict.get('last_quotation_day', DEFAULT_DATE_MIN):
        quotations_ticker_dict['last_quotation_day'] = date
        quotations_ticker_dict['last_quotation'] = daily_quotation

    # (c)
    # aggiornamento della quotazione annuale
    quotations_ticker_dict['quotations'] = quotations_ticker_dict.get('quotations', 0) + daily_quotation
    quotations_ticker_dict['day_counter'] = quotations_ticker_dict.get('day_counter', 0) + 1

# per ogni <settore, anno>
for key in sector_years.keys():
    # per ogni ticker
    for ticker in sector_years[key]['tickers'].keys():
        dict_ticker = sector_years[key]['tickers'][ticker]
        # calcolo dell'incremento percentuale del ticker
        percentage_increment = ((dict_ticker['last_quotation'] - dict_ticker['first_quotation']) / dict_ticker['first_quotation']) * 100
        # aggiornamento della somma degli incrementi percentuali dei ticker
        sector_years[key]['percentage_increments'] = sector_years[key].get('percentage_increments', 0) + percentage_increment
        # aggiornamento della somma delle quotazioni giornaliere medie dei ticker
        sector_years[key]['quotations_mean_sum'] = sector_years[key].get('quotations_mean_sum', 0) + (dict_ticker['quotations'] / dict_ticker['day_counter'])
    # calcolo dell'incremento percentuale medio come media degli incrementi dei ticker
    sector_years[key]['percentage_increments_mean'] = round(sector_years[key]['percentage_increments'] / len(sector_years[key]['tickers'].keys()), 2)
    # calcolo della quotazione media come media delle quotazioni giornaliere medie dei ticker
    sector_years[key]['mean_quotations_mean_sum'] = sector_years[key]['quotations_mean_sum'] / len(sector_years[key]['tickers'].keys())

array = []
# per ogni <settore, anno>
for key in sector_years.keys():
    # calcolo del volume medio dei ticker
    mean_volume = sector_years[key]['volumes'] / len(sector_years[key]['tickers'].keys())
    array += [[key, mean_volume, sector_years[key]['percentage_increments_mean'], sector_years[key]['mean_quotations_mean_sum']]]

for couple in sorted(array, key=itemgetter(0), reverse=True):
    print('{0}\t{1}\t{2}%\t{3}'.format(couple[0], couple[1], couple[2], couple[3]))


print('Execution took: ' + str((time.time() - t0)) + ' seconds')
