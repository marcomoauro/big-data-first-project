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
# tramite questo comando, senza l'ausilio di librerie esterne, leggiamo il file historical_stocks.csv da hdfs
# e raccogliamo l'output in un dizionario legando il nome dell'azienda al ticker.
dict_stocks = {}
cat = subprocess.Popen(["hdfs", "dfs", "-cat", "input/historical_stocks.csv"], stdout=subprocess.PIPE)
reader = csv.DictReader(cat.stdout)
for row in reader:
    dict_stocks[row['ticker']] = row['name']

# creo un dizionario contenente le informazioni utili per calcolare l'output con la seguente struttura:
# {
#    ticker1: {
#     2016: { ... },
#     ...
#    },
#   ...
# }
tickers = {}
for line in sys.stdin:
    ticker, _, close, _, _, _, _, date = line.strip().split(',')

    ticker_dict = tickers.setdefault(ticker, {})
    ticker_year = ticker_dict.setdefault(date.split('-')[0], {})
    # aggiornamento della prima e ultima quotazione
    daily_quotation = float(close)
    if date < ticker_year.get('first_quotation_day', DEFAULT_DATE_MAX):
        ticker_year['first_quotation_day'] = date
        ticker_year['first_quotation'] = daily_quotation
    if date > ticker_year.get('last_quotation_day', DEFAULT_DATE_MIN):
        ticker_year['last_quotation_day'] = date
        ticker_year['last_quotation'] = daily_quotation

increments_by_company = []
for key in tickers.keys():
    ticker_dict = tickers[key]
    # scarto i ticker a cui mancano le informazioni di almeno uno degli anni dell'ultimo triennio
    if len(ticker_dict.keys()) < 3:
        continue
    # calcolo delle variazioni di quotazione degli ultimi tre anni
    lll_inc = percentage_variation(ticker_dict['2016']['last_quotation'], ticker_dict['2016']['first_quotation'])
    ll_inc = percentage_variation(ticker_dict['2017']['last_quotation'], ticker_dict['2017']['first_quotation'])
    l_inc = percentage_variation(ticker_dict['2018']['last_quotation'], ticker_dict['2018']['first_quotation'])
    increments_by_company += [[dict_stocks.get(key, 'Default Name'), [lll_inc, ll_inc, l_inc]]]

# visto che un'azienda può avere più ticker allora effettuo un raggruppamento per nome dell'azienda e mi calcolo
# la variazione della quotazione come media delle variazioni delle quotazioni dei ticker.
companies_array = []
for name, increments in groupby(sorted(increments_by_company, key=itemgetter(0)), lambda x: x[0]):
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
    # scarto le aziende che non hanno una variazione della quotazione simile ad altre.
    if len(companies) < 2:
        continue
    lll_inc, ll_inc, l_inc = key.split('_')
    companies_string = '(' + '; '.join([company[0] for company in companies]) + ')'
    print('{0}: 2016:{1}%, 2017:{2}%, 2018:{3}%'.format(companies_string, lll_inc, ll_inc, l_inc))

print('Execution took: ' + str((time.time() - t0)) + ' seconds')
