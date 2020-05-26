#!/usr/bin/env python
"""reducer.py"""

import sys
import time

TICKER = 0  # simbolo univoco dell'azione
OPEN = 1  # prezzo di apertura
CLOSE = 2  # prezzo di chiusura
ADJ_CLOSE = 3  # prezzo di chiusura modificato
LOWTHE = 4  # prezzo minimo
HIGHTHE = 5  # prezzo massimo
VOLUME = 6  # numero di transizioni
DATE = 7  # data nel formato aaaa-mm-gg

t0 = time.time()

# stocks è il dizionario che aggrega le informazioni a livello di ticker che poi utilizziamo
# per il calcolo dei valori richiesti.
stocks = {}

# fase di inizializzazione della struttura stocks.
for line in sys.stdin:
    line = line.strip()
    components = line.split(',')

    stockName = components[TICKER]

    try:
        stock = stocks[stockName]
    except KeyError:
        stocks[stockName] = {
            'firstDayClosePrice': ['2100-01-01', 0],
            'lastDayClosePrice': ['2000-01-01', 0],
            'minimumPrice': float("inf"),
            'maximumPrice': float("-inf"),
            'stockCount': 0,
            'volumeSum': 0
        }
        stock = stocks[stockName]

    # aggiornamento firstDayClosePrice
    if components[DATE] < stock['firstDayClosePrice'][0]:
        stock['firstDayClosePrice'][0] = components[DATE]
        stock['firstDayClosePrice'][1] = float(components[CLOSE])

    # aggiornamento lastDayClosePrice
    if components[DATE] > stock['lastDayClosePrice'][0]:
        stock['lastDayClosePrice'][0] = components[DATE]
        stock['lastDayClosePrice'][1] = float(components[CLOSE])

    # aggiornamento minimumPrice
    if float(components[CLOSE]) < stock['minimumPrice']:
        stock['minimumPrice'] = float(components[CLOSE])

    # aggiornamento maximumPrice
    if float(components[CLOSE]) > stock['maximumPrice']:
        stock['maximumPrice'] = float(components[CLOSE])

    stock['stockCount'] += 1
    stock['volumeSum'] += float(components[VOLUME])

results = []

# calcolo della variazione della quotazione e del volume medio.
for key, value in stocks.items():
    initialValue = value['firstDayClosePrice'][1]
    finalValue = value['lastDayClosePrice'][1]
    percentileDifference = round(((finalValue - initialValue) / initialValue) * 100)
    volumeAverage = value['volumeSum'] / value['stockCount']
    results.append((key, percentileDifference, value['minimumPrice'], value['maximumPrice'], volumeAverage))

results.sort(key=lambda item: item[1], reverse=True)

for result in results:
    print("{0}\t{1}\t{2}\t{3}".format(result[0], result[1], result[2], result[3], result[4]))

print('Execution took: ' + str((time.time() - t0)) + ' seconds')
