import random

source = 'path/to/historical_stock_prices.csv'

percs = ['0.25', '0.50', '0.75']

for perc in percs:
    destination = source.split('.')[0] + '-' + perc.split('.')[1] + '.csv'
    rows = []
    with open(source, 'r') as f:
        lines = f.readlines()
        rows.append(lines[0])
        for line in lines[1:]:
            if random.uniform(0, 1) < float(perc):
                rows.append(line)

    with open(destination, 'a') as f:
        for line in rows:
            f.write(line)
