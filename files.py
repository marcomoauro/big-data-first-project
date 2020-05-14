import random

source = 'path/to/historical_stock_prices.csv'


def files(bool):
    percs = ['0.25', '0.50', '0.75']
    for perc in percs:
        destination = source.split('.')[0] + '-' + name(bool, perc) + '.csv'
        rows = []
        new_rows = []
        with open(source, 'r') as f:
            lines = f.readlines()
            rows.append(lines[0])
            for line in lines[1:]:
                if bool:
                    fields = line.strip().split(',')
                    fields[0] = 'MR' + fields[0]
                    fields = ','.join(fields)
                    new_rows.append(fields)
                if random.uniform(0, 1) < float(perc):
                    rows.append(line)

        with open(destination, 'a') as f:
            for line in rows:
                f.write(line)
            for line in new_rows:
                f.write(line + '\n')


def name(flag, percentage):
    return '1' + percentage.split('.')[1] if flag else percentage.split('.')[1]


files(True)
files(False)
