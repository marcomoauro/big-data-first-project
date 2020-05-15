MULTIPLIERS = [2, 4, 8]
SOURCE_STOCK_PRICES = '/home/marco/Documenti/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
SOURCE_STOCKS = '/home/marco/Documenti/daily-historical-stock-prices-1970-2018/historical_stocks.csv'
DEST_ROOT = ''


def files(input, multipliers):
    for mult in multipliers:
        print('work on ' + input + ' - ' + str(mult) + 'x')
        destination = filename(mult, input)
        with open(input, 'r') as input_file:
            with open(destination, 'a') as dest_file:
                lines = input_file.readlines()
                dest_file.write(lines[0])
                # all row of original file
                for line in lines[1:]:
                    dest_file.write(line)
                # fake tickers
                for i in range(mult - 1):
                    for line in lines[1:]:
                        fields = line.strip().split(',')
                        fields[0] = 'MR' + str(i+2) + fields[0]
                        fields = ','.join(fields)
                        dest_file.write(fields + '\n')


def filename(mult, input):
    return DEST_ROOT + input.split('.')[0] + '-' + str(mult) + 'x.csv'


files(SOURCE_STOCK_PRICES, MULTIPLIERS)
files(SOURCE_STOCKS, [MULTIPLIERS[-1]])
