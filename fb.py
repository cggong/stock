# Created on Feb 22, 2019. First attempt to study stock prices
# with computer programs. Want to try using a package to fetch
# stock prices data, and do some simple study of momentum. 
# Using this package: https://github.com/JECSand/yahoofinancials
from yahoofinancials import YahooFinancials
from schema import Schema, And, Use, Optional
import math

# Calculate the annual average return, given the start capital, 
# end capital, and the time duration in seconds. 
def annual_avg_return(start, end, duration):
    return math.exp((math.log(end) - math.log(start)) / duration * 365.25*86400) - 1

assert annual_avg_return(1, 1, 365.25*86400) == 0., 'if I start with $1 and end with $0, then my annual return should be 0'
assert annual_avg_return(1, 2, 365.25*86400) == 1., 'if I start with $1 and end with $2 after a year, then my annual return should be 100%'

yf = YahooFinancials('FB')
fb_yf = yf.get_historical_price_data(
    '2012-01-01',
    '2020-01-01',
    'daily',
)

# validate data schema
schema = Schema({'FB': {'eventsData': {}, 'firstTradeDate': {
    'formatted_date': str,
    'date': int,
}, 'currency': str, 'instrumentType': str, 'timeZone': {
    'gmtOffset': int,
}, 'prices': [{
    'date': int, 
    'high': float, 
    'low': float, 
    'open': float, 
    'close': float,
    'volume': int,
    'adjclose': float,
    'formatted_date': str,
}]}})

if schema.is_valid(fb_yf): 
    print('Yahoo data schema validated.')
else:
    print('Yahoo data schema validation failed.')
    exit(0)

fb_prices = fb_yf['FB']['prices']

print('loaded {} dates'.format(len(fb_prices)))

# annual return for naive buy and hold
print('start date {}, end date {}, start price {}, end price {}'.format(
    fb_prices[0]['formatted_date'],
    fb_prices[-1]['formatted_date'],
    fb_prices[0]['close'],
    fb_prices[-1]['close'],
))
naive_return = annual_avg_return(fb_prices[0]['close'], fb_prices[-1]['close'], fb_prices[-1]['date'] - fb_prices[0]['date'])
print('naive strategy has annual return {:.2%}'.format(naive_return))
# naive strategy has annual return 23.78%

# Feb 23, 2019: exploring to move to SQL. 
# Would like to move the YahooFinancials to a SQL function: 
# https://www.postgresql.org/docs/11/plpython-data.html
