-- psql -U postgres -f fb.sql
CREATE EXTENSION IF NOT EXISTS plpython3u;

CREATE TABLE IF NOT EXISTS price (
    ticker TEXT,
    datum INT, -- in seconds
    high REAL,
    low REAL,
    open REAL,
    close REAL,
    volume INT,
    adjclose REAL,
    formatted_date TEXT
);

-- Fetches from Yahoo financials. start_date like '2012-01-02', 
-- and freq like 'daily' or 'weekly'
CREATE OR REPLACE FUNCTION yf_prices(ticker TEXT, start_date TEXT, end_date TEXT, freq TEXT) 
    RETURNS SETOF price
AS $$
# modify the path variable so that the YahooFinancials package can
# be found. If we don't do this, we will find a ModuleNotFoundError: 
# No module named 'yahoofinancials'. To obtain this path, in the 
# python shell, just print out the path. 
import sys
# TODO Add error detection logic here to give an error message if 
# path not found
my_path = [
    '', 
    '/usr/local/Cellar/python/3.6.5/Frameworks/Python.framework/Versions/3.6/lib/python36.zip', 
    '/usr/local/Cellar/python/3.6.5/Frameworks/Python.framework/Versions/3.6/lib/python3.6', 
    '/usr/local/Cellar/python/3.6.5/Frameworks/Python.framework/Versions/3.6/lib/python3.6/lib-dynload', 
    '/usr/local/lib/python3.6/site-packages'
]
# If we simply append our paths to its original paths, we get an 
# ERROR:  OSError: [Errno socket error] [SSL: CERTIFICATE_VERIFY_FAILED] 
# certificate verify failed (_ssl.c:833). I suspect it's caused
# by some file related to SSL being searched in the original path
# rather than my path. So to fix it, we replace the original path
# completely. 
# sys.path.extend([p for p in my_path if p not in sys.path])
sys.path = my_path + sys.path

def cvt_dict_key(d, old_key, new_key):
    d[new_key] = d[old_key]
    del d[old_key]
    return d

from yahoofinancials import YahooFinancials
yf = YahooFinancials(ticker)
data = yf.get_historical_price_data(start_date, end_date, freq)
ticker_dict = {'ticker': ticker}
# so that we avoid the SQL reserved word "date"
return [cvt_dict_key(dict(**ticker_dict, **row_dict), 'date', 'datum') for row_dict in data[ticker]['prices']]
$$ LANGUAGE plpython3u;

INSERT INTO price
SELECT *
FROM yf_prices('FB', '2012-01-01', '2020-01-01', 'daily')
-- TODO: update the dedup logic
WHERE NOT EXISTS (SELECT ticker FROM price WHERE ticker = 'FB');

DROP TABLE strategies CASCADE;
-- for now only support MA strategies
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id SERIAL PRIMARY KEY,
    ticker TEXT,
    days INT,
    multiplier REAL
);

DELETE FROM strategies;
INSERT INTO strategies (ticker, days, multiplier)
SELECT
    'FB' AS ticker,
    t1.days AS days,
    t2.multiplier AS multiplier
FROM
    UNNEST(ARRAY [30, 60, 100, 200, 400, 800]) AS t1(days)
    CROSS JOIN UNNEST(ARRAY [1/*, 1.05, 1.1, 1.2*/]) AS t2(multiplier);

CREATE TABLE IF NOT EXISTS transactions (
    strategy_id INT,
    date_buy_signal INT,
    price_buy REAL,
    formatted_date_buy_signal TEXT,
    date_sell_signal INT,
    price_sell REAL,
    formatted_date_sell_signal TEXT
);

-- We want to evaluate performance of strategies. A strategy takes
-- in historical data of a stock (prices, MA, volume, etc) and makes
-- transaction decisions. So we can split the evaluation into three
-- parts: 1) feature generation, 2) action generation, 3) transaction
-- summary generation (match buy and sell, and prices) 4) report and
-- performance evaluation. 

CREATE TABLE IF NOT EXISTS mov_avg (
    ticker TEXT,
    datum INT,
    days INT,
    ma REAL
);

-- TODO: abstract out (ticker, date) and create data type for 
-- each separate analysis. Then I can join on analysis_id, and 
-- I won't be partitioning on many columns. For the current
-- setup, in the future if I add more functionalities, the 
-- schema will change as well. If I can use analysis_id to 
-- represent it, it will be more uniform. 
CREATE TABLE IF NOT EXISTS ma_action (
	strategy_id INT REFERENCES strategies ON DELETE CASCADE,
	datum INT,
	action TEXT -- 'buy', 'sell', NULL
);

-- generate moving average given window
-- TODO at first I wanted to use a JSON to store MA, but later
-- decided to create a separate table. Need to clean this up. 
CREATE TABLE IF NOT EXISTS features (
    ticker TEXT,
    datum INT,
    -- for dates that are too early so that we don't have an MA, 
    -- the key won't be here. Otherwise, the JSONB will look like
    -- {30: 100.3, 60: 33.4}, indicating 30d and 60d MA. 
    ma JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE OR REPLACE FUNCTION ft_ma(days INT) RETURNS SETOF mov_avg AS $$
BEGIN
    RETURN QUERY
    SELECT
        ticker,
        datum,
        days,
        CAST(CASE WHEN ROW_NUMBER() OVER (ORDER BY datum) >= days
        THEN AVG(close) OVER
            (ORDER BY datum ASC ROWS BETWEEN days PRECEDING AND CURRENT ROW)
        ELSE NULL
        END AS REAL) AS ma
    FROM price;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gen_ma_action() RETURNS SETOF ma_action AS $$
BEGIN
    RETURN QUERY
    -- whether the stock price is above or below MA
    WITH ma_above AS (
        SELECT
            strategy_id,
            price.datum,
            CASE WHEN ma > close * multiplier THEN TRUE
            WHEN ma <= close * multiplier THEN FALSE
            ELSE NULL -- not enough data to compute MA
            END AS above
        FROM mov_avg NATURAL JOIN strategies NATURAL JOIN price 
    )
    -- signal for whether to buy ot sell stock
    SELECT
        strategy_id,
        datum,
        CASE WHEN prev_above = TRUE AND above = FALSE THEN 'sell'
        WHEN prev_above = FALSE AND above = TRUE THEN 'buy'
        ELSE NULL
        END AS action
    FROM (
        SELECT
            strategy_id,
            datum,
            above,
            LAG(above) OVER (
				PARTITION BY strategy_id
				ORDER BY datum ASC
			) AS prev_above
        FROM
            ma_above
    ) ma_above_with_prev;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gen_transactions() RETURNS SETOF transactions AS $$
BEGIN
    RETURN QUERY
    -- match buy and sell into one row
    WITH transaction_pair AS (
        SELECT
            strategy_id,
            prev_date AS date_buy_signal,
            datum AS date_sell_signal
        FROM (
            SELECT
                strategy_id,
                datum,
                action,
                LAG(datum) OVER (PARTITION BY strategy_id ORDER BY datum ASC) AS prev_date,
                LAG(action) OVER (PARTITION BY strategy_id ORDER BY datum ASC) AS prev_action
            FROM
                ma_action
            WHERE
                action IS NOT NULL
        ) AS action_with_prev
        WHERE
            prev_action = 'buy' AND
            action = 'sell'
    )
    SELECT
        strategy_id,
        date_buy_signal,
        MAX(close) FILTER (WHERE prev_date = date_buy_signal) AS price_buy,
        MAX(formatted_date) FILTER (WHERE prev_date = date_buy_signal)
            AS formatted_date_buy,
        date_sell_signal,
        MAX(close) FILTER (WHERE prev_date = date_sell_signal) AS price_sell,
        MAX(formatted_date) FILTER (WHERE prev_date = date_sell_signal)
            AS formatted_date_sell
    FROM
        transaction_pair NATURAL JOIN strategies JOIN (
            SELECT
                ticker,
                datum,
                close,
                LAG(datum) OVER (ORDER BY datum ASC) AS prev_date,
                formatted_date
            FROM price
        ) AS price_with_prev
    ON
        strategies.ticker = price_with_prev.ticker AND
        prev_date IN (date_buy_signal, date_sell_signal)
    GROUP BY strategy_id, date_buy_signal, date_sell_signal;
END;
$$ LANGUAGE plpgsql;

DELETE FROM mov_avg;
INSERT INTO mov_avg
SELECT
    ticker,
    datum,
    t.days,
    ma
FROM (
    SELECT DISTINCT days
    FROM strategies
) t(days), ft_ma(days);

DELETE FROM ma_action;
INSERT INTO ma_action
SELECT *
FROM gen_ma_action();

DELETE FROM transactions;
INSERT INTO transactions
SELECT *
FROM gen_transactions();

DROP TABLE report;
CREATE TABLE report AS
SELECT
    strategy_id,
    COUNT(1) AS total_transactions,
    COUNT(1) FILTER (WHERE price_sell > price_buy) AS winning,
    COUNT(1) FILTER (WHERE price_sell < price_buy) AS losing,
    COUNT(1) FILTER (WHERE price_sell = price_buy) AS nothing,
    AVG(LN(price_sell) - LN(price_buy))
        FILTER (WHERE price_sell > price_buy) AS avg_win_ln,
    AVG(LN(price_sell) - LN(price_buy))
        FILTER (WHERE price_sell < price_buy) AS avg_lose_ln,
    -- naive revenue is the revenue if we always buy/sell 1 
    -- share, i.e., without any sophisticated position management
    SUM(price_sell - price_buy) AS naive_revenue,
    AVG(date_sell_signal - date_buy_signal) / 86400 AS avg_hold_days,
    SUM(date_sell_signal - date_buy_signal) / 86400 AS total_hold_days
FROM transactions
GROUP BY strategy_id;


-- TODO: compute total revenue, move the python function that compute the annual return rate here

-- 30d MA strategy on FB: 
--        key         |        value
----------------------+---------------------
-- total_transactions |                  79
-- winning            |                  54
-- losing             |                  25
-- nothing            |                   0
-- avg_win            |    3.34074098092538
-- avg_lose           |    -3.0380004119873
-- avg_win_ln         |  0.0349927497182383
-- avg_lose_ln        | -0.0372814566277602
-- naive_revenue      |    104.449996948242
-- avg_hold_days      |    11.2420886075949
-- total_hold_days    |                 888
-- num_days           |                1701

