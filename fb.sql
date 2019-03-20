-- psql -U postgres -f fb.sql
CREATE EXTENSION IF NOT EXISTS plpython3u;

CREATE TABLE IF NOT EXISTS price (
    ticker TEXT,
    date INT, -- in seconds
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
from yahoofinancials import YahooFinancials
yf = YahooFinancials(ticker)
data = yf.get_historical_price_data(start_date, end_date, freq)
ticker_dict = {'ticker': ticker}
return [dict(**ticker_dict, **row_dict) for row_dict in data[ticker]['prices']]
$$ LANGUAGE plpython3u;

INSERT INTO price
SELECT *
FROM yf_prices('FB', '2012-01-01', '2020-01-01', 'daily')
WHERE NOT EXISTS (SELECT ticker FROM price WHERE ticker = 'FB');

-- for now only support MA strategies
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id SERIAL PRIMARY KEY,
    ticker TEXT,
    days INT,
    multiplier REAL
);

INSERT INTO strategies (ticker, days, multiplier)
SELECT
    'FB' AS ticker,
    t1.days AS days,
    t2.multiplier AS multiplier
FROM
    UNNEST(ARRAY [30, 60]) AS t1(days)
    CROSS JOIN UNNEST(ARRAY [1, 1.05, 1.1, 1.3]) AS t2(multiplier);

CREATE TABLE IF NOT EXISTS transactions (
    ticker TEXT,
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
    date INT,
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
	ticker TEXT,
	date INT,
	days INT,
	multiplier REAL, -- stock price higher than MA * multiplier
	action TEXT -- 'buy', 'sell', NULL
);

-- generate moving average given window
-- TODO at first I wanted to use a JSON to store MA, but later
-- decided to create a separate table. Need to clean this up. 
CREATE TABLE IF NOT EXISTS features (
    ticker TEXT,
    date INT,
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
        "date",
        days,
        CAST(CASE WHEN ROW_NUMBER() OVER (ORDER BY date) >= days
        THEN AVG(close) OVER
            (ORDER BY date ASC ROWS BETWEEN days PRECEDING AND CURRENT ROW)
        ELSE NULL
        END AS REAL) AS ma
    FROM price;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gen_ma_action(real[]) RETURNS void AS $$
BEGIN
	EXECUTE $func$
        INSERT INTO ma_action
		WITH all_features AS (
			SELECT
				mov_avg.ticker,
				mov_avg.date,
				days,
				ma,
				multiplier,
				close
			FROM
				mov_avg
			CROSS JOIN UNNEST($1) AS t(multiplier)
			LEFT JOIN price 
            ON mov_avg.ticker = price.ticker
            AND mov_avg.date = price.date
		),
        -- whether the stock price is above or below MA
        ma_above AS (
            SELECT
                ticker,
                multiplier,
                "date",
                days,
                CASE WHEN ma > close * multiplier THEN TRUE
                WHEN ma <= close * multiplier THEN FALSE
                ELSE NULL -- not enough data to compute MA
                END AS above
            FROM all_features
        )
        -- signal for whether to buy ot sell stock
        SELECT
            ticker,
            date,
            days,
            multiplier,
            CASE WHEN prev_above = TRUE AND above = FALSE THEN 'sell'
            WHEN prev_above = FALSE AND above = TRUE THEN 'buy'
            ELSE NULL
            END AS action
        FROM (
            SELECT
                ticker,
                multiplier,
                "date",
                days,
                above,
                LAG(above) OVER (
					PARTITION BY ticker, multiplier 
					ORDER BY date ASC
				) AS prev_above
            FROM
                ma_above
        ) ma_above_with_prev
	$func$
	USING $1;
END;
$$ LANGUAGE plpgsql;

DELETE FROM mov_avg;
INSERT INTO mov_avg
SELECT
    ticker,
    date,
    t.days,
    ma
FROM (
    SELECT DISTINCT days
    FROM strategies
) t(days), ft_ma(days);

DELETE FROM ma_action;
SELECT gen_ma_action(ARRAY_AGG(DISTINCT multiplier))
FROM strategies;


-- TODO: 1) 2) 3)
INSERT INTO transactions
-- the annual return rate of the 30day MA
WITH ma AS (
    SELECT
        "date",
        close,
        CASE WHEN ROW_NUMBER() OVER (ORDER BY date) >= 30
        THEN AVG(close) OVER
            (ORDER BY date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        ELSE NULL
        END AS ma30
    FROM price
), 
-- whether the stock price is above or below MA
ma_above AS (
    SELECT
        "date",
        CASE WHEN ma30 > close THEN TRUE
        WHEN ma30 <= close THEN FALSE
        ELSE NULL -- not enough data to compute MA
        END AS above
    FROM ma
),
-- include ma_above for the previous day for action
-- computation
ma_above_with_prev AS (
    SELECT
        "date",
        above,
        LAG(above) OVER (ORDER BY date ASC) AS prev_above
    FROM
        ma_above
),
-- signal for whether to buy ot sell stock
action AS (
    SELECT
        date,
        CASE WHEN prev_above = TRUE AND above = FALSE THEN 'sell'
        WHEN prev_above = FALSE AND above = TRUE THEN 'buy'
        ELSE NULL
        END AS action
    FROM
        ma_above_with_prev
),
-- match buy and sell into one row
transaction_pair AS (
    SELECT
        prev_date AS date_buy_signal,
        date AS date_sell_signal
    FROM (
        SELECT
            date,
            action,
            LAG(date) OVER (ORDER BY date ASC) AS prev_date,
            LAG(action) OVER (ORDER BY date ASC) AS prev_action
        FROM
            action
        WHERE
            action IS NOT NULL
    ) AS action_with_prev
    WHERE
        prev_action = 'buy' AND
        action = 'sell'
)
SELECT
    'FB' AS ticker,
    date_buy_signal,
    MAX(close) FILTER (WHERE prev_date = date_buy_signal) AS price_buy,
    MAX(formatted_date) FILTER (WHERE prev_date = date_buy_signal)
        AS formatted_date_buy,
    date_sell_signal,
    MAX(close) FILTER (WHERE prev_date = date_sell_signal) AS price_sell,
    MAX(formatted_date) FILTER (WHERE prev_date = date_sell_signal)
        AS formatted_date_sell
FROM
    transaction_pair LEFT JOIN (
        SELECT
            ticker,
            date,
            close,
            LAG(date) OVER (ORDER BY date ASC) AS prev_date,
            formatted_date
        FROM
            price
    ) AS price_with_prev
ON
    ticker = 'FB' AND
    prev_date IN (date_buy_signal, date_sell_signal)
WHERE NOT EXISTS (SELECT * FROM transactions)
GROUP BY date_buy_signal, date_sell_signal;

WITH data AS (
    SELECT
        COUNT(1) AS total_transactions,
        COUNT(1) FILTER (WHERE price_sell > price_buy) AS winning,
        COUNT(1) FILTER (WHERE price_sell < price_buy) AS losing,
        COUNT(1) FILTER (WHERE price_sell = price_buy) AS nothing,
        AVG(price_sell - price_buy) 
            FILTER (WHERE price_sell > price_buy) AS avg_win,
        AVG(price_sell - price_buy)
            FILTER (WHERE price_sell < price_buy) AS avg_lose,
        AVG(LN(price_sell) - LN(price_buy))
            FILTER (WHERE price_sell > price_buy) AS avg_win_ln,
        AVG(LN(price_sell) - LN(price_buy))
            FILTER (WHERE price_sell < price_buy) AS avg_lose_ln,
        -- naive revenue is the revenue if we always buy/sell 1 
        -- share, i.e., without any sophisticated position management
        SUM(price_sell - price_buy) AS naive_revenue,
        AVG(date_sell_signal - date_buy_signal) / 86400 AS avg_hold_days,
        SUM(date_sell_signal - date_buy_signal) / 86400 AS total_hold_days
    FROM
        transactions
), 
data2 AS (
    SELECT
        COUNT(1) AS num_days
    FROM
        price
    WHERE
        ticker = 'FB'
)
SELECT key, value
FROM data, data2, LATERAL (
    VALUES
        ('total_transactions', data.total_transactions),
        ('winning', data.winning),
        ('losing', data.losing),
        ('nothing', data.nothing),
        ('avg_win', data.avg_win),
        ('avg_lose', data.avg_lose),
        ('avg_win_ln', data.avg_win_ln),
        ('avg_lose_ln', data.avg_lose_ln),
        ('naive_revenue', data.naive_revenue),
        ('avg_hold_days', data.avg_hold_days),
        ('total_hold_days', data.total_hold_days),
        ('num_days', data2.num_days)
) v(key, value)
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

