WITH date_range AS (
    -- Generate a date range from June to September, 2020
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS dt_report
),

filtered_trades AS (
    -- Filter trades for enabled accounts and date range
    SELECT 
        date(trades.close_time) AS dt_report,
        usr.login_hash,
        usr.server_hash,
        trades.symbol,
        usr.currency,
        sum(trades.volume) as volume,
        min(trades.open_time) as open_time,
		max(trades.close_time) as close_time,
        count(1) AS trade_count  
    FROM 
        (SELECT login_hash, server_hash, currency FROM users WHERE enable = 1) usr -- Get all enabled accounts
    LEFT JOIN 
        (SELECT * FROM trades WHERE close_time >= '2020-06-01' AND close_time < '2020-10-01') trades
    ON usr.login_hash = trades.login_hash
    AND usr.server_hash = trades.server_hash
	GROUP BY 1,2,3,4,5
),

combinations AS (
    -- Generate combinations of login_hash, server_hash, symbol, and currency
    SELECT DISTINCT login_hash, server_hash, symbol, currency
    FROM filtered_trades
),

all_dates_combinations AS (
    -- Generate all combinations of date/login_hash/server_hash/symbol/currency
    SELECT 
        d.dt_report,
        c.login_hash,
        c.server_hash,
        c.symbol,
        c.currency
    FROM 
        date_range d,combinations c
),

aggregated_data AS (
    -- Left join to get daily data and calculate rolling and cumulative sums
    SELECT 
        adc.dt_report,
        adc.login_hash,
        adc.server_hash,
        adc.symbol,
        adc.currency,
        
        -- the sum of volume traded by login/server/symbol in the previous 7 days including current dt_report
        COALESCE(SUM(ft.volume) OVER (
            PARTITION BY adc.login_hash, adc.server_hash, adc.symbol
            ORDER BY adc.dt_report
            RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
        ), 0) AS sum_volume_prev_7d,
        
        -- sum of volume traded by login/server/symbol all previous days including current dt_report
        COALESCE(SUM(ft.volume) OVER (
            PARTITION BY adc.login_hash, adc.server_hash, adc.symbol
            ORDER BY adc.dt_report
        ), 0) AS sum_volume_prev_all,
        
        -- Sum of volume for August 2020, up to and including current dt_report
        COALESCE(SUM(ft.volume) FILTER (WHERE adc.dt_report BETWEEN '2020-08-01' AND '2020-08-31') OVER (
            PARTITION BY adc.login_hash, adc.server_hash, adc.symbol
            ORDER BY adc.dt_report
        ), 0) AS sum_volume_2020_08,
        
        -- Timestamp of the first trade by login/server/symbol, up to and including current dt_report
        MIN(ft.close_time) OVER (
            PARTITION BY adc.login_hash, adc.server_hash, adc.symbol
            ORDER BY adc.dt_report
        ) AS date_first_trade,
        
        -- For dense rank: volume traded by login/symbol in previous 7 days, including current dt_report
        COALESCE(SUM(ft.volume) OVER (
            PARTITION BY adc.login_hash, adc.symbol
            ORDER BY adc.dt_report
            RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
        ), 0) AS volume_for_rank,
        
        -- For dense rank:  trade count traded by login in the previous 7 days including current  dt_report
        COALESCE(SUM(ft.trade_count) OVER (
            PARTITION BY adc.login_hash
            ORDER BY adc.dt_report
            RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
        ), 0) AS count_for_rank,
        
        ROW_NUMBER() OVER (
            ORDER BY adc.dt_report, adc.login_hash, adc.server_hash, adc.symbol
        ) AS row_number

    FROM 
        all_dates_combinations adc
    LEFT JOIN 
        filtered_trades ft 
    ON 
        adc.dt_report = ft.dt_report
        AND adc.login_hash = ft.login_hash
        AND adc.server_hash = ft.server_hash
        AND adc.symbol = ft.symbol
        AND adc.currency = ft.currency
),

final_data AS (
    SELECT 
        *,
        
        -- Dense rank of most volume traded by login/symbol in the previous 7 days
        DENSE_RANK() OVER (
            PARTITION BY login_hash, symbol
            ORDER BY volume_for_rank DESC
        ) AS rank_volume_symbol_prev_7d,
        
        -- Dense rank of most trade count by login in the previous 7 days
        DENSE_RANK() OVER (
            PARTITION BY login_hash
            ORDER BY count_for_rank DESC
        ) AS rank_count_prev_7d

    FROM 
        aggregated_data
)

SELECT dt_report
	,login_hash
	,server_hash
	,symbol
	,currency
	,sum_volume_prev_7d
	,sum_volume_prev_all
	,rank_volume_symbol_prev_7d
	,rank_count_prev_7d
	,sum_volume_2020_08
	,date_first_trade
	,row_number
FROM final_data
ORDER BY row_number DESC;
