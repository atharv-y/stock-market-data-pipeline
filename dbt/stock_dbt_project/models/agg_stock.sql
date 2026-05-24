-- models/agg_stock.sql

{{
	config(
		materialized='incremental',
		unique_key=['stock_date', 'symbol']
	)
}}

select timestamp::date as stock_date, symbol, sum(price) as overall_stock_price, round(avg(price), 3) as avg_stock_price
from {{ ref('raw_stock_data') }}
{% if is_incremental() %}
	WHERE timestamp::date > (SELECT MAX(stock_date) FROM {{ this }})
{% endif %}
group by 1, 2