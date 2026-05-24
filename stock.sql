CREATE TABLE IF NOT EXISTS public.stock_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    symbol VARCHAR(10),
    price NUMERIC(10,2)
)

select *
from stock_prices
order by price;

select symbol, price, count(*)
from stock_prices
group by 1,2;

select *
from stock_prices
where price = 301.53;