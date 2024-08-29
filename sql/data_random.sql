insert into customers (
    name, email, phone, address
)
select
    md5(random()::text),
    md5(random()::text),
    left(md5(random()::text), 12),
    md5(random()::text)
from generate_series(1, 1000000) s(i);

insert into orders (
    customer_id, order_date, total
)
select
    floor(random() * (1000000-1+1) + 1)::int,
    '2024-08-02'::date + (random() * 365)::int,
    random()*(100-1)+1
from generate_series(1, 2000000) s(i);

insert into payments (
    order_id, amount, credit_card
)
select
    floor(random() * (2000000-1+1) + 1)::int,
    random()*(100-1)+1,
    md5(random()::text)
from generate_series(1, 2000000) s(i);
