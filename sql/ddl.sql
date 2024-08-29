drop table if exists payments;
drop table if exists orders;
drop table if exists customers;

create table customers
(
    id      serial primary key,
    name    varchar(100) not null,
    email   varchar(100) not null,
    phone   varchar(20)  not null,
    address varchar(100) not null
);

create table orders
(
    id          serial primary key,
    customer_id integer        not null,
    order_date  date           not null,
    total       decimal(10, 2) not null
);

create table payments
(
    id          serial primary key,
    order_id    integer        not null,
    amount      decimal(10, 2) not null,
    credit_card varchar(100)   not null
);
