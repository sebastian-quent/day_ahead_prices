-- creates the live prices table. currently targets test.prices; rename to prod.prices
-- at go-live (see "Until prod.prices exists" in CLAUDE.md).
-- free-text bidding_zone/product/market/source per project-overview.md Resolved Decisions
-- (no FK/dimension tables -- see id-tables-design.drawio, archived as a future idea).

create schema if not exists test;

create table test.prices (
    valuetime     timestamptz     not null,
    forecasttime  timestamptz     not null,
    bidding_zone  varchar(20)     not null,
    product       varchar(20)     not null,
    market        varchar(20)     not null,
    source        varchar(20)     not null,
    resolution    smallint        not null,
    currency      varchar(10)     not null,
    price         numeric(10, 2)  not null,
    constraint prices_pkey primary key (
        valuetime, forecasttime, bidding_zone, product, market, source
    )
);
