create database tpch_sf100;
use tpch_sf100;

CREATE TABLE IF NOT EXISTS nation (
                                      n_nationkey INTEGER not null,
                                      n_name STRING not null,
                                      n_regionkey INTEGER not null,
                                      n_comment STRING
);

CREATE TABLE IF NOT EXISTS customer (
                                        c_custkey BIGINT not null,
                                        c_name STRING not null,
                                        c_address STRING not null,
                                        c_nationkey INTEGER not null,
                                        c_phone STRING not null,
                                        c_acctbal DECIMAL(15, 2) not null,
    c_mktsegment STRING not null,
    c_comment STRING not null
    ) ;

create table customer (
                          c_custkey INTEGER not null ,
                          c_name varchar(25) not null,
                          c_address varchar(40) not null,
                          c_nationkey INTEGER not null,
                          c_phone char(15) not null,
                          c_acctbal numeric(12,2) not null,
                          c_mktsegment char(10) not null,
                          c_comment varchar(117) not null
);

CREATE TABLE IF NOT EXISTS nation (
                                      n_nationkey INTEGER not null,
                                      n_name STRING not null,
                                      n_regionkey INTEGER not null,
                                      n_comment STRING
) ;

CREATE TABLE IF NOT EXISTS orders (
                                      o_orderkey BIGINT not null,
                                      o_custkey BIGINT not null,
                                      o_orderstatus STRING not null,
                                      o_totalprice DECIMAL(15, 2) not null,
    o_orderdate DATE not null,
    o_orderpriority STRING not null,
    o_clerk STRING not null,
    o_shippriority INTEGER not null,
    o_comment STRING not null
    );

CREATE TABLE IF NOT EXISTS lineitem (
                                        l_orderkey BIGINT not null,
                                        l_partkey BIGINT not null,
                                        l_suppkey BIGINT not null,
                                        l_linenumber BIGINT not null,
                                        l_quantity DECIMAL(15, 2) not null,
    l_extendedprice DECIMAL(15, 2) not null,
    l_discount DECIMAL(15, 2) not null,
    l_tax DECIMAL(15, 2) not null,
    l_returnflag STRING not null,
    l_linestatus STRING not null,
    l_shipdate DATE not null,
    l_commitdate DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct STRING not null,
    l_shipmode STRING not null,
    l_comment STRING not null
    ) ;

CREATE TABLE IF NOT EXISTS partsupp (
                                        ps_partkey BIGINT not null,
                                        ps_suppkey BIGINT not null,
                                        ps_availqty BIGINT not null,
                                        ps_supplycost DECIMAL(15, 2) not null,
    ps_comment STRING not null
    );

CREATE TABLE IF NOT EXISTS part (
                                    p_partkey BIGINT not null,
                                    p_name STRING not null,
                                    p_mfgr STRING not null,
                                    p_brand STRING not null,
                                    p_type STRING not null,
                                    p_size INTEGER not null,
                                    p_container STRING not null,
                                    p_retailprice DECIMAL(15, 2) not null,
    p_comment STRING not null
    ) ;

CREATE TABLE IF NOT EXISTS region (
                                      r_regionkey INTEGER not null,
                                      r_name STRING not null,
                                      r_comment STRING
);

CREATE TABLE IF NOT EXISTS supplier (
                                        s_suppkey BIGINT not null,
                                        s_name STRING not null,
                                        s_address STRING not null,
                                        s_nationkey INTEGER not null,
                                        s_phone STRING not null,
                                        s_acctbal DECIMAL(15, 2) not null,
    s_comment STRING not null
    ) ;

-- TPC-H 15-create
create view revenue0 (supplier_no, total_revenue) as
select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    snowflake_sample_data.tpch_sf1.lineitem
where
        l_shipdate >= date '1996-01-01'
  AND l_shipdate < DATEADD(month, 3, '1996-01-01')
group by
    l_suppkey;


COPY INTO customer FROM 's3://redshift-downloads/TPC-H/2.18/100GB/customer/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO lineitem FROM 's3://redshift-downloads/TPC-H/2.18/100GB/lineitem/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO nation FROM 's3://redshift-downloads/TPC-H/2.18/100GB/nation/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO orders FROM 's3://redshift-downloads/TPC-H/2.18/100GB/orders/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO partsupp FROM 's3://redshift-downloads/TPC-H/2.18/100GB/partsupp/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO part FROM 's3://redshift-downloads/TPC-H/2.18/100GB/part/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO region FROM 's3://redshift-downloads/TPC-H/2.18/100GB/region/' file_format =(TYPE = CSV, field_delimiter = '|');
COPY INTO supplier FROM 's3://redshift-downloads/TPC-H/2.18/100GB/supplier/' file_format =(TYPE = CSV, field_delimiter = '|');
