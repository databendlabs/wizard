CREATE DATABASE tpch_sf100;

USE tpch_sf100;

DROP TABLE IF EXISTS customer;
CREATE TABLE IF NOT EXISTS customer (
                                        c_custkey    BIGINT NOT NULL,
                                        c_name       STRING NOT NULL,
                                        c_address    STRING NOT NULL,
                                        c_nationkey  INTEGER NOT NULL,
                                        c_phone      STRING NOT NULL,
                                        c_acctbal    DECIMAL(15, 2) NOT NULL,
    c_mktsegment STRING NOT NULL,
    c_comment    STRING NOT NULL
    )
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS lineitem;
CREATE TABLE IF NOT EXISTS lineitem (
                                        l_orderkey       BIGINT NOT NULL,
                                        l_partkey        BIGINT NOT NULL,
                                        l_suppkey        BIGINT NOT NULL,
                                        l_linenumber     BIGINT NOT NULL,
                                        l_quantity       DECIMAL(15, 2) NOT NULL,
    l_extendedprice  DECIMAL(15, 2) NOT NULL,
    l_discount       DECIMAL(15, 2) NOT NULL,
    l_tax            DECIMAL(15, 2) NOT NULL,
    l_returnflag     STRING NOT NULL,
    l_linestatus     STRING NOT NULL,
    l_shipdate       DATE NOT NULL,
    l_commitdate     DATE NOT NULL,
    l_receiptdate    DATE NOT NULL,
    l_shipinstruct   STRING NOT NULL,
    l_shipmode       STRING NOT NULL,
    l_comment        STRING NOT NULL
    )
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS nation;
CREATE TABLE IF NOT EXISTS nation (
                                      n_nationkey INTEGER NOT NULL,
                                      n_name      STRING NOT NULL,
                                      n_regionkey INTEGER NOT NULL,
                                      n_comment   STRING
)
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS orders;
CREATE TABLE IF NOT EXISTS orders (
                                      o_orderkey      BIGINT NOT NULL,
                                      o_custkey       BIGINT NOT NULL,
                                      o_orderstatus   STRING NOT NULL,
                                      o_totalprice    DECIMAL(15, 2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority STRING NOT NULL,
    o_clerk         STRING NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       STRING NOT NULL
    )
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS partsupp;
CREATE TABLE IF NOT EXISTS partsupp (
                                        ps_partkey     BIGINT NOT NULL,
                                        ps_suppkey     BIGINT NOT NULL,
                                        ps_availqty    BIGINT NOT NULL,
                                        ps_supplycost  DECIMAL(15, 2) NOT NULL,
    ps_comment     STRING NOT NULL
    )
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS part;
CREATE TABLE IF NOT EXISTS part (
                                    p_partkey      BIGINT NOT NULL,
                                    p_name         STRING NOT NULL,
                                    p_mfgr         STRING NOT NULL,
                                    p_brand        STRING NOT NULL,
                                    p_type         STRING NOT NULL,
                                    p_size         INTEGER NOT NULL,
                                    p_container    STRING NOT NULL,
                                    p_retailprice  DECIMAL(15, 2) NOT NULL,
    p_comment      STRING NOT NULL
    )
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS region;
CREATE TABLE IF NOT EXISTS region (
                                      r_regionkey INTEGER NOT NULL,
                                      r_name      STRING NOT NULL,
                                      r_comment   STRING
)
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';

DROP TABLE IF EXISTS supplier;
CREATE TABLE IF NOT EXISTS supplier (
                                        s_suppkey   BIGINT NOT NULL,
                                        s_name      STRING NOT NULL,
                                        s_address   STRING NOT NULL,
                                        s_nationkey INTEGER NOT NULL,
                                        s_phone     STRING NOT NULL,
                                        s_acctbal   DECIMAL(15, 2) NOT NULL,
    s_comment   STRING NOT NULL
    )
    STORAGE_FORMAT = 'native'
    COMPRESSION = 'lz4';


-- ETL
COPY INTO customer
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/customer/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO lineitem
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/lineitem/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO nation
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/nation/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO orders
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/orders/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO part
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/part/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO partsupp
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/partsupp/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO region
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/region/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');

COPY INTO supplier
    FROM 's3://redshift-downloads/TPC-H/2.18/100GB/supplier/'
    CONNECTION = (allow_anonymous = 'true', region = 'us-east-1')
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|');
