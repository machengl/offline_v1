show databases;
select *
from bigdata_offline_v1_ws.ods_activity_info1;
use bigdata_offline_v1_ws;
CREATE DATABASE IF NOT EXISTS bigdata_offline_v1_ws
    COMMENT 'test-warehouse'
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/'
    WITH DBPROPERTIES (
    'creator' = 'Eric',
    'created_date' = '2025-09-17'
    );


create external table bigdata_offline_v1_ws.ods_activity_rule(
             id               int    comment '编号',
             activity_id      int    comment '类型',
             activity_type    STRING comment '活动类型',
             condition_amount DECIMAL(10,2) comment '满减金额',
             condition_num    bigint    comment '满减件数',
             benefit_amount   DECIMAL(10,2) comment '优惠金额',
             benefit_discount DECIMAL(10,2) comment '优惠折扣',
             benefit_level    bigint    comment '优惠级别',
             create_time BIGINT comment '创建时间',
             operate_time BIGINT comment '修改时间'
)
    PARTITIONED BY (ds STRING)
    STORED AS parquet
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/ods_activity_rule/'
    TBLPROPERTIES (
        'parquet.compress' = 'SNAPPY',
        'external.table.purge' = 'true'
        );
select * from ods_activity_rule;