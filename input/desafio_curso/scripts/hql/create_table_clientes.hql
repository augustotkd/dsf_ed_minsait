CREATE DATABASE IF NOT EXISTS ${TARGET_STAGE_DATABASE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL} (
    Address_Number string,
    Business_family string,
    Business_Unit string,
    Customer string,
    CustomerKey string,
    Customer_Type string,
    Division string,
    Line_of_Business string,
    Phone string,
    Region_Code string,
    Regional_Sales_Mgr string,
    Search_Type string
    )
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    Address_Number string,
    Business_family string,
    Business_Unit string,
    Customer string,
    CustomerKey string,
    Customer_Type string,
    Division string,
    Line_of_Business string,
    Phone string,
    Region_Code string,
    Regional_Sales_Mgr string,
    Search_Type string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA}
PARTITION(DT_FOTO)
SELECT
    Address_Number string,
    Business_family string,
    Business_Unit string,
    Customer string,
    CustomerKey string,
    Customer_Type string,
    Division string,
    Line_of_Business string,
    Phone string,
    Region_Code string,
    Regional_Sales_Mgr string,
    Search_Type string,
    ${PARTICAO} as DT_FOTO
FROM ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL}
;