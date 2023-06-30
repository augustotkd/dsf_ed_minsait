CREATE DATABASE IF NOT EXISTS ${TARGET_STAGE_DATABASE};

DROP TABLE IF EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL} (
    Address_Number INT,
    Business_family string,
    Business_Unit INT,
    Customer string,
    CustomerKey INT,
    Customer_Type string,
    Division INT,
    Line_of_Business string,
    Phone string,
    Region_Code INT,
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

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA};

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    Address_Number INT,
    Business_family string,
    Business_Unit INT,
    Customer string,
    CustomerKey INT,
    Customer_Type string,
    Division INT,
    Line_of_Business string,
    Phone string,
    Region_Code INT,
    Regional_Sales_Mgr string,
    Search_Type string
    )
PARTITIONED BY (DT_FOTO date)
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
    Address_Number,
    Business_family,
    Business_Unit,
    Customer,
    CustomerKey,
    Customer_Type,
    Division,
    Line_of_Business,
    Phone,
    Region_Code,
    Regional_Sales_Mgr,
    Search_Type,
    ${PARTICAO} as DT_FOTO
FROM ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL}
;