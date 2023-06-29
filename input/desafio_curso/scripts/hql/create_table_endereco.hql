CREATE DATABASE IF NOT EXISTS ${TARGET_STAGE_DATABASE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL} (
    Address_Number string,
    City string,
    Country string,
    Customer_Address_1 string,
    Customer_Address_2 string,
    Customer_Address_3 string,
    Customer_Address_4 string,
    State string,
    Zip_Code string
    )
COMMENT 'Tabela de Endereco'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    Address_Number string,
    City string,
    Country string,
    Customer_Address_1 string,
    Customer_Address_2 string,
    Customer_Address_3 string,
    Customer_Address_4 string,
    State string,
    Zip_Code string
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
    City string,
    Country string,
    Customer_Address_1 string,
    Customer_Address_2 string,
    Customer_Address_3 string,
    Customer_Address_4 string,
    State string,
    Zip_Code string,
    ${PARTICAO} as DT_FOTO
FROM ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL}
;