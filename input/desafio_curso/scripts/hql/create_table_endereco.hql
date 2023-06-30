CREATE DATABASE IF NOT EXISTS ${TARGET_STAGE_DATABASE};

DROP TABLE IF EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL} (
    Address_Number INT,
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

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA};

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    Address_Number INT,
    City string,
    Country string,
    Customer_Address_1 string,
    Customer_Address_2 string,
    Customer_Address_3 string,
    Customer_Address_4 string,
    State string,
    Zip_Code string
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
    City,
    Country,
    Customer_Address_1,
    Customer_Address_2,
    Customer_Address_3,
    Customer_Address_4,
    State,
    Zip_Code,
    ${PARTICAO} as DT_FOTO
FROM ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL}
;