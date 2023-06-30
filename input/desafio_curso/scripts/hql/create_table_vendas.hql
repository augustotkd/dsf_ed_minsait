CREATE DATABASE IF NOT EXISTS ${TARGET_STAGE_DATABASE};

DROP TABLE IF EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL} (
    Actual_Delivery_Date string,
    CustomerKey string,
    DateKey string,
    Discount_Amount string,
    Invoice_Date string,
    Invoice_Number string,
    Item_Class string,
    Item_Number string,
    Item string,
    Line_Number string,
    List_Price string,
    Order_Number string,
    Promised_Delivery_Date string,
    Sales_Amount string,
    Sales_Amount_Based_on_List_Price string,
    Sales_Cost_Amount string,
    Sales_Margin_Amount string,
    Sales_Price string,
    Sales_Quantity string,
    Sales_Rep string,
    U_M string
    )
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA};

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    Actual_Delivery_Date string,
    CustomerKey INT,
    DateKey string,
    Discount_Amount decimal(29,2),
    Invoice_Date string,
    Invoice_Number INT,
    Item_Class string,
    Item_Number INT,
    Item string,
    Line_Number INT,
    List_Price decimal(29,2),
    Order_Number INT,
    Promised_Delivery_Date string,
    Sales_Amount decimal(29,2),
    Sales_Amount_Based_on_List_Price decimal(29,2),
    Sales_Cost_Amount decimal(29,2),
    Sales_Margin_Amount decimal(29,2),
    Sales_Price decimal(29,2),
    Sales_Quantity INT,
    Sales_Rep INT,
    U_M string
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
    Actual_Delivery_Date,
    CustomerKey,
    DateKey,
    replace(Discount_Amount,",","."),
    Invoice_Date,
    Invoice_Number,
    Item_Class,
    Item_Number,
    Item,
    Line_Number,
    replace(List_Price,",","."),
    Order_Number,
    Promised_Delivery_Date,
    replace(Sales_Amount,",","."),
    replace(Sales_Amount_Based_on_List_Price,",","."),
    replace(Sales_Cost_Amount,",","."),
    replace(Sales_Margin_Amount,",","."),
    replace(Sales_Price,",","."),
    Sales_Quantity,
    Sales_Rep,
    U_M,
    ${PARTICAO} as DT_FOTO
FROM ${TARGET_STAGE_DATABASE}.${TARGET_TABLE_EXTERNAL}
;