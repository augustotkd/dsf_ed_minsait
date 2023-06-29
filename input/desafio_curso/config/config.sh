#!/bin/bash

DATE="$(date --date="-0 day" "+%Y%m%d")"
PARTICAO="$(date --date="-0 day" "+%Y%m%d")"

TABLES=("clientes" "divisao" "endereco" "regiao" "vendas")

TARGET_STAGE_DATABASE="stg_desafio_curso"
TARGET_DATABASE="desafio_curso"
HDFS_DIR="/datalake/raw/"
#TARGET_TABLE_EXTERNAL="categoria"
#TARGET_TABLE_GERENCIADA="tbl_categoria"
