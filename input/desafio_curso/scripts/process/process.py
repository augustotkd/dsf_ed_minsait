from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive

# Tabela Clientes
df_clientes = spark.sql("select * from desafio_curso.tbl_clientes")
# Tabela Divisao
df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")
# Tabela Endereco
df_endereco = spark.sql("select * from desafio_curso.tbl_endereco")
# Tabela Regiao
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")
# Tabela Vendas
df_vendas = spark.sql("select * from desafio_curso.tbl_vendas")


# Espaço para tratar e juntar os campos e a criação do modelo dimensional

#Transformação Dados
df_endereco = df_endereco.withColumn('city', when(col('city').isNull() | (trim(col('city')) == ''), 'Não informado').otherwise(col('city'))) \
                         .withColumn('customer_address_1', when(col('customer_address_1').isNull() | (trim(col('customer_address_1')) == ''), 'Não informado').otherwise(col('customer_address_1'))) \
                         .withColumn('state', when(col('state').isNull() | (trim(col('state')) == ''), 'Não informado').otherwise(col('state'))) \
                         .withColumn('zip_code', when(col('zip_code').isNull() | (trim(col('zip_code')) == ''), 'Não informado').otherwise(col('zip_code')))

df_vendas = df_vendas.withColumn('discount_amount', when(col('discount_amount').isNull() | (trim(col('discount_amount')) == ''), '0').otherwise(col('discount_amount'))) \
                     .withColumn('item_number', when(col('item_number').isNull() | (trim(col('item_number')) == ''), '0').otherwise(col('item_number')))

#Alterar tipo
df_vendas = df_vendas.withColumn("discount_amount", regexp_replace(col("discount_amount"), ",", ".").cast(DoubleType())) \
                     .withColumn("list_price", regexp_replace(col("list_price"), ",", ".").cast(DoubleType())) \
                     .withColumn("sales_amount", regexp_replace(col("sales_amount"), ",", ".").cast(DoubleType())) \
                     .withColumn("sales_amount_based_on_list_price", regexp_replace(col("sales_amount_based_on_list_price"), ",", ".").cast(DoubleType())) \
                     .withColumn("sales_cost_amount", regexp_replace(col("sales_cost_amount"), ",", ".").cast(DoubleType())) \
                     .withColumn("sales_margin_amount", regexp_replace(col("sales_margin_amount"), ",", ".").cast(DoubleType())) \
                     .withColumn("sales_price", regexp_replace(col("sales_price"), ",", ".").cast(DoubleType()))

df_vendas = df_vendas.withColumn("actual_delivery_date", to_date(col("actual_delivery_date"), "dd/MM/yyyy").cast(DateType())) \
                     .withColumn("datekey", to_date(col("datekey"), "dd/MM/yyyy").cast(DateType())) \
                     .withColumn("invoice_date", to_date(col("invoice_date"), "dd/MM/yyyy").cast(DateType())) \
                     .withColumn("promised_delivery_date", to_date(col("promised_delivery_date"), "dd/MM/yyyy").cast(DateType()))

df_vendas = df_vendas.withColumn("item_number",col("item_number").cast(IntegerType()))



df_clientes.createOrReplaceTempView('clientes')
df_divisao.createOrReplaceTempView('divisao')
df_endereco.createOrReplaceTempView('endereco')
df_regiao.createOrReplaceTempView('regiao')
df_vendas.createOrReplaceTempView('vendas')


sql = '''
select
a.customerkey,
a.discount_amount,
a.invoice_date,
a.invoice_number,
a.item_number,
a.item,
a.line_number,
a.list_price,
a.order_number,
a.promised_delivery_date,
a.sales_amount,
a.sales_amount_based_on_list_price,
a.sales_cost_amount,
a.sales_margin_amount,
a.sales_price,
a.sales_quantity,
a.sales_rep,
a.u_m,

b.business_amily,
b.business_unit,
b.customer,
b.customer_type,
b.line_of_business,
b.phone,
b.regional_sales_mgr,
b.search_type,
b.dt_foto,

c.division,
c.division_name,

d.region_code,
d.region_name,

e.address_number,
e.city,
e.country,
e.customer_address_1,
e.customer_address_2,
e.customer_address_3,
e.customer_address_4,
e.state,
e.zip_code

from vendas a
left join clientes b
on a.customerkey = b.customerkey

left join divisao c
on b.division = c.division

left join regiao d
on b.region_code = d.region_code

left join endereco e
on b.address_number = e.address_number
'''

# Criação da STAGE
df_stage = spark.sql(sql)

# Criação dos Campos Calendario
df_stage = (df_stage
            .withColumn('Ano', year(df_stage.invoice_date))
            .withColumn('Mes', month(df_stage.invoice_date))
            .withColumn('Dia', dayofmonth(df_stage.invoice_date))
            .withColumn('Trimestre', quarter(df_stage.invoice_date))
           )

# Criação das Chaves do Modelo
df_stage = df_stage.withColumn("DW_CLIENTE", sha2(concat_ws("", df_stage.customer, df_stage.customerkey, df_stage.customer_type), 256))
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.invoice_date, df_stage.Ano, df_stage.Mes, df_stage.Dia), 256))
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.address_number, df_stage.city, df_stage.state, df_stage.country, df_stage.region_name), 256))

df_stage.createOrReplaceTempView('stage')


# criando o fato

#Criando a fato Vendas
ft_vendas = spark.sql('''
    SELECT 
        DW_CLIENTE,
        DW_TEMPO,
        DW_LOCALIDADE,
        sum(sales_amount) as vl_total
    FROM stage
    group by 
        DW_CLIENTE,
        DW_TEMPO,
        DW_LOCALIDADE
''')

#criando as dimensões

#Criando a dimensão Cliente
dim_cliente = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTE,
        customerkey,
        address_number,
        business_amily,
        business_unit,
        customer,
        customer_type,
        division,
        line_of_business,
        phone,
        region_code,
        regional_sales_mgr,
        search_type
    FROM stage    
''')

#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        invoice_date,
        Ano,
        Mes,
        Dia
    FROM stage   
''')

#Criando a dimensão Localidade
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        address_number,
        city,
        state,
        country,
        region_name
    FROM stage   
''')


# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_cliente, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')
salvar_df(ft_vendas, 'ft_vendas')
