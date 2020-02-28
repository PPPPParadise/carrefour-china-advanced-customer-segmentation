
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pyspark.sql import SQLContext
import os
import pandas as pd
import pyspark
from os.path import expanduser, join, abspath
import csv
from decimal import * 
import re


def step4_seg_item_detail_2(spark,config):
    # From: vartefact.family_code_, vartefact.trxns_from_kudu, vartefact.dept
    # To: A pandas DataFrame object, df1
    # Usage: Get distinct family_code, family_chn_name (chinese name), 
    #        dept_nm_lcl(chinese name)
    sql = f"""
    select 
        distinct 
        family_code_info.family_code as family_code,
        family_code_info.family_chn_name,
        dept.dept_nm_lcl
    from {config['database_name']}.family_code_info 
    join (
        select 
            distinct
            family_code,
            dept_code
        from {config['database_name']}.trxns_from_kudu 
        ) trxns
        on trxns.family_code = family_code_info.family_code
    join {config['database_name']}.dept
        on trxns.dept_code = dept.dept_cd
    """
    df1 = spark.sql(sql).toPandas()
    
    ### 2. Data cleansing 
    # 1. Replace any sentences in () by '' in family_chn_name columsn
    # 2. Combine new_family_name(with kew word '进口' eliminated) and dept_name
    # 3. Combine families end with 猪肉', '鸡肉', '羊肉', '牛肉

    # data cleaning
    # Replace any sentences in () by '' in family_chn_name columsn
    df1['family_chn_name_cleaned'] = [re.sub(r'[\(|（].*[\)|）]', '', _) for _ in  df1.family_chn_name]

    df1[df1.family_chn_name.str.contains(r'\(')]

    # Combine new_family_name(with kew word '进口' eliminated) and dept_name
    # new_f do not contain imported 'water', 'snacks' or 'tea'
    df_n = df1[df1.family_chn_name.str.contains('进口')].copy()
    df_n['new_f'] = [ i.split('进口')[1] for i in df_n.family_chn_name_cleaned ]
    new_f = list(zip(df_n.new_f, df_n.dept_nm_lcl))
    new_f = [_ for _ in new_f if _[0] not in ['水','食品','茶']]

    def arrange_family_name(row):
        for f, dept in new_f:
            if row['family_chn_name_cleaned'].endswith(f) and row['dept_nm_lcl'] == dept:
                return dept + '|' + f

    # Combine dept_name and family_name in column 'n_f2'
    df1['n_f2'] = df1.apply(arrange_family_name, axis=1)

    def normalize_meat(row):
        meats = ['猪肉', '鸡肉', '羊肉', '牛肉']
        for meat in meats:
            if row['family_chn_name_cleaned'].endswith(meat):
                return meat

    # Get column 'n_f3', the family_name end with ['猪肉', '鸡肉', '羊肉', '牛肉']
    df1['n_f3'] = df1.apply(normalize_meat, axis=1)

    def f4(row):
        if row['n_f3'] is not None:
            return row['n_f3']
        elif row['n_f2'] is not None:
            return row['n_f2']
        else:
            return row['family_chn_name']

    # Fill empty rows with family_chn_name, all new names store in column 'new_family_name'
    df1['new_family_name'] = df1.apply(f4,axis=1)

    # Subset of df1
    df1 = df1[['family_code','family_chn_name','dept_nm_lcl','new_family_name']].copy()

    # From: vartefact.seg_item_detail_1 (created by seg_item_detail)
    # To: Pandas ic_item_df
    # Uasage: Get all item related infomations
    sql = f"""
    select * from {config['database_name']}.seg_item_detail_1
    """
    ic_item_df = spark.sql(sql).toPandas()

    # Left join ic_item_df and df1 on family_code, thus we get brand_family columns and new_family_name
    item_detail_2_df = pd.merge(ic_item_df, df1[['family_code','new_family_name']]\
                                , how='left', on=['family_code'])

    # Export to Hive
    # spark_df = spark.createDataFrame(item_detail_2_df)
    # spark_df.write.mode('overwrite').saveAsTable(f"{config['database_name']}.seg_item_detail_2")

    item_detail_2_df.to_csv(f'{config["temp_data_dir"]}/seg_step4.csv', index = False)
    linux_command_result = os.system(f'hadoop fs -put -f {config["temp_data_dir"]}/seg_step4.csv  {config["hdfs_dir"]}/seg_step4.csv ')
    if linux_command_result != 0:
        raise Exception("""upload seg_step4.csv to hdfs failed. 
    (due to the result is large, we upload the result in csv format first, then insert into the database.)
        """)

    sqlContext = SQLContext(spark)
    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('seg_step4.csv')
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database_name']}.seg_item_detail_2")

