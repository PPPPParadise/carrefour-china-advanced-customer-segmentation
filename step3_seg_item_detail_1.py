
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pyspark.sql import SQLContext
import pandas as pd
from decimal import * 
import os
import re

def step3_seg_item_detail_1(spark,config):
    ### 1. Read All Family Code
    #===================================================#
    # From: vartefact.trxns_from_kudu, p4md.itmgld, vartefact.family_code_info
    # To: Pandas dataframe df_full
    # Usage: Get distinct 'item_id', 'item_brand', 'itmldesc', 'family_code', 'item_brand_', 
    #        and 'brand_family'

    # Note: item information source talbe should switch to p4md.itmgld
    #===================================================#

    # when we cant read kudu table(p4md.itmgld) directly
    itmgld=spark.read.format('org.apache.kudu.spark.kudu') \
    .option('kudu.master', "dtla1apps11:7051,dtla1apps12:7051,dtla1apps13:7051") \
    .option('kudu.table', "impala::p4md.itmgld") \
    .load()
    itmgld.registerTempTable('p4md_itmgld')

    sql_focus_family = f"""
    -- From: vartefact.trxns_from_kudu; vartefact.family_code_info;
    -- To: df_full
    -- Usage: Get distinct item_id, item_brand, itmldesc, family_code,
    --        item_brand_ and family_chn_name
    select 
        *,
        concat(item_brand__, '_', family_chn_name) as brand_family
    from (
        select 
            distinct
            item_info.item_id,
            item_info.item_brand,
            item.itmldesc,
            item_info.family_code,
            ( item_info.item_brand ) as item_brand__, 
            family_code_info.family_chn_name 
        from 
            -- From: vartefact.trxn_from_kudu
            -- To: item_info
            -- Usage: get distinct item_id, item_brand, family_code, dept_code
            (
                select
                    distinct
                    item_id,
                    item_brand,
                    family_code,
                    dept_code
                from {config['database_name']}.trxns_from_kudu 
            ) item_info
        left join p4md_itmgld item -- kudu table p4md.itmgld
            on item_info.item_id = item.itmitmid 
        left join {config['database_name']}.family_code_info
            on item_info.family_code = family_code_info.family_code
    ) as a
    where itmldesc is not null
    """

    # Convert data into pandas DataFrame
    df_full = spark.sql(sql_focus_family).toPandas()

    # Try to find a new brand name from itmldesc
    # Logic: Keep the shortest name, to cover the most similar itmldes.
    def retrieve_brand(brand):
        brand_now = brand
        w = 2
        descs = [_ for _ in df_itmldesc['itmldesc'] if _.startswith(brand_now)]
        len_descs = len(descs)
        for i in range(10):
            brand_previous = brand_now
            len_descs_p = len_descs
            brand_now = descs[0][:w+1]
            descs = [_ for _ in df_itmldesc['itmldesc'] if _.startswith(brand_now)]
            len_descs = len(descs)
            if len_descs < len_descs_p:
                break
        return brand_previous

    # Usage: Get unique family_code
    family_codes = set(df_full.family_code)

    # Add one columns brand_, as the new brand name we denote for each item.
    dfs = []
    for family_code in family_codes:
        df_itmldesc = df_full[df_full.family_code == family_code].copy()
        brand_first2_w = list(set([_[:2] for _ in df_itmldesc['itmldesc']]))
        brand_dict = dict([(_, retrieve_brand(_)) for _ in brand_first2_w])
        df_itmldesc['brand_'] = df_itmldesc.apply(lambda _: brand_dict.get(_['itmldesc'][:2], ''), axis=1)
        dfs.append(df_itmldesc)


    # Recreate the dataframe
    df_merged = pd.concat(dfs)

    # Deal empty rows
    def refine_brand_family(row):
        if row.item_brand__ == '' or row.item_brand__ is None:
            return row.brand_ + '_' + row.family_chn_name
        else:
            return row.item_brand__ + '_' + row.family_chn_name

    df_merged['brand_family_'] = df_merged.apply(refine_brand_family,axis=1)

    ### 3. Import data to Hive

    df_merged.to_csv(f'{config["temp_data_dir"]}/seg_step3.csv', index = False)
    linux_command_result = os.system(f'hadoop fs -put -f {config["temp_data_dir"]}/seg_step3.csv  {config["hdfs_dir"]}/seg_step3.csv ')
    if linux_command_result != 0:
        raise Exception("""upload seg_customer_labels.csv to hdfs failed. 
    (due to the result is large, we upload the result in csv format first, then insert into the database.)
        """)

    sqlContext = SQLContext(spark)
    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('seg_step3.csv')
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database_name']}.seg_item_detail_1")

