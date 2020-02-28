
#!/usr/bin/python3

import logging
import sys
from multiprocessing import Pool
from os import path, makedirs
from impala.dbapi import connect

logger = logging.getLogger("AppName")
appname = "sc_customer_sales"
datadir = "/data/etlapps/lddb/data/"
logsdir = "./logs/"

# /* ==================== config added ==========================
config = {}
config['database_name'] = 'temp'
config['temp_data_dir'] = './data'
config['hdfs_dir'] = 'hdfs://nameservice1/user/jupyter'
config['dashboard_time_period'] = [
    {'type':'L3M','range':{'date_start':'2018-01-01','date_end':'2018-01-07'}},\
    {'type':'L12M','range':{'date_start':'2018-01-01','date_end':'2018-01-07'}}
    ]

config['config_date_start'] = '2018-01-01'
config['config_date_end'] = '2018-01-07'
# /* ============================================================

# /* ==================== library added ==========================
import pyspark
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
warehouse_location = abspath('spark-warehouse')
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/infrastructure/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
# /* ============================================================

# /* ==================== data processing functions  ==================
from step3_seg_item_detail_1 import *
from step4_seg_item_detail_2 import *
from step5_seg_finest_amplitude_word2vec import *
from step6_seg_pleasure_score_word2vec import *
from step7_set_customers_value import *
from step8_seg_cutoff_method import *
from customer_lifestyle_segmetation_data_processor_for_dashboard import process_data_for_dashboard
# /* ============================================================


from datetime import datetime, timedelta


def initLogger(date, debug=False):
    '''
    configurate the logger
    '''
    logfile = "{0}.{1}.log".format(appname,date)
    if not path.exists(logsdir):
        makedirs(logsdir)
    formatter = logging.Formatter('%(asctime)s %(levelname)-5s: %(message)s')
    file_handler = logging.FileHandler(path.join(logsdir, logfile))
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


def impalaexec(sql):
    '''
    execute sql by impala
    '''
    print(sql)
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config['database_name']) as conn:
        curr = conn.cursor()
        try:
            curr.execute(sql)
        except:
            logger.exception("error on execute sql: %s", sql)
            sys.exit()


def hiveexec(sql):
    '''
    execute sql by hive
    '''
    print(sql)
    with connect(host='dtla1apps11', port=10000, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019',database=config['database_name']) as conn:
        curr = conn.cursor()
        try:
            curr.execute(sql)
        except:
            logger.exception("error on execute sql: %s", sql)
            sys.exit()



if __name__ == '__main__':
    initLogger(datetime.strftime(datetime.today(), '%Y%m%d'))
    # impalaexec("invalidate metadata lddb.sc_customer_cp")
    # hiveexec("invalidate metadata lddb.sc_customer_cp")

    # -- 1. create vartefact.dept
    # -- from:    p4cm.dept 
    # -- to:      vartefact.bi_tool_data_step1;
    # -- usage:   get distinct dept_cd, by lastest date_key.
    # # hiveexec(f"DROP TABLE IF EXISTS {config['database_name']}.dept")
    hiveexec(f"""
    CREATE TABLE IF NOT EXISTS {config['database_name']}.dept AS 
    select 
        dept_id,
        dept_cd,
        dept_nm_lcl,
        dept_nm_en,
        div_id
    from (
        select
            *,
            row_number() over(partition by dept_cd order by date_key desc) as r
        from p4cm.dept
    ) as a
    where a.r = 1
    """)


    # # -- 1.2 vartefact.family_code_info
    # # -- from:    p4md.mstms
    # # -- to:      python file: 4.seg_finest_familyname.ipynb table: vartefact.seg_item_detail_1
    # # -- usage:   get family code information
    impalaexec(f"""
    CREATE TABLE IF NOT EXISTS {config['database_name']}.family_code_info  as
    select 
        concat(mstdpcd,mstmscd) as family_code,
        mstedesc as family_name,
        mstldesc as family_chn_name
    from p4md.mstms
    """)

    
    # -- 2. create vartefact.trxns_from_kudu
    # -- from:    lddb.trxns_sales_daily_kudu;
    # -- to:      vartefact.seg_customer_value;
    # -- usage:   
    # # hiveexec(f"drop table if exists {config['database_name']}.trxns_from_kudu ")
    impalaexec(f"""
    create table {config['database_name']}.trxns_from_kudu  as 
    select 
        trxns.ticket_id,
        trxns.store_code,
        trxns.territory_code,
        trxns.city_code,
        trxns.trxn_time,
        trxns.month_key,
        trxns.day_of_week,
        trxns.time_of_day,
        trxns.item_id,
        trxns.sub_id,
        trxns.dept_code,
        trxns.family_code,
        trxns.member_card,
        trxns.selling_price,
        trxns.sales_qty,
        trxns.coupon_disc,
        trxns.promo_disc,
        trxns.mpsp_disc,
        trxns.ext_amt,
        trxns.sales_amt,
        item_info.itmlbrand as item_brand,
        item_info.itmebrand,
        item_info.itmldesc,
        trxns.price,
        trxns.date_key
    from (
        select
            trxns.ticket_id,
            trxns.store_code,
            trxns.territory_code,
            trxns.city_code,
            trxns.trxn_time,
            trxns.month_key,
            dayofweek(trxns.trxn_time) as day_of_week,
            from_unixtime(unix_timestamp(trxns.trxn_time),'HH:mm:ss')as time_of_day,
            trxns.item_id,
            trxns.sub_id,
            trxns.dept_code,
            trxns.family_code,
            trxns.member_card,
            trxns.selling_price,
            trxns.sales_qty,
            trxns.coupon_disc,
            trxns.promo_disc,
            trxns.mpsp_disc,
            trxns.ext_amt,
            trxns.sales_amt,
            (trxns.sales_amt/trxns.sales_qty) as price, -- when the source of price switch, here is the place to adjust.
            trxns.date_key
        from lddb.trxns_sales_daily_kudu trxns
        where 
            member_account is not null
            and member_card is not null
            and (dept_code <> '22')
            and bp_flag is null
            and sales_qty > 0
            and pos_group != 9 
            and trxn_date >= '{config['config_date_start']}' 
            and trxn_date < '{config['config_date_end']}' 
    ) trxns
    left join p4md.itmgld item_info 
        on trxns.item_id = item_info.itmitmid 
    """)

    impalaexec(f"""
        REFRESH {config['database_name']}.trxns_from_kudu
    """)

    spark = SparkSession \
        .builder \
        .appName("Python Spark Typo Review Calculation") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
    

    # # -- 3
    # # -- vartefact.seg_item_detail_1
    try:
        step3_seg_item_detail_1(spark,config)
    except:
        logger.exception("error on step3_seg_item_detail")
        sys.exit()

    # # -- 4
    # # -- vartefact.seg_item_detail_sec
    try:
        step4_seg_item_detail_2(spark,config)
    except:
        logger.exception("error on step4_seg_item_detail_2")
        sys.exit()

    # -- 5
    # # -- seg_finest_amplitude
    try:
        step5_seg_finest_amplitude_word2vec(spark,config)
    except:
        logger.exception("error on step5_seg_finest_amplitude_word2vec")
        sys.exit()

    # # -- 6
    # # -- seg_pleasure_score
    try:
        step6_seg_pleasure_score_word2vec(spark,config)
    except:
        logger.exception("error on step6_seg_pleasure_score_word2vec")
        sys.exit()

    # # -- 7 member
    # # -- create seg_customer_value
    try:
        step7_set_customers_value(spark,config,hiveexec)
    except:
        logger.exception("error on step7_set_customers_value")
        sys.exit()

    # # -- 8
    # # -- seg_pleasure_score
    try:
        step8_seg_cutoff_method(spark,config)
    except:
        logger.exception("error on step8_seg_cutoff_method")
        sys.exit()
    
    # # /* ============================================================
    # #                        Module 1.2
    # #                        Dashboard Speed Optimization
    # # ===============================================================
    # # -- 9 
    # # -- process_data_for_dashboard
    # # -- Customer lifestyle Segmentation dashboard
    # # -- insert into: vartefact.bi_tool_tabs_seg_customer_label；
    # # -- from:        vartefact.bi_tool_data; vartefact.seg_customer_label;
    # # -- to:          vartefact.bi_tool_tabs_seg_customer_label；
    # # -- usage:   
    # # --          Customer lifestyle Segmentation dashboard 
    # # --          period_type = L3M
    # # -- drop table if exists vartefact.bi_tool_tabs_seg_customer_label ;

    try:
        process_data_for_dashboard(period = config['dashboard_time_period'],\
                    database_name = config['database_name'],header=True)
    except:
        logger.exception("error on process_data_for_dashboard function")
        sys.exit()

    



