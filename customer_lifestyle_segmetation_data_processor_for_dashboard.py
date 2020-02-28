#!/usr/bin/python3

import logging
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool
from os import path, makedirs

from impala.dbapi import connect


import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import pyspark
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import re


logger = logging.getLogger("AppName")
appname = "sc_customer_sales"
datadir = "/data/etlapps/lddb/data/"
logsdir = "./logs/"
# /* ==================== config added ==========================
config_database_name = 'temp'

config_period = [{'type':'WTD','range':{'date_start':'2019-03-25','date_end':'2019-03-26'}}\
                ,{'type':'MTD','range':{'date_start':'2019-03-01','date_end':'2019-03-26'}}\
                ,{'type':'YTD','range':{'date_start':'2019-01-01','date_end':'2019-03-26'}}\
                ,{'type':'L1W','range':{'date_start':'2019-03-18','date_end':'2019-03-25'}}\
                ,{'type':'L4W','range':{'date_start':'2019-02-25','date_end':'2019-03-25'}}\
                ,{'type':'L3M','range':{'date_start':'2018-12-01','date_end':'2019-03-01'}}\
                ,{'type':'L1M','range':{'date_start':'2019-02-01','date_end':'2019-03-01'}}\
                ,{'type':'L6M','range':{'date_start':'2018-09-01','date_end':'2019-03-01'}}\
                ,{'type':'L1Y','range':{'date_start':'2018-01-01','date_end':'2019-01-01'}}\
            ]

config_period = [
    {'type':'L3M','range':{'date_start':'2018-01-01','date_end':'2018-01-07'}},\
    {'type':'L12M','range':{'date_start':'2018-01-01','date_end':'2018-01-07'}}]
# /* ============================================================


# /* ============================================================
#                        Module 1.2
#                        Dashboard Speed Optimization
# ===============================================================
# */ 
# -- 9 
# -- process_data_for_dashboard
# -- Customer lifestyle Segmentation dashboard
# -- insert into: vartefact.bi_tool_tabs_seg_customer_label；
# -- from:        vartefact.bi_tool_data; vartefact.seg_customer_labels;
# -- to:          vartefact.bi_tool_tabs_seg_customer_label；
# -- usage:   
# --          Customer lifestyle Segmentation dashboard 
# --          period_type = L3M
# -- drop table if exists vartefact.bi_tool_tabs_seg_customer_label ;

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
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config_database_name) as conn:
        curr = conn.cursor()
        try:
            curr.execute(sql)
        except:
            logger.exception("error on execute sql: %s", sql)
            sys.exit()


def process_data_for_dashboard(period,database_name,header):
    impalaexec(f"refresh {database_name}.seg_customer_labels")
    for cb in period:
        arg = {}
        arg['database_name'] = database_name
        arg['period_type'] = cb['type']
        arg['date_start'] = cb['range']['date_start']
        arg['date_end'] = cb['range']['date_end']
        print(arg)
        if header:
            sql_check = f"""drop table if exists {database_name}.bi_tool_tabs_seg_customer_label """
            impalaexec(sql_check)
            dashboard_data_process('create',arg)
        else:
            dashboard_data_process('insert',arg)
        header = False

    return

def dashboard_data_process(option,arg):
    database_name   = arg['database_name']
    period_type     = arg['period_type']
    date_start      = arg['date_start']
    date_end        = arg['date_end']
    create = f'create table {database_name}.bi_tool_tabs_seg_customer_label as'
    insert = f'insert into {database_name}.bi_tool_tabs_seg_customer_label  '
    
    if option == 'create':
        sentence1 = create
        sentence2 = ''
    elif option == 'insert':
        sentence1 = ''
        sentence2 = insert
    
    sql = f"""
    {sentence1}
    with 
        PT_1 as (
            select
                distinct
                card_account as member_card,
                brandname,
                store_code,
                territoryname,
                categoryname
            from {database_name}.bi_tool_data
            where 1=1
                and ticket_date >= '{date_start}'
                and ticket_date < '{date_end}'
                and categoryname is not null  -- only display the data with categoryname
        ),
        PT_2 as (
            select
                PT_1.brandname,
                label.label,
                label.store_code,
                stogld.stoformat as store_format,
                PT_1.territoryname,
                PT_1.categoryname
            from PT_1 
            left join {database_name}.seg_customer_labels label
                on label.member_card = PT_1.member_card
                and label.store_code = PT_1.store_code
            left join p4md.stogld 
                on stogld.stostocd = PT_1.store_code
        ),
        PT_3 as (
            select
                brandname,
                territoryname,
                categoryname,
                store_format,
                count(1) as total
            from PT_2
            group by brandname,territoryname,categoryname,store_format
        ),
        PT_4 as (
            select
                brandname,
                label,
                territoryname,
                categoryname,
                store_format,
                count(1) as number
            from PT_2
            group by brandname,label,territoryname,categoryname,store_format
        ),
        PT as (
            select
                PT_4.brandname,
                PT_4.label,
                PT_4.number,
                PT_4.territoryname,
                PT_4.categoryname,
                PT_4.store_format,
                (PT_4.number/PT_3.total) as perct
            from PT_4
            left join PT_3
                on PT_4.brandname = PT_3.brandname
                and PT_4.territoryname = PT_3.territoryname
                and PT_4.categoryname = PT_3.categoryname
                and PT_4.store_format = PT_3.store_format
        )
    {sentence2}
    select 
        brandname,
        territoryname,
        categoryname,
        store_format,
        '{period_type}' as period_type,
        (case 
            when label is null then "others"
            else label
        end) as label,
        number,
        perct
    from PT 
    """

    impalaexec(sql)

    return


if __name__ == '__main__':
    initLogger(datetime.strftime(datetime.today(), '%Y%m%d'))
    
     # -- 9 
    # -- process_data_for_dashboard
    # -- Customer lifestyle Segmentation dashboard
    # -- insert into: vartefact.bi_tool_tabs_seg_customer_label；
    # -- from:        vartefact.bi_tool_data; vartefact.seg_customer_labels;
    # -- to:          vartefact.bi_tool_tabs_seg_customer_label；
    # -- usage:   
    # --          Customer lifestyle Segmentation dashboard 
    # --          period_type = L3M
    # -- drop table if exists vartefact.bi_tool_tabs_seg_customer_label ;
    try:
        process_data_for_dashboard(period = config_period,database_name = config_database_name,header=False)
    except:
        logger.exception("error on process_data_for_dashboard function")
        sys.exit()
    


