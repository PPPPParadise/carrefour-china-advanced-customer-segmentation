### 0. Initialization

import numpy as np
import pandas as pd
import pyspark
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
import math
import time,datetime


def step7_set_customers_value(spark,config,hiveexec):
    # Extract city code from vartefact.trxns_from_kudu
    # From: vartefact.trxns_from_kudu
    # To: dataframe city_code
    # Usage: Retrieve city_code as list for assigning customers by cities
    sql_check = f"""
    select 
        distinct
        city_code
    from {config['database_name']}.trxns_from_kudu
    """
    city_code = spark.sql(sql_check)
    city_code = city_code.toPandas()

    city_list = list(city_code['city_code'])

    # generate customer value by city
    # User need to have privileges for CREATETABLE

    def generate_seg_customer_value(hiveexec,city_list,p_len = 10,header=True):
        city_code_list = city_list
        range_num = math.ceil(len(city_code_list)/p_len)
        # header = True
        for i in range(range_num):
            sql_str = ''
            for times in range(p_len):
                try:
                    sql_str += '"' + city_code_list.pop(0) + '", '
                except Exception as e:
                    print(e)
            city_code_str = sql_str[:-2]
            print(city_code_str)
            if header:
                sql_check = f""" drop table if exists {config['database_name']}.seg_customer_value """
                result = hiveexec(sql_check)
                get_seg_customer_value(hiveexec,city_code_str,'create')
            else:
                get_seg_customer_value(hiveexec,city_code_str,'insert')
            header = False

    #====================================================================
    #   Get Three dimension value based on the Formula we created
    #====================================================================
        
    def get_seg_customer_value(hiveexec,city_code_str,option):
        create = f"create table {config['database_name']}.seg_customer_value as"
        insert = f"insert into  {config['database_name']}.seg_customer_value "
        if option == 'create':
            sentence1 = create
            sentence2 = ''
        elif option == 'insert':
            sentence1 = ''
            sentence2 = insert
        sql_check = f"""
        {sentence1}
        with 
            trxns as (
                select 
                    *
                from {config['database_name']}.trxns_from_kudu --按城市筛选transactions
                where 
                    city_code in ({city_code_str})
            ),
            member_info as (
                select
                    member_card,
                    city_code,
                    store_code,
                    cast(count(distinct ticket_id) as int) as ticket_num
                from trxns
                group by city_code, store_code, member_card --按city、store、member 汇总trxns
            ),
            -- Formula discount
            item_info_disc as (
                select
                    item_id,
                    city_code,
                    max(trxns.price) as max_price,                          --算出最高单位价格
                    count(DISTINCT trxns.ticket_id) as tickets_num,         --总共购买过的次数
                    count(DISTINCT trxns.member_card) AS member_card_num,   --总共购买过的人
                    sum(trxns.sales_qty) as total_num,                      --总共销售量
                    (sum(trxns.sales_qty)/count(DISTINCT trxns.ticket_id)) as unit_quantity, --每单平均件数 
                    (LOG10(count(DISTINCT trxns.member_card)+1)) as disc_weight --get item discount weight
                from trxns
                group by city_code, item_id  --by city,by item 
            ),
            ticket_info_disc as (
                select 
                    distinct
                    trxns.member_card,
                    trxns.city_code,
                    trxns.store_code,
                    trxns.ticket_id,
                    trxns.item_id,
                    trxns.dept_code,
                    ((1 - ((trxns.price)/(info.max_price))) ) as disc_amplitude,
                    trxns.sales_qty as quantity
                from trxns
                left join item_info_disc info
                    on info.item_id = trxns.item_id
                    and info.city_code = trxns.city_code
            ),
            disc_value_each as ( 
                select 
                    ticekt_item.member_card,
                    ticekt_item.store_code,
                    ticekt_item.ticket_id,
                    ticekt_item.item_id,
                    (ticekt_item.disc_amplitude*item.disc_weight*pow((ticekt_item.quantity/item.unit_quantity),2)) as E1,
                    (item.disc_weight*pow((ticekt_item.quantity/item.unit_quantity),2)) as E2
                from ticket_info_disc ticekt_item
                left join item_info_disc item 
                    on item.item_id = ticekt_item.item_id 
                    and item.city_code = ticekt_item.city_code 
            ), 
            disc_value_sum as (
                select 
                    member_card,
                    store_code,
                    sum(E1)/sum(E2) as E_value
                from disc_value_each 
                group by member_card,store_code
            ),
            -- finest
            trxn_brand_family as (
                select 
                    distinct
                    trxns.ticket_id,
                    trxns.item_id,
                    item_.brand_family_ as brand_family
                from trxns
                left join {config['database_name']}.seg_item_detail_2 item_ ---mapping transaction 中item的brand_family信息
                    on trxns.item_id = item_.item_id
            ),
            brand_family_info as (
                select
                    trxn_.brand_family,
                    count(distinct trxn_.ticket_id) as ticket_num,
                    (1/LOG10(count(distinct trxn_.ticket_id) + 1)) as weight ---brand_family 粒度的finest_weight 
                from trxn_brand_family trxn_
                group by brand_family
            ),
            item_info_finest as (
                select 
                    item_.item_id, 
                    item_.brand_family_ as brand_family, 
                    (case 
                        when amplitude_.amplitude is null then 0  --finest_score 为空的时候，取0
                        else amplitude_.amplitude end) as amplitude,
                    brand_family_info.weight 
                from {config['database_name']}.seg_item_detail_1 item_
                left join {config['database_name']}.seg_finest_amplitude amplitude_
                    on item_.brand_family_ = amplitude_.brand_family_
                left join brand_family_info
                    on item_.brand_family_ = brand_family_info.brand_family
            ),
            ticket_info_finest as (
                select 
                    distinct
                    trxns.store_code,
                    trxns.ticket_id,
                    trxns.item_id,
                    trxns.member_card,
                    (item_.weight*amplitude) as E1,
                    item_.weight as E2
                from 
                    trxns
                left join item_info_finest item_
                    on trxns.item_id = item_.item_id
            ),
            finest_value_sum as (
                select 
                    member_card,
                    store_code,
                    sum(E1)/sum(E2) as E_value
                from ticket_info_finest 
                group by member_card,store_code
            ),
            -- pleasure
            trxns_pleasure as (
                select  
                    distinct
                    trxns.member_card,
                    trxns.store_code,
                    trxns.dept_code,
                    trxns.family_code,
                    trxns.item_brand,
                    trxns.item_id,
                    trxns.ticket_id,
                    family_score.n_score as pleasure_amplitude,
                    trxns.sales_qty as quantity
                from trxns 
                left join {config['database_name']}.seg_pleasure_score family_score
                    on family_score.family_code = trxns.family_code
                where 
                    family_score.n_score is not null
            ),
            pleasure_value_each as (
                select 
                    ticket.member_card,
                    ticket.store_code,
                    ticket.item_id,
                    (ticket.pleasure_amplitude*1*(ticket.quantity/item.unit_quantity)) as E1,
                    (1*(ticket.quantity/item.unit_quantity)) as E2
                from trxns_pleasure ticket
                left join item_info_disc item
                    on item.item_id = ticket.item_id
            ),
            pleasure_value_sum as (
                select 
                    member_card,
                    store_code,
                    (sum(E1)/sum(E2)) as pleasure_value
                from pleasure_value_each 
                group by member_card,store_code
            )
        {sentence2}
        select 
            member_info.member_card,
            member_info.city_code,
            member_info.store_code,
            finest_.E_value as value_f,
            disc_.E_value as value_d,
            pleasure_.pleasure_value as value_p,
            member_info.ticket_num
        from member_info
        left join finest_value_sum finest_
            on member_info.member_card = finest_.member_card
            and member_info.store_code = finest_.store_code
        left join disc_value_sum disc_
            on member_info.member_card = disc_.member_card
            and member_info.store_code = disc_.store_code
        left join pleasure_value_sum pleasure_
            on member_info.member_card = pleasure_.member_card
            and member_info.store_code = pleasure_.store_code
        """
        result = hiveexec(sql_check)
        print('--------')
        return result
        
    generate_seg_customer_value(hiveexec,city_list,header=True)

    