

### 0. Initialization

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from sklearn import mixture
import os
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D
from mpl_toolkits.axes_grid1 import make_axes_locatable
import math
from math import pi, cos, sin
import time
from pyspark.sql import SQLContext
import os



def step8_seg_cutoff_method(spark,config):



    #====================================================================
    #                              Functions
    #   
    #====================================================================

    def generate_seg_customer_value_csv():
        sql = f"""
        select distinct city_code from {config['database_name']}.seg_customer_value
        """
        city_code = spark.sql(sql)
        print(sql)
        city_code_df = city_code.toPandas()
        city_code_list = list(city_code_df['city_code'])
        p_len = 5
        range_num = math.ceil(len(city_code_list)/p_len)
        header = True
        for i in range(range_num):
            print(i)
            sql_str = ''
            for times in range(p_len):
                try:
                    sql_str += '"' + city_code_list.pop(0) + '", '
                except Exception as e:
                    print(e)
                    print('empty')
            sql_str = sql_str[:-2]
            print(sql_str)
            for member in range(0,10):
                sql = f"""
                    select * from {config['database_name']}.seg_customer_value
                    where city_code in ({sql_str})
                    and substr(member_card, -1) = '{member}'
                """
    #             print(sql)
                members = spark.sql(sql)
                print(member)
                members_df = members.toPandas()
                if header:
                    members_df.to_csv(f'{config["temp_data_dir"]}/seg_customer_value.csv',mode='w',index=False, header=header)
                else:
                    members_df.to_csv(f'{config["temp_data_dir"]}/seg_customer_value.csv',mode='a',index=False, header=header)
                header = False

    ## Calculate percentile
    def give_pect(df, tot_qtile = 10):
        '''
        Give the percentile value of three dimension
        '''
        
        # Percentile for value_d
        intervals = df.value_d.quantile([i/tot_qtile for i in range(tot_qtile + 1)]).drop_duplicates()
        names = intervals.index
        df['d_score'] = pd.cut(df.value_d, bins = intervals, labels=names[1: ]).astype(float)

        # Percentile for value_f
        intervals = df.value_f.quantile([i/tot_qtile for i in range(tot_qtile + 1)]).drop_duplicates()
        name = intervals.index
        df['f_score'] = pd.cut(df.value_f, bins = intervals, labels = name[1: ]).astype(float)

        # Percentile for value_p
        intervals = df.value_p.quantile([i/tot_qtile for i in range(tot_qtile + 1)]).drop_duplicates()
        name = intervals.index
        df['p_score'] = pd.cut(df.value_p, bins = intervals, labels = name[1: ]).astype(float)
        
        return df

    ## All Segmentation

    def find_smart_member(dataframe, center = (1, 0.05), x_radius = 0.25, y_radius = 0.6, rot = np.pi / 4):
        """
        center: Tuple give the position of center
        rot : rotation angle measured from x-axis ( pi/2 for example)
        x_radius: radius on x axis
        y_radius: radius on y axis
        
        # Return:
            dataframe: if follows our constrain then smart shopper else no.
        """
        h, k = center
        x = dataframe.f_score.copy()
        y = dataframe.d_score.copy()
        # calculate the y values of  correspond x
        res = ( (x - h) * np.cos(rot) + (y - k) * np.sin(rot) )**2 / x_radius**2 \
            + ( (x - h) * np.sin(rot) - (y - k) * np.cos(rot) )**2 / y_radius**2
        dataframe['smart_shopper'] = (res <= 1)
        dataframe['s_score'] = 1 - res + np.random.uniform(-0.01, 0.01, len(res))
        dataframe['s_score'] = dataframe.s_score.apply(lambda x: x if 0<= x <=1 else 0)
        return dataframe.copy()

    def find_bargain(dataframe, bargain_pect = 0.75):
        # cut_val = dataframe.value_d.quantile(bargain_pect)
        dataframe['bargain'] = (dataframe.value_d > bargain_pect)
        return dataframe.copy()

    def find_finest(dataframe, finest_pect = 0.75):
        # cut_val = dataframe.value_f.quantile(finest_pect)
        dataframe['finest'] = (dataframe.value_f > finest_pect)
        return dataframe.copy()

    def find_quick(dataframe, a = 0.25, b = 1.7):
        cut_val = (1 - a) - b * dataframe.p_score
        dataframe['quick'] = dataframe.d_score <= cut_val
        
        dataframe['q_score'] = (1 - (dataframe.d_score / cut_val) + np.random.uniform(-0.1, 0.1, len(cut_val)))
        dataframe['q_score'] = dataframe.q_score.apply(lambda x: x if 0<= x <=1 else 0)
        return dataframe.copy()

    def find_pleasure(dataframe, pleasure_pect = 0.75):
        #cut_val = dataframe.value_p.quantile(pleasure_pect)
        dataframe['pleasure'] = (dataframe.p_score >= pleasure_pect)
        return dataframe.copy()

    def find_all_seg(dataframe, percentile = (0.75, 0.75, 0.75), coef = (0.25, 1.7), center = (1, 0.05),
                    x_radius = 0.25, y_radius = 0.6, rot = np.pi / 4):
        """
        dataframe: a dataframe object contains member_card, store_code, value_d, value_f, value_p
        percentile: tuple correspond to bargain_pect, finest_pect and pleasure_pect 
        coef: coefficient to find quick shopper
        
        Control smart:
        center: Tuple give the position of center
        rot : rotation angle measured from x-axis ( pi/2 for example)
        x_radius: radius on x axis
        y_radius: radius on y axis
        """
        dataframe = find_smart_member(dataframe, 
            center = center, 
            x_radius = x_radius,
            y_radius = y_radius,
            rot = rot)
        
        dataframe = find_bargain(dataframe, bargain_pect= percentile[0])
        dataframe = find_finest(dataframe, finest_pect= percentile[1])
        dataframe = find_pleasure(dataframe, pleasure_pect= percentile[2])
        dataframe = find_quick(dataframe, a = coef[0], b = coef[1])
        return dataframe.copy()

    ##  Tag methode

    def smart_process(dat):
        '''
        Deal overlap smart
        '''
        temp = dat[dat.smart_shopper == True].copy()
        scores = ['p_score', 's_score', 'q_score']
        temp = temp[temp[scores].idxmax(axis=1) == 's_score'].copy()
        temp['label'] = 'smart'
        return temp.drop(columns = ['smart_shopper', 'bargain', 'finest', 'pleasure', 'quick'])

    def finest_process(dat):
        '''
        Deal overlap finest
        '''
        all_smart = smart_process(dat)
        temp = dat[dat.finest == True].copy()
        
        # Exclude smart customer from finest
        temp = temp[~(temp.member_card.isin(all_smart.member_card))]
        
        # Exclude quick shopper, pleasure_focus, bargain hunter
        scores = ['q_score', 'p_score', 'd_score', 'f_score']
        temp = temp[temp[scores].idxmax(axis=1) == 'f_score'].copy()
        temp['label'] = 'finest'
        return temp.drop(columns = ['smart_shopper', 'bargain', 'finest', 'pleasure', 'quick'])

    def bargain_process(dat):
        '''
        Deal overlap bargain
        '''
        all_smart = smart_process(dat)
        all_finest = finest_process(dat)
        
        temp = dat[dat.bargain == True].copy()
        # Exclude smart and finest
        cond = (temp.member_card.isin(all_smart.member_card)) | (temp.member_card.isin(all_finest.member_card))
        temp = temp[~(cond)]
        
        # # Exclude quick shopper and pleasure focus
        scores = ['d_score', 'p_score', 'f_score']
        temp = temp[temp[scores].idxmax(axis = 1) == 'd_score'].copy()
        temp['label'] = 'bargain'
        return temp.drop(columns = ['smart_shopper', 'bargain', 'finest', 'pleasure', 'quick'])

    def quick_process(dat):
        '''
        Deal overlap quick shoppper
        '''
        all_smart = smart_process(dat)
        all_finest = finest_process(dat)
        all_bargain = bargain_process(dat)
        
        temp = dat[dat.quick == True].copy()
        
        # Exclude smart, finest and bargain
        cond = ( temp.member_card.isin(all_smart.member_card) ) |\
            ( temp.member_card.isin(all_finest.member_card) )|\
            ( temp.member_card.isin(all_bargain.member_card))
        temp = temp[~(cond)]
        
        # Exclude pleasure focus
        scores = ['q_score', 'p_score']
        temp = temp[temp[scores].idxmax(axis =1) != 'p_score'].copy()
        temp['label'] = 'quick'
        return temp.drop(columns = ['smart_shopper', 'bargain', 'finest', 'pleasure', 'quick'])  

    def pleasure_process(dat):
        '''
        Deal overlap pleasure focus
        '''
        all_smart = smart_process(dat)
        all_finest = finest_process(dat)
        all_bargain = bargain_process(dat)
        all_quick = quick_process(dat)
        
        temp = dat[dat.pleasure == True].copy()
        # Exclude all others
        cond = ( temp.member_card.isin(all_smart.member_card) ) |\
            ( temp.member_card.isin(all_finest.member_card) )|\
            ( temp.member_card.isin(all_bargain.member_card) ) |\
            ( temp.member_card.isin(all_quick.member_card))
        
        temp = temp[~(cond)]
        temp['label'] = 'pleasure'
        return temp.drop(columns = ['smart_shopper', 'bargain', 'finest', 'pleasure', 'quick'])

    def deal_overlap(dat_cutoff):
        df_smt = smart_process(dat_cutoff)
        df_finest = finest_process(dat_cutoff)
        df_bargain = bargain_process(dat_cutoff)
        df_quick = quick_process(dat_cutoff)
        df_pleasure = pleasure_process(dat_cutoff)
        return pd.concat([df_smt, df_finest, df_bargain, df_quick, df_pleasure]).drop_duplicates(subset =['member_card'])

    # Start 

    generate_seg_customer_value_csv()

    # Read data from csv
    df_all = pd.read_csv(f'{config["temp_data_dir"]}/seg_customer_value.csv',
        dtype = {'member_card':str, 'store_code':str})

    # Drop all convenience store
    store_remove = df_all[df_all.store_code.str.startswith('9')].store_code.unique()
    df_all = df_all[~(df_all.store_code.isin(store_remove))]

    # Remove store 353
    df_all = df_all[df_all.store_code != '353']


    ## Get all cluster label

    # Calculate d_score, f_score and p_score
    df_use = df_all.groupby(['city_code']).apply(give_pect)

    # Denote smart_score and quick_score
    df_seg = find_all_seg(df_use)

    # Get the label for some customers
    df_label = df_seg.groupby(['store_code']).apply(deal_overlap).reset_index(drop = True)

    # Denote a segmentation for each customer
    df_res =df_seg.merge(df_label[['member_card', 'store_code', 'label']], how = 'left', on = ['member_card', 'store_code'])

    df_res.drop(columns = ['value_f', 'value_d', 'value_p'], inplace = True)

    df_res.to_csv(f'{config["temp_data_dir"]}/seg_customer_labels.csv', index = False)
    
    # /* ============== insert the result into hive ===================    
    # noted it is replace
    linux_command_result = os.system(f'hadoop fs -put -f {config["temp_data_dir"]}/seg_customer_labels.csv  {config["hdfs_dir"]}/seg_customer_labels.csv ')
    
    if linux_command_result != 0:
        raise Exception("""upload seg_customer_labels.csv to hdfs failed. 
    (due to the result is large, we upload the result in csv format first, then insert into the database.)
        """)
    
    sqlContext = SQLContext(spark)
    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('seg_customer_labels.csv')
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database_name']}.seg_customer_labels")
    # /* ==============================================================
    
