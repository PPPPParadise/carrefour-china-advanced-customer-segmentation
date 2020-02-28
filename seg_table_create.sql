

-- dataflow step

-- 1. create vartefact.dept
-- from:    p4cm.dept 
-- to:      vartefact.bi_tool_data_step1;
-- usage:   get distinct dept_cd, by lastest date_key.
DROP TABLE IF EXISTS vartefact.dept;
CREATE TABLE vartefact.dept AS 
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
where a.r = 1;

-- 1.2 vartefact.family_code_info
-- from:    p4md.mstms
-- to:      python file: 4.seg_finest_familyname.ipynb table: vartefact.seg_item_detail_1
-- usage:   get family code information
DROP TABLE IF EXISTS vartefact.family_code_info;
CREATE TABLE IF NOT EXISTS vartefact.family_code_info  as
select 
    concat(mstdpcd,mstmscd) as family_code,
    mstedesc as family_name,
    mstldesc as family_chn_name
from p4md.mstms
;

-- 1.2 vartefact.family_code_info
-- from:    p4md.mstms
-- to:      python file: 4.seg_finest_familyname.ipynb table: vartefact.seg_item_detail_1
-- usage:   get family code information
DROP TABLE IF EXISTS vartefact.family_code_info;
CREATE TABLE IF NOT EXISTS vartefact.family_code_info  as
select 
    concat(mstdpcd,mstmscd) as family_code,
    mstedesc as family_name,
    mstldesc as family_chn_name
from p4md.mstms
where length(concat(mstdpcd,mstmscd)) = 4;


-- 2. create vartefact.trxns_from_kudu
-- from:    lddb.trxns_sales_daily_kudu;
-- to:      vartefact.seg_customer_value;
-- usage:   
drop table if exists vartefact.trxns_from_kudu ;
create table vartefact.trxns_from_kudu  as 
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
        and year_key = '2018'
) trxns
left join p4md.itmgld item_info 
    on trxns.item_id = item_info.itmitmid 
;


-- update at 2019-04-18
-- 2. create vartefact.trxns_from_kudu
-- from:    lddb.trxns_sales_daily_kudu;p4md.itmgld;
-- to:      vartefact.seg_customer_value;
-- usage:   
drop table if exists vartefact.trxns_from_kudu ;
create table vartefact.trxns_from_kudu  as 
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
    (substring(concat(item_info.itmdpcd,item_info.itmitmcd),1,4)) as family_code,
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
        (trxns.sales_amt/trxns.sales_qty) as price, 
        trxns.date_key
    from lddb.trxns_sales_daily_kudu trxns
    where 
        member_account is not null
        and member_card is not null
        and (dept_code <> '22')
        and bp_flag is null
        and sales_qty > 0
        and pos_group != 9 
        and year_key = '2018'
) trxns
left join p4md.itmgld item_info 
    on trxns.item_id = item_info.itmitmid 
;

-- 3
-- vartefact.seg_item_detail_1
3.seg_item_detail.ipynb


-- 4
-- vartefact.seg_item_detail_2
4.seg_finest_familyname.ipynb


-- 5
-- seg_finest_amplitude
5.seg_finest_amplitude_word2vec


-- 6
-- seg_pleasure_score
6.seg_pleasure_score_word2vec.ipynb


-- 7 member in each group, base on the city
-- 2019-03-04 create seg_customer_value
drop table if exists vartefact.seg_customer_value ;
CREATE TABLE IF NOT EXISTS vartefact.seg_customer_value
   (member_card STRING,city_code STRING,store_code STRING, value_f float, value_d float, value_p float, ticket_num int );
create table vartefact.seg_customer_value  as 
    with 
        trxns as (
            select 
                *
            from vartefact.trxns_from_kudu
            where 
                city_code in ({city_code_str}) --按城市筛选transactions
        ),
        member_info as (
            select
                member_card,
                city_code,
                store_code,
                cast(count(distinct ticket_id) as int) as ticket_num
            from trxns
            group by city_code, store_code, member_card  --按city、store、member 汇总trxns
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
                (LOG10(count(DISTINCT trxns.member_card)+1)) as disc_weight --get item discount weight --get item discount weight
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
            left join vartefact.seg_item_detail_2 item_
                on trxns.item_id = item_.item_id   ---mapping transaction 中item的brand_family信息
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
                    when amplitude_.amplitude is null then 0   --finest_score 为空的时候，取0
                    else amplitude_.amplitude end) as amplitude,
                brand_family_info.weight 
            from vartefact.seg_item_detail_1 item_
            left join vartefact.seg_finest_amplitude amplitude_
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
            left join vartefact.seg_pleasure_score family_score
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




-- 8 BI query example
-- # -- process_data_for_dashboard
-- # -- Customer lifestyle Segmentation dashboard
-- # -- insert into: vartefact.bi_tool_tabs_seg_customer_label；
-- # -- from:        vartefact.bi_tool_data; vartefact.seg_customer_label;
-- # -- to:          vartefact.bi_tool_tabs_seg_customer_label；
-- # -- usage:   
-- # --          Customer lifestyle Segmentation dashboard 
-- # --          period_type = L3M
-- # -- drop table if exists vartefact.bi_tool_tabs_seg_customer_label ;
drop table if exists vartefact.bi_tool_tabs_seg_customer_label ;
create table vartefact.bi_tool_tabs_seg_customer_label as
with 
    L3M_1 as (
        select
            distinct
            card_account as member_card,
            brandname,
            store_code,
            territoryname,
            categoryname
        from vartefact.bi_tool_data
        where 1=1
            and ticket_date >= '2018-10-01'
            and ticket_date <= '2018-12-31'
            and categoryname is not null  -- only display the data with categoryname
    ),
    L3M_2 as (
        select
            L3M_1.brandname,
            label.label,
            label.store_code,
            stogld.stoformat as store_format,
            L3M_1.territoryname,
            L3M_1.categoryname
        from L3M_1 
        left join vartefact.seg_customer_label label
            on label.member_card = L3M_1.member_card
            and label.store_code = L3M_1.store_code
        left join p4md.stogld 
            on stogld.stostocd = L3M_1.store_code
            
    ),
    L3M_3 as (
        select
            brandname,
            territoryname,
            categoryname,
            store_format,
            count(1) as total
        from L3M_2
        group by brandname,territoryname,categoryname,store_format
    ),
    L3M_4 as (
        select
            brandname,
            label,
            territoryname,
            categoryname,
            store_format,
            count(1) as number
        from L3M_2
        group by brandname,label,territoryname,categoryname,store_format
    ),
    L3M as (
        select
            L3M_4.brandname,
            L3M_4.label,
            L3M_4.number,
            L3M_4.territoryname,
            L3M_4.categoryname,
            L3M_4.store_format,
            (L3M_4.number/L3M_3.total) as perct
        from L3M_4
        left join L3M_3
            on L3M_4.brandname = L3M_3.brandname
            and L3M_4.territoryname = L3M_3.territoryname
            and L3M_4.categoryname = L3M_3.categoryname
            and L3M_4.store_format = L3M_3.store_format
    )
-- insert into vartefact.bi_tool_tabs_seg_customer_label
select 
    brandname,
    territoryname,
    categoryname,
    store_format,
    'L3M' as period_type,
    (case 
        when label is null then "others"
        else label
    end) as label,
    number,
    perct
from L3M 




