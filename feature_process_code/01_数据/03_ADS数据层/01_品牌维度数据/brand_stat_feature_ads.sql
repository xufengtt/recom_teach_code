--@exclude_input=dw_user_item_click_log
--@exclude_input=dw_user_item_collect_log
--@exclude_input=dw_user_item_cart_log
--@exclude_input=dw_user_item_alipay_log
--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-20 17:39:59
--********************************************************************--
create table if not exists brand_stat_feature_ads (
    brand_id string,
    click_num bigint,
    collect_num bigint,
    cart_num bigint,
    alipay_num bigint
)PARTITIONED BY (ds string) LIFECYCLE 60 ;

INSERT OVERWRITE TABLE brand_stat_feature_ads PARTITION(ds=${bizdate})
select t1.brand_id, t1.click_num
    ,if(collect_num is null, 0, collect_num)
    ,if(cart_num is null, 0, cart_num)
    ,if(alipay_num is null, 0, alipay_num)
from (
    select brand_id, count(DISTINCT user_id) as click_num
    from dw_user_item_click_log
    where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd')
    group by brand_id
)t1 left join (
    select brand_id, count(DISTINCT user_id) as collect_num
    from dw_user_item_collect_log
    where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd')
    group by brand_id
)t2 on t1.brand_id=t2.brand_id
left join (
    select brand_id, count(DISTINCT user_id) as cart_num
    from dw_user_item_cart_log
    where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd')
    group by brand_id
)t3 on t1.brand_id=t3.brand_id
left join (
    select brand_id, count(DISTINCT user_id) as alipay_num
    from dw_user_item_alipay_log
    where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd')
    group by brand_id
)t4 on t1.brand_id=t4.brand_id
;