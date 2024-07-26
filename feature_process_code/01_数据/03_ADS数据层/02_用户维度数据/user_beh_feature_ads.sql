--@exclude_input=dw_user_item_alipay_log
--@exclude_input=dw_user_item_cart_log
--@exclude_input=dw_user_item_collect_log
--@exclude_input=dw_user_item_click_log
--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-20 17:41:58
--********************************************************************--
create table if not exists user_click_beh_feature_ads (
    user_id string,
    item_num_3d BIGINT ,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,
    cnt_days_3d BIGINT,
    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,
    cnt_days_7d BIGINT,
    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,
    cnt_days_15d BIGINT,
    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,
    cnt_days_30d BIGINT,
    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,
    cnt_days_60d BIGINT,
    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT,
    cnt_days_90d BIGINT
)PARTITIONED BY (ds string) LIFECYCLE 60;

INSERT OVERWRITE TABLE user_click_beh_feature_ads PARTITION (ds=${bizdate}) -- 用户点击行为特征
select user_id
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate2, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), ds, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate2, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), ds, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate2, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), ds, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate2, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), ds, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate2, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), ds, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate2, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), ds, null))
from dw_user_item_click_log
where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
group by user_id
;

create table if not exists user_collect_beh_feature_ads (
    user_id string,
    item_num_3d BIGINT ,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT
)PARTITIONED BY (ds string) LIFECYCLE 60;

INSERT OVERWRITE TABLE user_collect_beh_feature_ads PARTITION (ds=${bizdate})
select user_id
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate2, null))
from dw_user_item_collect_log
where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
group by user_id
;

create table if not exists user_cart_beh_feature_ads (
    user_id string,
    item_num_3d BIGINT ,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT

)PARTITIONED BY (ds string) LIFECYCLE 60;

INSERT OVERWRITE TABLE user_cart_beh_feature_ads PARTITION (ds=${bizdate}) -- 用户点击行为特征
select user_id
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate2, null))
from dw_user_item_cart_log
where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
group by user_id
;

create table if not exists user_alipay_beh_feature_ads (
    user_id string,
    item_num_3d BIGINT ,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT
)PARTITIONED BY (ds string) LIFECYCLE 60;

INSERT OVERWRITE TABLE user_alipay_beh_feature_ads PARTITION (ds=${bizdate}) -- 用户点击行为特征
select user_id
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), cate2, null))

    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), brand_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), seller_id, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate1, null))
    ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), cate2, null))
from dw_user_item_alipay_log
where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
group by user_id
;

