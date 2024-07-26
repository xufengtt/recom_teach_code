--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-20 11:42:01
--********************************************************************--
create table if not exists dw_user_item_click_log (
    user_id string comment '用户id'
    ,item_id string comment '商品id'
    ,brand_id string comment '品牌id'
    ,seller_id string comment '商家id'
    ,cate1 string COMMENT '类目1id'
    ,cate2 string COMMENT '类目2id'
    ,op_time string comment '点击时间'
)PARTITIONED BY (
    ds STRING COMMENT '日期分区'
)
LIFECYCLE 60
;

create table if not exists dw_user_item_cart_log (
    user_id string comment '用户id'
    ,item_id string comment '商品id'
    ,brand_id string comment '品牌id'
    ,seller_id string comment '商家id'
    ,cate1 string COMMENT '类目1id'
    ,cate2 string COMMENT '类目2id'
    ,op_time string comment '点击时间'
)PARTITIONED BY (
    ds STRING COMMENT '日期分区'
)
LIFECYCLE 60
;

create table if not exists dw_user_item_collect_log (
    user_id string comment '用户id'
    ,item_id string comment '商品id'
    ,brand_id string comment '品牌id'
    ,seller_id string comment '商家id'
    ,cate1 string COMMENT '类目1id'
    ,cate2 string COMMENT '类目2id'
    ,op_time string comment '点击时间'
)PARTITIONED BY (
    ds STRING COMMENT '日期分区'
)
LIFECYCLE 60
;

create table if not exists dw_user_item_alipay_log (
    user_id string comment '用户id'
    ,item_id string comment '商品id'
    ,brand_id string comment '品牌id'
    ,seller_id string comment '商家id'
    ,cate1 string COMMENT '类目1id'
    ,cate2 string COMMENT '类目2id'
    ,op_time string comment '点击时间'
)PARTITIONED BY (
    ds STRING COMMENT '日期分区'
)
LIFECYCLE 60
;

INSERT OVERWRITE TABLE dw_user_item_click_log PARTITION (ds)
select t1.user_id, t2.item_id, t2.brand_id, t2.seller_id, t2.cate1, t2.cate2, t1.vtime, t1.ds
from (
    select user_id,item_id, vtime, to_char(TO_DATE(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from user_item_beh_log
    where action='click'
)t1 join (
    select item_id, brand_id, seller_id, SPLIT_PART(category,'-',1) as cate1, SPLIT_PART(category,'-',2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id
;

INSERT OVERWRITE TABLE dw_user_item_collect_log PARTITION (ds)
select t1.user_id, t2.item_id, t2.brand_id, t2.seller_id, t2.cate1, t2.cate2, t1.vtime, t1.ds
from (
    select user_id,item_id, vtime, to_char(TO_DATE(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from user_item_beh_log
    where action='collect'
)t1 join (
    select item_id, brand_id, seller_id, SPLIT_PART(category,'-',1) as cate1, SPLIT_PART(category,'-',2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id
;

INSERT OVERWRITE TABLE dw_user_item_cart_log PARTITION (ds)
select t1.user_id, t2.item_id, t2.brand_id, t2.seller_id, t2.cate1, t2.cate2, t1.vtime, t1.ds
from (
    select user_id,item_id, vtime, to_char(TO_DATE(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from user_item_beh_log
    where action='cart'
)t1 join (
    select item_id, brand_id, seller_id, SPLIT_PART(category,'-',1) as cate1, SPLIT_PART(category,'-',2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id
;

INSERT OVERWRITE TABLE dw_user_item_alipay_log PARTITION (ds)
select t1.user_id, t2.item_id, t2.brand_id, t2.seller_id, t2.cate1, t2.cate2, t1.vtime, t1.ds
from (
    select user_id,item_id, vtime, to_char(TO_DATE(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from user_item_beh_log
    where action='alipay'
)t1 join (
    select item_id, brand_id, seller_id, SPLIT_PART(category,'-',1) as cate1, SPLIT_PART(category,'-',2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id
;
