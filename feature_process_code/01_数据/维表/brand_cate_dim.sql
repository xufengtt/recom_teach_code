--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-20 17:06:23
--********************************************************************--
create table if not exists brand_top500_alipay_dim (
    brand_id string,
    alipay_num bigint
)LIFECYCLE 60;

insert OVERwrite table brand_top500_alipay_dim
select brand_id, alipay_num
from (
    select brand_id, count(DISTINCT user_id) as alipay_num
    from dw_user_item_alipay_log
    where ds<=${bizdate} and ds>to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
        and brand_id is not null
    group by brand_id
)t1 order by alipay_num desc limit 500
;

create table if not exists brand_top1w_alipay_dim (
    brand_id string,
    alipay_num bigint
)LIFECYCLE 60;

insert OVERwrite table brand_top1w_alipay_dim
select brand_id, alipay_num
from (
    select brand_id, count(DISTINCT user_id) as alipay_num
    from dw_user_item_alipay_log
    where ds<=${bizdate} and ds>to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
        and brand_id is not null
    group by brand_id
)t1 order by alipay_num desc limit 10000
;

create table if not exists brand_top20_item_dim LIFECYCLE 60 as
select brand_id, alipay_num, WM_CONCAT(';', title) as title
from (
    select brand_id,item_id,title,num, alipay_num, ROW_NUMBER() OVER(PARTITION BY brand_id ORDER BY num desc) AS number
    from (
        select t2.brand_id, t2.item_id, t2.title, t1.num, t2.alipay_num
        from (
            select item_id, count(distinct user_id) as num
            from dw_user_item_click_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            group by item_id
        )t1 join (
            select t1.brand_id, t2.item_id, t2.title, t1.alipay_num
            from (
                select brand_id, alipay_num
                from brand_top500_alipay_dim
            )t1 join (
                select item_id, category, brand_id, seller_id, title
                from item_dim
            )t2 on t1.brand_id=t2.brand_id
        )t2 on t1.item_id=t2.item_id
    )t1
)t1 where number<=20
group by brand_id, alipay_num
;

create table if not exists brand_cate1_dim (
    brand_id string,
    cate1 string
)lifecycle 60;

insert overwrite table brand_cate1_dim
select brand_id, cate1
from (
    select brand_id, cate1, ROW_NUMBER() OVER(PARTITION BY brand_id ORDER BY num desc) AS number
    from (
        select t1.brand_id, t1.cate1, count(distinct user_id) as num
        from (
            select user_id, ds, brand_id, cate1
            from dw_user_item_alipay_log
            where ds<${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
        )t1 join (
            select brand_id
            from brand_top500_alipay_dim
        )t2 on t1.brand_id=t2.brand_id
        group by t1.cate1, t1.brand_id
    )t1
)t1 where number <=2
;

create table if not exists brand_cate2_dim (
    brand_id string,
    cate2 string
)lifecycle 60;

insert overwrite table brand_cate2_dim
select brand_id, cate2
from (
    select brand_id, cate2, ROW_NUMBER() OVER(PARTITION BY brand_id ORDER BY num desc) AS number
    from (
        select t1.brand_id, t1.cate2, count(distinct user_id) as num
        from (
            select user_id, ds, brand_id, cate2
            from dw_user_item_alipay_log
            where ds<${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
        )t1 join (
            select brand_id
            from brand_top500_alipay_dim
        )t2 on t1.brand_id=t2.brand_id
        group by t1.cate2, t1.brand_id
    )t1
)t1 where number <=2
;
