--@exclude_input=user_id_number
--@exclude_input=dw_user_item_alipay_log
--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-22 21:43:38
--********************************************************************--
create table if not exists user_pay_sample_eval (
    user_id string,
    brand_id string,
    label bigint
)PARTITIONED BY (ds STRING) LIFECYCLE 60;

insert OVERWRITE TABLE user_pay_sample_eval partition (ds=${bizdate})
select user_id, brand_id, max(label) as label
from (
    select /*+mapjoin(t2)*/
        t1.user_id, t2.brand_id, 0 as label
    from (
        select user_id
        from user_id_number
        where number<=3000000
    )t1 join (
        --b47686 韩都衣舍
        --b56508 三星手机
        --b62063 诺基亚
        --b78739 LILY
        select 'b47686' as brand_id
        union all
        select 'b56508' as brand_id
        union all
        select 'b62063' as brand_id
        union all
        select 'b78739' as brand_id

    )t2
    union all
    select user_id, brand_id, 1 as label
    from dw_user_item_alipay_log
    where ds > '${bizdate}' and ds <= to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),7, 'dd'),'yyyymmdd') and brand_id in ('b47686','b56508','b62063','b78739')
    group by user_id, brand_id
)t1 group by user_id, brand_id