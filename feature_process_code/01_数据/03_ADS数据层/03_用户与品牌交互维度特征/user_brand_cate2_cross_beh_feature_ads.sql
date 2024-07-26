--@exclude_input=dw_user_item_alipay_log
--@exclude_input=dw_user_item_cart_log
--@exclude_input=dw_user_item_collect_log
--@exclude_input=brand_cate2_dim
--@exclude_input=dw_user_item_click_log
--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-20 17:52:51
--********************************************************************--
create table if not exists user_brand_cate2_cross_beh_feature_ads (
    user_id string,
    brand_id string,

    clk_item_90d bigint,
    clk_item_60d bigint,
    clk_item_30d bigint,
    clk_item_15d bigint,
    clk_item_7d bigint,
    clk_item_3d bigint,
    clk_item_1d bigint,

    clk_num_90d bigint,
    clk_num_60d bigint,
    clk_num_30d bigint,
    clk_num_15d bigint,
    clk_num_7d bigint,
    clk_num_3d bigint,
    clk_num_1d bigint,

    clk_day_90d bigint,
    clk_day_60d bigint,
    clk_day_30d bigint,
    clk_day_15d bigint,
    clk_day_7d bigint,
    clk_day_3d bigint,
    clk_day_1d bigint,

    clt_item_90d bigint,
    clt_item_60d bigint,
    clt_item_30d bigint,
    clt_item_15d bigint,
    clt_item_7d bigint,
    clt_item_3d bigint,
    clt_item_1d bigint,

    cart_item_90d bigint,
    cart_item_60d bigint,
    cart_item_30d bigint,
    cart_item_15d bigint,
    cart_item_7d bigint,
    cart_item_3d bigint,
    cart_item_1d bigint,

    cart_num_90d bigint,
    cart_num_60d bigint,
    cart_num_30d bigint,
    cart_num_15d bigint,
    cart_num_7d bigint,
    cart_num_3d bigint,
    cart_num_1d bigint,

    pay_item_90d bigint,
    pay_item_60d bigint,
    pay_item_30d bigint,
    pay_item_15d bigint,
    pay_item_7d bigint,
    pay_item_3d bigint,
    pay_item_1d bigint,

    pay_num_90d bigint,
    pay_num_60d bigint,
    pay_num_30d bigint,
    pay_num_15d bigint,
    pay_num_7d bigint,
    pay_num_3d bigint,
    pay_num_1d bigint

)PARTITIONED BY (ds STRING) LIFECYCLE 60;

set odps.sql.mapper.split.size=16;
insert overwrite table user_brand_cate2_cross_beh_feature_ads partition (ds=${bizdate})
select t1.user_id, t1.brand_id,
    clk_item_90d,
    clk_item_60d,
    clk_item_30d,
    clk_item_15d,
    clk_item_7d,
    clk_item_3d,
    clk_item_1d,

    clk_num_90d,
    clk_num_60d,
    clk_num_30d,
    clk_num_15d,
    clk_num_7d,
    clk_num_3d,
    clk_num_1d,

    clk_day_90d,
    clk_day_60d,
    clk_day_30d,
    clk_day_15d,
    clk_day_7d,
    clk_day_3d,
    clk_day_1d,

    if(clt_item_90d is null, 0, clt_item_90d) as clk_item_90d,
    if(clt_item_60d is null, 0, clt_item_60d) as clt_item_60d,
    if(clt_item_30d is null, 0, clt_item_30d) as clt_item_30d,
    if(clt_item_15d is null, 0, clt_item_15d) as clt_item_15d,
    if(clt_item_7d is null, 0, clt_item_7d) as clt_item_7d,
    if(clt_item_3d is null, 0, clt_item_3d) as clt_item_3d,
    if(clt_item_1d is null, 0, clt_item_1d) as clt_item_1d,

    if(cart_item_90d is null, 0, cart_item_90d) as cart_item_90d,
    if(cart_item_60d is null, 0, cart_item_60d) as cart_item_60d,
    if(cart_item_30d is null, 0, cart_item_30d) as cart_item_30d,
    if(cart_item_15d is null, 0, cart_item_15d) as cart_item_15d,
    if(cart_item_7d is null, 0, cart_item_7d) as cart_item_7d,
    if(cart_item_3d is null, 0, cart_item_3d) as cart_item_3d,
    if(cart_item_1d is null, 0, cart_item_1d) as cart_item_1d,

    if(cart_num_90d is null, 0, cart_num_90d) as cart_num_90d,
    if(cart_num_60d is null, 0, cart_num_60d) as cart_num_60d,
    if(cart_num_30d is null, 0, cart_num_30d) as cart_num_30d,
    if(cart_num_15d is null, 0, cart_num_15d) as cart_num_15d,
    if(cart_num_7d is null, 0, cart_num_7d) as cart_num_7d,
    if(cart_num_3d is null, 0, cart_num_3d) as cart_num_3d,
    if(cart_num_1d is null, 0, cart_num_1d) as cart_num_1d,

    if(pay_item_90d is null, 0, pay_item_90d) as pay_item_90d,
    if(pay_item_60d is null, 0, pay_item_60d) as pay_item_60d,
    if(pay_item_30d is null, 0, pay_item_30d) as pay_item_30d,
    if(pay_item_15d is null, 0, pay_item_15d) as pay_item_15d,
    if(pay_item_7d is null, 0, pay_item_7d) as pay_item_7d,
    if(pay_item_3d is null, 0, pay_item_3d) as pay_item_3d,
    if(pay_item_1d is null, 0, pay_item_1d) as pay_item_1d,

    if(pay_num_90d is null, 0, pay_num_90d) as pay_num_90d,
    if(pay_num_60d is null, 0, pay_num_60d) as pay_num_60d,
    if(pay_num_30d is null, 0, pay_num_30d) as pay_num_30d,
    if(pay_num_15d is null, 0, pay_num_15d) as pay_num_15d,
    if(pay_num_7d is null, 0, pay_num_7d) as pay_num_7d,
    if(pay_num_3d is null, 0, pay_num_3d) as pay_num_3d,
    if(pay_num_1d is null, 0, pay_num_1d) as pay_num_1d

from (
    select user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as clk_item_1d

        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_90d
        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_60d
        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_30d
        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_15d
        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_7d
        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_3d
        ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as clk_num_1d

        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), ds, null)) as clk_day_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), ds, null)) as clk_day_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), ds, null)) as clk_day_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), ds, null)) as clk_day_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), ds, null)) as clk_day_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), ds, null)) as clk_day_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), ds, null)) as clk_day_1d
    from (
        select /*+mapjoin(t2)*/
            t1.user_id, t1.ds, t2.brand_id, item_id
        from (
            select cate2, user_id, ds, item_id
            from dw_user_item_click_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        )t1 join (
            select brand_id, cate2
            from brand_cate2_dim
        )t2 on t1.cate2=t2.cate2
    )t1
    group by user_id, brand_id
)t1 left join (
    select user_id, brand_id
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_90d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_60d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_30d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_15d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_7d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_3d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as clt_item_1d
    from (
        select /*+mapjoin(t2)*/
            t1.user_id, t1.ds, t2.brand_id, item_id
        from (
            select cate2, user_id, ds, item_id
            from dw_user_item_collect_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        )t1 join (
            select brand_id, cate2
            from brand_cate2_dim
        )t2 on t1.cate2=t2.cate2
    )t1
    group by user_id, brand_id
)t2 on t1.user_id=t2.user_id and t1.brand_id=t2.brand_id
left join (
    select user_id, brand_id
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_90d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_60d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_30d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_15d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_7d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_3d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as cart_num_1d

         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_90d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_60d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_30d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_15d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_7d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_3d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_1d
    from (
        select /*+mapjoin(t2)*/
            t1.user_id, t1.ds, t2.brand_id, item_id
        from (
            select  user_id, ds, cate2, item_id
            from dw_user_item_cart_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        )t1 join (
            select brand_id, cate2
            from brand_cate2_dim
        )t2 on t1.cate2=t2.cate2
    )t1
    group by user_id, brand_id
)t3 on t1.user_id=t3.user_id and t1.brand_id=t3.brand_id
left join (
    select user_id, brand_id
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_90d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_60d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_30d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_15d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_7d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_3d
         ,count(if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as pay_num_1d

         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_90d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_60d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_30d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_15d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_7d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_3d
         ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as pay_item_1d
    from (
        select /*+mapjoin(t2)*/
            t1.user_id, t1.ds, t2.brand_id, item_id
        from (
            select user_id, cate2, ds, item_id
            from dw_user_item_alipay_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        )t1 join (
            select brand_id, cate2
            from brand_cate2_dim
        )t2 on t1.cate2=t2.cate2
    )t1
    group by user_id, brand_id
)t4 on t1.user_id=t4.user_id and t1.brand_id=t4.brand_id