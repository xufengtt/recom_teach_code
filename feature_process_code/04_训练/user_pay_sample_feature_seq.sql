--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-05-13 23:25:44
--********************************************************************--
create table if not exists user_pay_sample_feature_seq (
    feature string ,
    number bigint
)LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_seq
select feature, ROW_NUMBER() OVER(ORDER BY feature) AS number
from (
    select DISTINCT feature
    from (
        select target_brand_id as feature
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',clk_brand_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',clt_brand_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',pay_brand_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',clk_cate_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',clt_cate_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',pay_cate_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_click_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_clt_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_cart_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_pay_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',brand_stat_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_cate2_cross_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_brand_cross_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
    )t1
)t1
;


create table if not exists user_pay_sample_feature_join_dnn_seq(
    user_id string,
    brand_id string,
    label bigint,
    bizdate string,
    target_brand_id string,
    clk_brand_seq string,
    clt_brand_seq string,
    pay_brand_seq string,
    clk_cate_seq string,
    clt_cate_seq string,
    pay_cate_seq string,
    user_click_feature string,
    user_clt_feature string,
    user_cart_feature string,
    user_pay_feature string,
    brand_stat_feature string,
    user_cate2_cross_feature string,
    user_brand_cross_feature string
)LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_join_dnn_seq
select t1.user_id, t1.brand_id, t1.label, t1.ds, t1.number,
    t2.feature, t3.feature, t5.feature, t6.feature, t7.feature, t8.feature
    ,t9.feature, t10.feature, t11.feature, t12.feature, t13.feature, t14.feature, t15.feature
from (
    select t1.user_id, t1.brand_id, t1.label, t1.ds, t2.number
    from (
        select user_id, brand_id, label, ds, target_brand_id
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
    )t1 join (
        select feature, number
        from user_pay_sample_feature_seq
    )t2 on t1.target_brand_id=t2.feature
)t1 join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, clk_brand_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t2 on t1.user_id=t2.user_id and t1.brand_id=t2.brand_id and t1.label=t2.label and t1.ds=t2.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, clt_brand_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t3 on t1.user_id=t3.user_id and t1.brand_id=t3.brand_id and t1.label=t3.label and t1.ds=t3.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, pay_brand_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t5 on t1.user_id=t5.user_id and t1.brand_id=t5.brand_id and t1.label=t5.label and t1.ds=t5.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, clk_cate_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t6 on t1.user_id=t6.user_id and t1.brand_id=t6.brand_id and t1.label=t6.label and t1.ds=t6.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, clt_cate_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t7 on t1.user_id=t7.user_id and t1.brand_id=t7.brand_id and t1.label=t7.label and t1.ds=t7.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, pay_cate_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t8 on t1.user_id=t8.user_id and t1.brand_id=t8.brand_id and t1.label=t8.label and t1.ds=t8.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_click_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t9 on t1.user_id=t9.user_id and t1.brand_id=t9.brand_id and t1.label=t9.label and t1.ds=t9.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_clt_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t10 on t1.user_id=t10.user_id and t1.brand_id=t10.brand_id and t1.label=t10.label and t1.ds=t10.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_cart_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t11 on t1.user_id=t11.user_id and t1.brand_id=t11.brand_id and t1.label=t11.label and t1.ds=t11.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_pay_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t12 on t1.user_id=t12.user_id and t1.brand_id=t12.brand_id and t1.label=t12.label and t1.ds=t12.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, brand_stat_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t13 on t1.user_id=t13.user_id and t1.brand_id=t13.brand_id and t1.label=t13.label and t1.ds=t13.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_cate2_cross_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t14 on t1.user_id=t14.user_id and t1.brand_id=t14.brand_id and t1.label=t14.label and t1.ds=t14.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_brand_cross_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t15 on t1.user_id=t15.user_id and t1.brand_id=t15.brand_id and t1.label=t15.label and t1.ds=t15.ds
;

create table if not exists user_pay_sample_feature_join_dnn_seq_shuffle(
    key_all string,
    label bigint,
    target_brand_id string,
    clk_brand_seq string,
    clt_brand_seq string,
    pay_brand_seq string,
    clk_cate_seq string,
    clt_cate_seq string,
    pay_cate_seq string,
    user_click_feature string,
    user_clt_feature string,
    user_cart_feature string,
    user_pay_feature string,
    brand_stat_feature string,
    user_cate2_cross_feature string,
    user_brand_cross_feature string
)LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_join_dnn_seq_shuffle
select key_all, max(label), MAX(target_brand_id), MAX(clk_brand_seq), MAX(clt_brand_seq), MAX(pay_brand_seq)
    ,max(clk_cate_seq), max(clt_cate_seq), max(pay_cate_seq), max(user_click_feature)
    ,max(user_clt_feature), max(user_cart_feature), max(user_pay_feature), max(brand_stat_feature)
    ,max(user_cate2_cross_feature), max(user_brand_cross_feature)
from (
    select *, concat(RAND(),'_',RAND(),'_',user_id,'_',brand_id,'_',label,'_',bizdate) as key_all
    from user_pay_sample_feature_join_dnn_seq
)t1 group by key_all
;

select count(*) from user_pay_sample_feature_join_dnn_seq_shuffle limit 100;
        select count(*)
        from user_pay_sample_feature_join
        where ds>='20130701' and ds<='20130916'