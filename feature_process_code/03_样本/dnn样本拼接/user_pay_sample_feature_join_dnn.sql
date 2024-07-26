--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-05-05 21:42:04
--********************************************************************--
create table if not exists user_pay_sample_feature_join_dnn(
    user_id string,
    brand_id string,
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
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_join_dnn partition (ds=${bizdate})
select t1.user_id, t1.brand_id, t1.label, concat('b_',t1.brand_id)
    ,if(t2.brand_id_seq is null, 'clkb_seq_null', t2.brand_id_seq)
    ,if(t3.brand_id_seq is null, 'cltb_seq_null', t3.brand_id_seq)
    ,if(t4.brand_id_seq is null, 'payb_seq_null', t4.brand_id_seq)
    ,if(t5.cate_seq is null, 'clkc_seq_null', t5.cate_seq)
    ,if(t6.cate_seq is null, 'cltc_seq_null', t6.cate_seq)
    ,if(t7.cate_seq is null, 'payc_seq_null', t7.cate_seq)
    ,if(t8.user_id is null, 'user_click_null', concat(
        concat('uclk_item_num_3d','_',if(t8.item_num_3d is null, 'null', cast(log(2,t8.item_num_3d+1) as bigint))),',',
        concat('uclk_brand_num_3d','_',if(t8.brand_num_3d is null, 'null', cast(log(2,t8.brand_num_3d+1) as bigint))),',',
        concat('uclk_seller_num_3d','_',if(t8.seller_num_3d is null, 'null', cast(log(2,t8.seller_num_3d+1) as bigint))),',',
        concat('uclk_cate1_num_3d','_',if(t8.cate1_num_3d is null, 'null', cast(log(2,t8.cate1_num_3d+1) as bigint))),',',
        concat('uclk_cate2_num_3d','_',if(t8.cate2_num_3d is null, 'null', cast(log(2,t8.cate2_num_3d+1) as bigint))),',',
        concat('uclk_cnt_days_3d','_',if(t8.cnt_days_3d is null, 'null', cast(log(2,t8.cnt_days_3d+1) as bigint))),',',
        concat('uclk_item_num_7d','_',if(t8.item_num_7d is null, 'null', cast(log(2,t8.item_num_7d+1) as bigint))),',',
        concat('uclk_brand_num_7d','_',if(t8.brand_num_7d is null, 'null', cast(log(2,t8.brand_num_7d+1) as bigint))),',',
        concat('uclk_seller_num_7d','_',if(t8.seller_num_7d is null, 'null', cast(log(2,t8.seller_num_7d+1) as bigint))),',',
        concat('uclk_cate1_num_7d','_',if(t8.cate1_num_7d is null, 'null', cast(log(2,t8.cate1_num_7d+1) as bigint))),',',
        concat('uclk_cate2_num_7d','_',if(t8.cate2_num_7d is null, 'null', cast(log(2,t8.cate2_num_7d+1) as bigint))),',',
        concat('uclk_cnt_days_7d','_',if(t8.cnt_days_7d is null, 'null', cast(log(2,t8.cnt_days_7d+1) as bigint))),',',
        concat('uclk_item_num_15d','_',if(t8.item_num_15d is null, 'null', cast(log(2,t8.item_num_15d+1) as bigint))),',',
        concat('uclk_brand_num_15d','_',if(t8.brand_num_15d is null, 'null', cast(log(2,t8.brand_num_15d+1) as bigint))),',',
        concat('uclk_seller_num_15d','_',if(t8.seller_num_15d is null, 'null', cast(log(2,t8.seller_num_15d+1) as bigint))),',',
        concat('uclk_cate1_num_15d','_',if(t8.cate1_num_15d is null, 'null', cast(log(2,t8.cate1_num_15d+1) as bigint))),',',
        concat('uclk_cate2_num_15d','_',if(t8.cate2_num_15d is null, 'null', cast(log(2,t8.cate2_num_15d+1) as bigint))),',',
        concat('uclk_cnt_days_15d','_',if(t8.cnt_days_15d is null, 'null', cast(log(2,t8.cnt_days_15d+1) as bigint))),',',
        concat('uclk_item_num_30d','_',if(t8.item_num_30d is null, 'null', cast(log(2,t8.item_num_30d+1) as bigint))),',',
        concat('uclk_brand_num_30d','_',if(t8.brand_num_30d is null, 'null', cast(log(2,t8.brand_num_30d+1) as bigint))),',',
        concat('uclk_seller_num_30d','_',if(t8.seller_num_30d is null, 'null', cast(log(2,t8.seller_num_30d+1) as bigint))),',',
        concat('uclk_cate1_num_30d','_',if(t8.cate1_num_30d is null, 'null', cast(log(2,t8.cate1_num_30d+1) as bigint))),',',
        concat('uclk_cate2_num_30d','_',if(t8.cate2_num_30d is null, 'null', cast(log(2,t8.cate2_num_30d+1) as bigint))),',',
        concat('uclk_cnt_days_30d','_',if(t8.cnt_days_30d is null, 'null', cast(log(2,t8.cnt_days_30d+1) as bigint))),',',
        concat('uclk_item_num_60d','_',if(t8.item_num_60d is null, 'null', cast(log(2,t8.item_num_60d+1) as bigint))),',',
        concat('uclk_brand_num_60d','_',if(t8.brand_num_60d is null, 'null', cast(log(2,t8.brand_num_60d+1) as bigint))),',',
        concat('uclk_seller_num_60d','_',if(t8.seller_num_60d is null, 'null', cast(log(2,t8.seller_num_60d+1) as bigint))),',',
        concat('uclk_cate1_num_60d','_',if(t8.cate1_num_60d is null, 'null', cast(log(2,t8.cate1_num_60d+1) as bigint))),',',
        concat('uclk_cate2_num_60d','_',if(t8.cate2_num_60d is null, 'null', cast(log(2,t8.cate2_num_60d+1) as bigint))),',',
        concat('uclk_cnt_days_60d','_',if(t8.cnt_days_60d is null, 'null', cast(log(2,t8.cnt_days_60d+1) as bigint))),',',
        concat('uclk_item_num_90d','_',if(t8.item_num_90d is null, 'null', cast(log(2,t8.item_num_90d+1) as bigint))),',',
        concat('uclk_brand_num_90d','_',if(t8.brand_num_90d is null, 'null', cast(log(2,t8.brand_num_90d+1) as bigint))),',',
        concat('uclk_seller_num_90d','_',if(t8.seller_num_90d is null, 'null', cast(log(2,t8.seller_num_90d+1) as bigint))),',',
        concat('uclk_cate1_num_90d','_',if(t8.cate1_num_90d is null, 'null', cast(log(2,t8.cate1_num_90d+1) as bigint))),',',
        concat('uclk_cate2_num_90d','_',if(t8.cate2_num_90d is null, 'null', cast(log(2,t8.cate2_num_90d+1) as bigint))),',',
        concat('uclk_cnt_days_90d','_',if(t8.cnt_days_90d is null, 'null', cast(log(2,t8.cnt_days_90d+1) as bigint)))
    )) as user_click_beh_feature
    ,if(t9.user_id is null, 'user_clt_null',concat(
        concat('uclt_item_num_3d','_',if(t9.item_num_3d is null, 'null', cast(log(2,t9.item_num_3d+1) as bigint))),',',
        concat('uclt_brand_num_3d','_',if(t9.brand_num_3d is null, 'null', cast(log(2,t9.brand_num_3d+1) as bigint))),',',
        concat('uclt_seller_num_3d','_',if(t9.seller_num_3d is null, 'null', cast(log(2,t9.seller_num_3d+1) as bigint))),',',
        concat('uclt_cate1_num_3d','_',if(t9.cate1_num_3d is null, 'null', cast(log(2,t9.cate1_num_3d+1) as bigint))),',',
        concat('uclt_cate2_num_3d','_',if(t9.cate2_num_3d is null, 'null', cast(log(2,t9.cate2_num_3d+1) as bigint))),',',
        concat('uclt_item_num_7d','_',if(t9.item_num_7d is null, 'null', cast(log(2,t9.item_num_7d+1) as bigint))),',',
        concat('uclt_brand_num_7d','_',if(t9.brand_num_7d is null, 'null', cast(log(2,t9.brand_num_7d+1) as bigint))),',',
        concat('uclt_seller_num_7d','_',if(t9.seller_num_7d is null, 'null', cast(log(2,t9.seller_num_7d+1) as bigint))),',',
        concat('uclt_cate1_num_7d','_',if(t9.cate1_num_7d is null, 'null', cast(log(2,t9.cate1_num_7d+1) as bigint))),',',
        concat('uclt_cate2_num_7d','_',if(t9.cate2_num_7d is null, 'null', cast(log(2,t9.cate2_num_7d+1) as bigint))),',',
        concat('uclt_item_num_15d','_',if(t9.item_num_15d is null, 'null', cast(log(2,t9.item_num_15d+1) as bigint))),',',
        concat('uclt_brand_num_15d','_',if(t9.brand_num_15d is null, 'null', cast(log(2,t9.brand_num_15d+1) as bigint))),',',
        concat('uclt_seller_num_15d','_',if(t9.seller_num_15d is null, 'null', cast(log(2,t9.seller_num_15d+1) as bigint))),',',
        concat('uclt_cate1_num_15d','_',if(t9.cate1_num_15d is null, 'null', cast(log(2,t9.cate1_num_15d+1) as bigint))),',',
        concat('uclt_cate2_num_15d','_',if(t9.cate2_num_15d is null, 'null', cast(log(2,t9.cate2_num_15d+1) as bigint))),',',
        concat('uclt_item_num_30d','_',if(t9.item_num_30d is null, 'null', cast(log(2,t9.item_num_30d+1) as bigint))),',',
        concat('uclt_brand_num_30d','_',if(t9.brand_num_30d is null, 'null', cast(log(2,t9.brand_num_30d+1) as bigint))),',',
        concat('uclt_seller_num_30d','_',if(t9.seller_num_30d is null, 'null', cast(log(2,t9.seller_num_30d+1) as bigint))),',',
        concat('uclt_cate1_num_30d','_',if(t9.cate1_num_30d is null, 'null', cast(log(2,t9.cate1_num_30d+1) as bigint))),',',
        concat('uclt_cate2_num_30d','_',if(t9.cate2_num_30d is null, 'null', cast(log(2,t9.cate2_num_30d+1) as bigint))),',',
        concat('uclt_item_num_60d','_',if(t9.item_num_60d is null, 'null', cast(log(2,t9.item_num_60d+1) as bigint))),',',
        concat('uclt_brand_num_60d','_',if(t9.brand_num_60d is null, 'null', cast(log(2,t9.brand_num_60d+1) as bigint))),',',
        concat('uclt_seller_num_60d','_',if(t9.seller_num_60d is null, 'null', cast(log(2,t9.seller_num_60d+1) as bigint))),',',
        concat('uclt_cate1_num_60d','_',if(t9.cate1_num_60d is null, 'null', cast(log(2,t9.cate1_num_60d+1) as bigint))),',',
        concat('uclt_cate2_num_60d','_',if(t9.cate2_num_60d is null, 'null', cast(log(2,t9.cate2_num_60d+1) as bigint))),',',
        concat('uclt_item_num_90d','_',if(t9.item_num_90d is null, 'null', cast(log(2,t9.item_num_90d+1) as bigint))),',',
        concat('uclt_brand_num_90d','_',if(t9.brand_num_90d is null, 'null', cast(log(2,t9.brand_num_90d+1) as bigint))),',',
        concat('uclt_seller_num_90d','_',if(t9.seller_num_90d is null, 'null', cast(log(2,t9.seller_num_90d+1) as bigint))),',',
        concat('uclt_cate1_num_90d','_',if(t9.cate1_num_90d is null, 'null', cast(log(2,t9.cate1_num_90d+1) as bigint))),',',
        concat('uclt_cate2_num_90d','_',if(t9.cate2_num_90d is null, 'null', cast(log(2,t9.cate2_num_90d+1) as bigint)))
    )) as user_clt_beh_feature
    ,if(t10.user_id is null, 'user_cart_null',concat(
        concat('ucart_item_num_3d','_',if(t10.item_num_3d is null, 'null', cast(log(2,t10.item_num_3d+1) as bigint))),',',
        concat('ucart_brand_num_3d','_',if(t10.brand_num_3d is null, 'null', cast(log(2,t10.brand_num_3d+1) as bigint))),',',
        concat('ucart_seller_num_3d','_',if(t10.seller_num_3d is null, 'null', cast(log(2,t10.seller_num_3d+1) as bigint))),',',
        concat('ucart_cate1_num_3d','_',if(t10.cate1_num_3d is null, 'null', cast(log(2,t10.cate1_num_3d+1) as bigint))),',',
        concat('ucart_cate2_num_3d','_',if(t10.cate2_num_3d is null, 'null', cast(log(2,t10.cate2_num_3d+1) as bigint))),',',
        concat('ucart_item_num_7d','_',if(t10.item_num_7d is null, 'null', cast(log(2,t10.item_num_7d+1) as bigint))),',',
        concat('ucart_brand_num_7d','_',if(t10.brand_num_7d is null, 'null', cast(log(2,t10.brand_num_7d+1) as bigint))),',',
        concat('ucart_seller_num_7d','_',if(t10.seller_num_7d is null, 'null', cast(log(2,t10.seller_num_7d+1) as bigint))),',',
        concat('ucart_cate1_num_7d','_',if(t10.cate1_num_7d is null, 'null', cast(log(2,t10.cate1_num_7d+1) as bigint))),',',
        concat('ucart_cate2_num_7d','_',if(t10.cate2_num_7d is null, 'null', cast(log(2,t10.cate2_num_7d+1) as bigint))),',',
        concat('ucart_item_num_15d','_',if(t10.item_num_15d is null, 'null', cast(log(2,t10.item_num_15d+1) as bigint))),',',
        concat('ucart_brand_num_15d','_',if(t10.brand_num_15d is null, 'null', cast(log(2,t10.brand_num_15d+1) as bigint))),',',
        concat('ucart_seller_num_15d','_',if(t10.seller_num_15d is null, 'null', cast(log(2,t10.seller_num_15d+1) as bigint))),',',
        concat('ucart_cate1_num_15d','_',if(t10.cate1_num_15d is null, 'null', cast(log(2,t10.cate1_num_15d+1) as bigint))),',',
        concat('ucart_cate2_num_15d','_',if(t10.cate2_num_15d is null, 'null', cast(log(2,t10.cate2_num_15d+1) as bigint))),',',
        concat('ucart_item_num_30d','_',if(t10.item_num_30d is null, 'null', cast(log(2,t10.item_num_30d+1) as bigint))),',',
        concat('ucart_brand_num_30d','_',if(t10.brand_num_30d is null, 'null', cast(log(2,t10.brand_num_30d+1) as bigint))),',',
        concat('ucart_seller_num_30d','_',if(t10.seller_num_30d is null, 'null', cast(log(2,t10.seller_num_30d+1) as bigint))),',',
        concat('ucart_cate1_num_30d','_',if(t10.cate1_num_30d is null, 'null', cast(log(2,t10.cate1_num_30d+1) as bigint))),',',
        concat('ucart_cate2_num_30d','_',if(t10.cate2_num_30d is null, 'null', cast(log(2,t10.cate2_num_30d+1) as bigint))),',',
        concat('ucart_item_num_60d','_',if(t10.item_num_60d is null, 'null', cast(log(2,t10.item_num_60d+1) as bigint))),',',
        concat('ucart_brand_num_60d','_',if(t10.brand_num_60d is null, 'null', cast(log(2,t10.brand_num_60d+1) as bigint))),',',
        concat('ucart_seller_num_60d','_',if(t10.seller_num_60d is null, 'null', cast(log(2,t10.seller_num_60d+1) as bigint))),',',
        concat('ucart_cate1_num_60d','_',if(t10.cate1_num_60d is null, 'null', cast(log(2,t10.cate1_num_60d+1) as bigint))),',',
        concat('ucart_cate2_num_60d','_',if(t10.cate2_num_60d is null, 'null', cast(log(2,t10.cate2_num_60d+1) as bigint))),',',
        concat('ucart_item_num_90d','_',if(t10.item_num_90d is null, 'null', cast(log(2,t10.item_num_90d+1) as bigint))),',',
        concat('ucart_brand_num_90d','_',if(t10.brand_num_90d is null, 'null', cast(log(2,t10.brand_num_90d+1) as bigint))),',',
        concat('ucart_seller_num_90d','_',if(t10.seller_num_90d is null, 'null', cast(log(2,t10.seller_num_90d+1) as bigint))),',',
        concat('ucart_cate1_num_90d','_',if(t10.cate1_num_90d is null, 'null', cast(log(2,t10.cate1_num_90d+1) as bigint))),',',
        concat('ucart_cate2_num_90d','_',if(t10.cate2_num_90d is null, 'null', cast(log(2,t10.cate2_num_90d+1) as bigint)))
    )) as user_cart_beh_feature
    ,if(t11.user_id is null, 'user_pay_null',concat(
        concat('upay_item_num_3d','_',if(t11.item_num_3d is null, 'null', cast(log(2,t11.item_num_3d+1) as bigint))),',',
        concat('upay_brand_num_3d','_',if(t11.brand_num_3d is null, 'null', cast(log(2,t11.brand_num_3d+1) as bigint))),',',
        concat('upay_seller_num_3d','_',if(t11.seller_num_3d is null, 'null', cast(log(2,t11.seller_num_3d+1) as bigint))),',',
        concat('upay_cate1_num_3d','_',if(t11.cate1_num_3d is null, 'null', cast(log(2,t11.cate1_num_3d+1) as bigint))),',',
        concat('upay_cate2_num_3d','_',if(t11.cate2_num_3d is null, 'null', cast(log(2,t11.cate2_num_3d+1) as bigint))),',',
        concat('upay_item_num_7d','_',if(t11.item_num_7d is null, 'null', cast(log(2,t11.item_num_7d+1) as bigint))),',',
        concat('upay_brand_num_7d','_',if(t11.brand_num_7d is null, 'null', cast(log(2,t11.brand_num_7d+1) as bigint))),',',
        concat('upay_seller_num_7d','_',if(t11.seller_num_7d is null, 'null', cast(log(2,t11.seller_num_7d+1) as bigint))),',',
        concat('upay_cate1_num_7d','_',if(t11.cate1_num_7d is null, 'null', cast(log(2,t11.cate1_num_7d+1) as bigint))),',',
        concat('upay_cate2_num_7d','_',if(t11.cate2_num_7d is null, 'null', cast(log(2,t11.cate2_num_7d+1) as bigint))),',',
        concat('upay_item_num_15d','_',if(t11.item_num_15d is null, 'null', cast(log(2,t11.item_num_15d+1) as bigint))),',',
        concat('upay_brand_num_15d','_',if(t11.brand_num_15d is null, 'null', cast(log(2,t11.brand_num_15d+1) as bigint))),',',
        concat('upay_seller_num_15d','_',if(t11.seller_num_15d is null, 'null', cast(log(2,t11.seller_num_15d+1) as bigint))),',',
        concat('upay_cate1_num_15d','_',if(t11.cate1_num_15d is null, 'null', cast(log(2,t11.cate1_num_15d+1) as bigint))),',',
        concat('upay_cate2_num_15d','_',if(t11.cate2_num_15d is null, 'null', cast(log(2,t11.cate2_num_15d+1) as bigint))),',',
        concat('upay_item_num_30d','_',if(t11.item_num_30d is null, 'null', cast(log(2,t11.item_num_30d+1) as bigint))),',',
        concat('upay_brand_num_30d','_',if(t11.brand_num_30d is null, 'null', cast(log(2,t11.brand_num_30d+1) as bigint))),',',
        concat('upay_seller_num_30d','_',if(t11.seller_num_30d is null, 'null', cast(log(2,t11.seller_num_30d+1) as bigint))),',',
        concat('upay_cate1_num_30d','_',if(t11.cate1_num_30d is null, 'null', cast(log(2,t11.cate1_num_30d+1) as bigint))),',',
        concat('upay_cate2_num_30d','_',if(t11.cate2_num_30d is null, 'null', cast(log(2,t11.cate2_num_30d+1) as bigint))),',',
        concat('upay_item_num_60d','_',if(t11.item_num_60d is null, 'null', cast(log(2,t11.item_num_60d+1) as bigint))),',',
        concat('upay_brand_num_60d','_',if(t11.brand_num_60d is null, 'null', cast(log(2,t11.brand_num_60d+1) as bigint))),',',
        concat('upay_seller_num_60d','_',if(t11.seller_num_60d is null, 'null', cast(log(2,t11.seller_num_60d+1) as bigint))),',',
        concat('upay_cate1_num_60d','_',if(t11.cate1_num_60d is null, 'null', cast(log(2,t11.cate1_num_60d+1) as bigint))),',',
        concat('upay_cate2_num_60d','_',if(t11.cate2_num_60d is null, 'null', cast(log(2,t11.cate2_num_60d+1) as bigint))),',',
        concat('upay_item_num_90d','_',if(t11.item_num_90d is null, 'null', cast(log(2,t11.item_num_90d+1) as bigint))),',',
        concat('upay_brand_num_90d','_',if(t11.brand_num_90d is null, 'null', cast(log(2,t11.brand_num_90d+1) as bigint))),',',
        concat('upay_seller_num_90d','_',if(t11.seller_num_90d is null, 'null', cast(log(2,t11.seller_num_90d+1) as bigint))),',',
        concat('upay_cate1_num_90d','_',if(t11.cate1_num_90d is null, 'null', cast(log(2,t11.cate1_num_90d+1) as bigint))),',',
        concat('upay_cate2_num_90d','_',if(t11.cate2_num_90d is null, 'null', cast(log(2,t11.cate2_num_90d+1) as bigint)))
            )) as user_pay_beh_feature
    ,if(t12.brand_id is null, 'brand_stat_null',concat(
        concat('b_click_num','_',if(t12.click_num is null, 'null', cast(log(2,t12.click_num+1) as bigint))),',',
        concat('b_collect_num','_',if(t12.collect_num is null, 'null', cast(log(2,t12.collect_num+1) as bigint))),',',
        concat('b_cart_num','_',if(t12.cart_num is null, 'null', cast(log(2,t12.cart_num+1) as bigint))),',',
        concat('b_alipay_num','_',if(t12.alipay_num is null, 'null', cast(log(2,t12.alipay_num+1) as bigint)))
    )) as brand_stat_feature
    ,if(t13.user_id is null, 'uc2_null',concat(
        concat('uc2_clk_item_90d','_',if(t13.clk_item_90d is null, 'null', cast(log(2,t13.clk_item_90d+1) as bigint))),',',
        concat('uc2_clk_item_60d','_',if(t13.clk_item_60d is null, 'null', cast(log(2,t13.clk_item_60d+1) as bigint))),',',
        concat('uc2_clk_item_30d','_',if(t13.clk_item_30d is null, 'null', cast(log(2,t13.clk_item_30d+1) as bigint))),',',
        concat('uc2_clk_item_15d','_',if(t13.clk_item_15d is null, 'null', cast(log(2,t13.clk_item_15d+1) as bigint))),',',
        concat('uc2_clk_item_7d','_',if(t13.clk_item_7d is null, 'null', cast(log(2,t13.clk_item_7d+1) as bigint))),',',
        concat('uc2_clk_item_3d','_',if(t13.clk_item_3d is null, 'null', cast(log(2,t13.clk_item_3d+1) as bigint))),',',
        concat('uc2_clk_item_1d','_',if(t13.clk_item_1d is null, 'null', cast(log(2,t13.clk_item_1d+1) as bigint))),',',
        concat('uc2_clk_num_90d','_',if(t13.clk_num_90d is null, 'null', cast(log(2,t13.clk_num_90d+1) as bigint))),',',
        concat('uc2_clk_num_60d','_',if(t13.clk_num_60d is null, 'null', cast(log(2,t13.clk_num_60d+1) as bigint))),',',
        concat('uc2_clk_num_30d','_',if(t13.clk_num_30d is null, 'null', cast(log(2,t13.clk_num_30d+1) as bigint))),',',
        concat('uc2_clk_num_15d','_',if(t13.clk_num_15d is null, 'null', cast(log(2,t13.clk_num_15d+1) as bigint))),',',
        concat('uc2_clk_num_7d','_',if(t13.clk_num_7d is null, 'null', cast(log(2,t13.clk_num_7d+1) as bigint))),',',
        concat('uc2_clk_num_3d','_',if(t13.clk_num_3d is null, 'null', cast(log(2,t13.clk_num_3d+1) as bigint))),',',
        concat('uc2_clk_num_1d','_',if(t13.clk_num_1d is null, 'null', cast(log(2,t13.clk_num_1d+1) as bigint))),',',
        concat('uc2_clk_day_90d','_',if(t13.clk_day_90d is null, 'null', cast(log(2,t13.clk_day_90d+1) as bigint))),',',
        concat('uc2_clk_day_60d','_',if(t13.clk_day_60d is null, 'null', cast(log(2,t13.clk_day_60d+1) as bigint))),',',
        concat('uc2_clk_day_30d','_',if(t13.clk_day_30d is null, 'null', cast(log(2,t13.clk_day_30d+1) as bigint))),',',
        concat('uc2_clk_day_15d','_',if(t13.clk_day_15d is null, 'null', cast(log(2,t13.clk_day_15d+1) as bigint))),',',
        concat('uc2_clk_day_7d','_',if(t13.clk_day_7d is null, 'null', cast(log(2,t13.clk_day_7d+1) as bigint))),',',
        concat('uc2_clk_day_3d','_',if(t13.clk_day_3d is null, 'null', cast(log(2,t13.clk_day_3d+1) as bigint))),',',
        concat('uc2_clk_day_1d','_',if(t13.clk_day_1d is null, 'null', cast(log(2,t13.clk_day_1d+1) as bigint))),',',
        concat('uc2_clt_item_90d','_',if(t13.clt_item_90d is null, 'null', cast(log(2,t13.clt_item_90d+1) as bigint))),',',
        concat('uc2_clt_item_60d','_',if(t13.clt_item_60d is null, 'null', cast(log(2,t13.clt_item_60d+1) as bigint))),',',
        concat('uc2_clt_item_30d','_',if(t13.clt_item_30d is null, 'null', cast(log(2,t13.clt_item_30d+1) as bigint))),',',
        concat('uc2_clt_item_15d','_',if(t13.clt_item_15d is null, 'null', cast(log(2,t13.clt_item_15d+1) as bigint))),',',
        concat('uc2_clt_item_7d','_',if(t13.clt_item_7d is null, 'null', cast(log(2,t13.clt_item_7d+1) as bigint))),',',
        concat('uc2_clt_item_3d','_',if(t13.clt_item_3d is null, 'null', cast(log(2,t13.clt_item_3d+1) as bigint))),',',
        concat('uc2_clt_item_1d','_',if(t13.clt_item_1d is null, 'null', cast(log(2,t13.clt_item_1d+1) as bigint))),',',
        concat('uc2_cart_item_90d','_',if(t13.cart_item_90d is null, 'null', cast(log(2,t13.cart_item_90d+1) as bigint))),',',
        concat('uc2_cart_item_60d','_',if(t13.cart_item_60d is null, 'null', cast(log(2,t13.cart_item_60d+1) as bigint))),',',
        concat('uc2_cart_item_30d','_',if(t13.cart_item_30d is null, 'null', cast(log(2,t13.cart_item_30d+1) as bigint))),',',
        concat('uc2_cart_item_15d','_',if(t13.cart_item_15d is null, 'null', cast(log(2,t13.cart_item_15d+1) as bigint))),',',
        concat('uc2_cart_item_7d','_',if(t13.cart_item_7d is null, 'null', cast(log(2,t13.cart_item_7d+1) as bigint))),',',
        concat('uc2_cart_item_3d','_',if(t13.cart_item_3d is null, 'null', cast(log(2,t13.cart_item_3d+1) as bigint))),',',
        concat('uc2_cart_item_1d','_',if(t13.cart_item_1d is null, 'null', cast(log(2,t13.cart_item_1d+1) as bigint))),',',
        concat('uc2_cart_num_90d','_',if(t13.cart_num_90d is null, 'null', cast(log(2,t13.cart_num_90d+1) as bigint))),',',
        concat('uc2_cart_num_60d','_',if(t13.cart_num_60d is null, 'null', cast(log(2,t13.cart_num_60d+1) as bigint))),',',
        concat('uc2_cart_num_30d','_',if(t13.cart_num_30d is null, 'null', cast(log(2,t13.cart_num_30d+1) as bigint))),',',
        concat('uc2_cart_num_15d','_',if(t13.cart_num_15d is null, 'null', cast(log(2,t13.cart_num_15d+1) as bigint))),',',
        concat('uc2_cart_num_7d','_',if(t13.cart_num_7d is null, 'null', cast(log(2,t13.cart_num_7d+1) as bigint))),',',
        concat('uc2_cart_num_3d','_',if(t13.cart_num_3d is null, 'null', cast(log(2,t13.cart_num_3d+1) as bigint))),',',
        concat('uc2_cart_num_1d','_',if(t13.cart_num_1d is null, 'null', cast(log(2,t13.cart_num_1d+1) as bigint))),',',
        concat('uc2_pay_item_90d','_',if(t13.pay_item_90d is null, 'null', cast(log(2,t13.pay_item_90d+1) as bigint))),',',
        concat('uc2_pay_item_60d','_',if(t13.pay_item_60d is null, 'null', cast(log(2,t13.pay_item_60d+1) as bigint))),',',
        concat('uc2_pay_item_30d','_',if(t13.pay_item_30d is null, 'null', cast(log(2,t13.pay_item_30d+1) as bigint))),',',
        concat('uc2_pay_item_15d','_',if(t13.pay_item_15d is null, 'null', cast(log(2,t13.pay_item_15d+1) as bigint))),',',
        concat('uc2_pay_item_7d','_',if(t13.pay_item_7d is null, 'null', cast(log(2,t13.pay_item_7d+1) as bigint))),',',
        concat('uc2_pay_item_3d','_',if(t13.pay_item_3d is null, 'null', cast(log(2,t13.pay_item_3d+1) as bigint))),',',
        concat('uc2_pay_item_1d','_',if(t13.pay_item_1d is null, 'null', cast(log(2,t13.pay_item_1d+1) as bigint))),',',
        concat('uc2_pay_num_90d','_',if(t13.pay_num_90d is null, 'null', cast(log(2,t13.pay_num_90d+1) as bigint))),',',
        concat('uc2_pay_num_60d','_',if(t13.pay_num_60d is null, 'null', cast(log(2,t13.pay_num_60d+1) as bigint))),',',
        concat('uc2_pay_num_30d','_',if(t13.pay_num_30d is null, 'null', cast(log(2,t13.pay_num_30d+1) as bigint))),',',
        concat('uc2_pay_num_15d','_',if(t13.pay_num_15d is null, 'null', cast(log(2,t13.pay_num_15d+1) as bigint))),',',
        concat('uc2_pay_num_7d','_',if(t13.pay_num_7d is null, 'null', cast(log(2,t13.pay_num_7d+1) as bigint))),',',
        concat('uc2_pay_num_3d','_',if(t13.pay_num_3d is null, 'null', cast(log(2,t13.pay_num_3d+1) as bigint))),',',
        concat('uc2_pay_num_1d','_',if(t13.pay_num_1d is null, 'null', cast(log(2,t13.pay_num_1d+1) as bigint)))
    )) as user_cate2_cross_feature
    ,if(t14.user_id is null, 'ub_null',concat(
        concat('ub_clk_item_90d','_',if(t14.clk_item_90d is null, 'null', cast(log(2,t14.clk_item_90d+1) as bigint))),',',
        concat('ub_clk_item_60d','_',if(t14.clk_item_60d is null, 'null', cast(log(2,t14.clk_item_60d+1) as bigint))),',',
        concat('ub_clk_item_30d','_',if(t14.clk_item_30d is null, 'null', cast(log(2,t14.clk_item_30d+1) as bigint))),',',
        concat('ub_clk_item_15d','_',if(t14.clk_item_15d is null, 'null', cast(log(2,t14.clk_item_15d+1) as bigint))),',',
        concat('ub_clk_item_7d','_',if(t14.clk_item_7d is null, 'null', cast(log(2,t14.clk_item_7d+1) as bigint))),',',
        concat('ub_clk_item_3d','_',if(t14.clk_item_3d is null, 'null', cast(log(2,t14.clk_item_3d+1) as bigint))),',',
        concat('ub_clk_item_1d','_',if(t14.clk_item_1d is null, 'null', cast(log(2,t14.clk_item_1d+1) as bigint))),',',
        concat('ub_clk_num_90d','_',if(t14.clk_num_90d is null, 'null', cast(log(2,t14.clk_num_90d+1) as bigint))),',',
        concat('ub_clk_num_60d','_',if(t14.clk_num_60d is null, 'null', cast(log(2,t14.clk_num_60d+1) as bigint))),',',
        concat('ub_clk_num_30d','_',if(t14.clk_num_30d is null, 'null', cast(log(2,t14.clk_num_30d+1) as bigint))),',',
        concat('ub_clk_num_15d','_',if(t14.clk_num_15d is null, 'null', cast(log(2,t14.clk_num_15d+1) as bigint))),',',
        concat('ub_clk_num_7d','_',if(t14.clk_num_7d is null, 'null', cast(log(2,t14.clk_num_7d+1) as bigint))),',',
        concat('ub_clk_num_3d','_',if(t14.clk_num_3d is null, 'null', cast(log(2,t14.clk_num_3d+1) as bigint))),',',
        concat('ub_clk_num_1d','_',if(t14.clk_num_1d is null, 'null', cast(log(2,t14.clk_num_1d+1) as bigint))),',',
        concat('ub_clk_day_90d','_',if(t14.clk_day_90d is null, 'null', cast(log(2,t14.clk_day_90d+1) as bigint))),',',
        concat('ub_clk_day_60d','_',if(t14.clk_day_60d is null, 'null', cast(log(2,t14.clk_day_60d+1) as bigint))),',',
        concat('ub_clk_day_30d','_',if(t14.clk_day_30d is null, 'null', cast(log(2,t14.clk_day_30d+1) as bigint))),',',
        concat('ub_clk_day_15d','_',if(t14.clk_day_15d is null, 'null', cast(log(2,t14.clk_day_15d+1) as bigint))),',',
        concat('ub_clk_day_7d','_',if(t14.clk_day_7d is null, 'null', cast(log(2,t14.clk_day_7d+1) as bigint))),',',
        concat('ub_clk_day_3d','_',if(t14.clk_day_3d is null, 'null', cast(log(2,t14.clk_day_3d+1) as bigint))),',',
        concat('ub_clk_day_1d','_',if(t14.clk_day_1d is null, 'null', cast(log(2,t14.clk_day_1d+1) as bigint))),',',
        concat('ub_clt_item_90d','_',if(t14.clt_item_90d is null, 'null', cast(log(2,t14.clt_item_90d+1) as bigint))),',',
        concat('ub_clt_item_60d','_',if(t14.clt_item_60d is null, 'null', cast(log(2,t14.clt_item_60d+1) as bigint))),',',
        concat('ub_clt_item_30d','_',if(t14.clt_item_30d is null, 'null', cast(log(2,t14.clt_item_30d+1) as bigint))),',',
        concat('ub_clt_item_15d','_',if(t14.clt_item_15d is null, 'null', cast(log(2,t14.clt_item_15d+1) as bigint))),',',
        concat('ub_clt_item_7d','_',if(t14.clt_item_7d is null, 'null', cast(log(2,t14.clt_item_7d+1) as bigint))),',',
        concat('ub_clt_item_3d','_',if(t14.clt_item_3d is null, 'null', cast(log(2,t14.clt_item_3d+1) as bigint))),',',
        concat('ub_clt_item_1d','_',if(t14.clt_item_1d is null, 'null', cast(log(2,t14.clt_item_1d+1) as bigint))),',',
        concat('ub_cart_item_90d','_',if(t14.cart_item_90d is null, 'null', cast(log(2,t14.cart_item_90d+1) as bigint))),',',
        concat('ub_cart_item_60d','_',if(t14.cart_item_60d is null, 'null', cast(log(2,t14.cart_item_60d+1) as bigint))),',',
        concat('ub_cart_item_30d','_',if(t14.cart_item_30d is null, 'null', cast(log(2,t14.cart_item_30d+1) as bigint))),',',
        concat('ub_cart_item_15d','_',if(t14.cart_item_15d is null, 'null', cast(log(2,t14.cart_item_15d+1) as bigint))),',',
        concat('ub_cart_item_7d','_',if(t14.cart_item_7d is null, 'null', cast(log(2,t14.cart_item_7d+1) as bigint))),',',
        concat('ub_cart_item_3d','_',if(t14.cart_item_3d is null, 'null', cast(log(2,t14.cart_item_3d+1) as bigint))),',',
        concat('ub_cart_item_1d','_',if(t14.cart_item_1d is null, 'null', cast(log(2,t14.cart_item_1d+1) as bigint))),',',
        concat('ub_cart_num_90d','_',if(t14.cart_num_90d is null, 'null', cast(log(2,t14.cart_num_90d+1) as bigint))),',',
        concat('ub_cart_num_60d','_',if(t14.cart_num_60d is null, 'null', cast(log(2,t14.cart_num_60d+1) as bigint))),',',
        concat('ub_cart_num_30d','_',if(t14.cart_num_30d is null, 'null', cast(log(2,t14.cart_num_30d+1) as bigint))),',',
        concat('ub_cart_num_15d','_',if(t14.cart_num_15d is null, 'null', cast(log(2,t14.cart_num_15d+1) as bigint))),',',
        concat('ub_cart_num_7d','_',if(t14.cart_num_7d is null, 'null', cast(log(2,t14.cart_num_7d+1) as bigint))),',',
        concat('ub_cart_num_3d','_',if(t14.cart_num_3d is null, 'null', cast(log(2,t14.cart_num_3d+1) as bigint))),',',
        concat('ub_cart_num_1d','_',if(t14.cart_num_1d is null, 'null', cast(log(2,t14.cart_num_1d+1) as bigint))),',',
        concat('ub_pay_item_90d','_',if(t14.pay_item_90d is null, 'null', cast(log(2,t14.pay_item_90d+1) as bigint))),',',
        concat('ub_pay_item_60d','_',if(t14.pay_item_60d is null, 'null', cast(log(2,t14.pay_item_60d+1) as bigint))),',',
        concat('ub_pay_item_30d','_',if(t14.pay_item_30d is null, 'null', cast(log(2,t14.pay_item_30d+1) as bigint))),',',
        concat('ub_pay_item_15d','_',if(t14.pay_item_15d is null, 'null', cast(log(2,t14.pay_item_15d+1) as bigint))),',',
        concat('ub_pay_item_7d','_',if(t14.pay_item_7d is null, 'null', cast(log(2,t14.pay_item_7d+1) as bigint))),',',
        concat('ub_pay_item_3d','_',if(t14.pay_item_3d is null, 'null', cast(log(2,t14.pay_item_3d+1) as bigint))),',',
        concat('ub_pay_item_1d','_',if(t14.pay_item_1d is null, 'null', cast(log(2,t14.pay_item_1d+1) as bigint))),',',
        concat('ub_pay_num_90d','_',if(t14.pay_num_90d is null, 'null', cast(log(2,t14.pay_num_90d+1) as bigint))),',',
        concat('ub_pay_num_60d','_',if(t14.pay_num_60d is null, 'null', cast(log(2,t14.pay_num_60d+1) as bigint))),',',
        concat('ub_pay_num_30d','_',if(t14.pay_num_30d is null, 'null', cast(log(2,t14.pay_num_30d+1) as bigint))),',',
        concat('ub_pay_num_15d','_',if(t14.pay_num_15d is null, 'null', cast(log(2,t14.pay_num_15d+1) as bigint))),',',
        concat('ub_pay_num_7d','_',if(t14.pay_num_7d is null, 'null', cast(log(2,t14.pay_num_7d+1) as bigint))),',',
        concat('ub_pay_num_3d','_',if(t14.pay_num_3d is null, 'null', cast(log(2,t14.pay_num_3d+1) as bigint))),',',
        concat('ub_pay_num_1d','_',if(t14.pay_num_1d is null, 'null', cast(log(2,t14.pay_num_1d+1) as bigint)))
    )) as user_brand_cross_feature
from (
    select *
    from user_pay_sample
    where ds=${bizdate}
)t1 left join (
    select user_id, brand_id_seq
    from user_click_brand_seq_feature
    where ds=${bizdate}
)t2 on t1.user_id=t2.user_id
left join (
    select user_id, brand_id_seq
    from user_clt_brand_seq_feature
    where ds=${bizdate}
)t3 on t1.user_id=t3.user_id
left join (
    select user_id, brand_id_seq
    from user_pay_brand_seq_feature
    where ds=${bizdate}
)t4 on t1.user_id=t4.user_id
left join (
    select user_id, cate_seq
    from user_click_cate_seq_feature
    where ds=${bizdate}
)t5 on t1.user_id=t5.user_id
left join (
    select user_id, cate_seq
    from user_clt_cate_seq_feature
    where ds=${bizdate}
)t6 on t1.user_id=t6.user_id
left join (
    select user_id, cate_seq
    from user_pay_cate_seq_feature
    where ds=${bizdate}
)t7 on t1.user_id=t7.user_id

left join (
    select *
    from user_click_beh_feature_ads
    where ds=${bizdate}
)t8 on t1.user_id=t8.user_id
left join (
    select *
    from user_collect_beh_feature_ads
    where ds=${bizdate}
)t9 on t1.user_id=t9.user_id
left join (
    select *
    from user_cart_beh_feature_ads
    where ds=${bizdate}
)t10 on t1.user_id=t10.user_id
left join (
    select *
    from user_alipay_beh_feature_ads
    where ds=${bizdate}
)t11 on t1.user_id=t11.user_id
left join (
    select *
    from brand_stat_feature_ads
    where ds=${bizdate}
)t12 on t1.brand_id=t12.brand_id
left join (
    select *
    from user_brand_cate2_cross_beh_feature_ads
    where ds=${bizdate}
)t13 on t1.user_id=t13.user_id and t1.brand_id=t13.brand_id
left join (
    select *
    from user_brand_cross_beh_feature_ads
    where ds=${bizdate}
)t14 on t1.user_id=t14.user_id and t1.brand_id=t14.brand_id
where (t2.brand_id_seq is not null or t3.brand_id_seq is not null or t4.brand_id_seq is not null or
    t5.cate_seq is not null or t6.cate_seq is not null or t7.cate_seq is not null or
    t8.cnt_days_90d is not null or t9.cate1_num_90d is not null or t10.item_num_90d is not null
    or t11.item_num_90d is not null or t12.click_num is not null or t13.clk_item_90d is not null or
    t14.clk_item_90d is not null)
;