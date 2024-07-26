--@exclude_input=dw_user_item_alipay_log
--@exclude_input=dw_user_item_collect_log
--@exclude_input=dw_user_item_click_log
--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-05-05 21:27:11
--********************************************************************--
create TABLE if not exists user_click_cate_seq_feature (
    user_id string,
    cate_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_click_cate_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('c_',cate2)) as cate2_seq
from (
    select user_id, cate2
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC) AS number
    from (
        select user_id, cate2, max(op_time) as op_time
        from dw_user_item_click_log
        where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            and cate2 is not null
        group by user_id, cate2
    )t1
)t1
where number<=50
group by user_id
;

create TABLE if not exists user_clt_cate_seq_feature (
    user_id string,
    cate_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_clt_cate_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('c_',cate2)) as cate2_seq
from (
    select user_id, cate2
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC) AS number
    from (
        select user_id, cate2, MAX(op_time) as op_time
        from dw_user_item_collect_log
        where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            and cate2 is not null
        group by user_id, cate2
    )t1
)t1
where number<=50
group by user_id
;

create TABLE if not exists user_pay_cate_seq_feature (
    user_id string,
    cate_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_cate_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('c_',cate2)) as cate_seq
from (
    select user_id, cate2
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC) AS number
    from (
        select user_id, cate2, max(op_time) as op_time
        from dw_user_item_alipay_log
        where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
            and cate2 is not null
        group by user_id, cate2
    )t1
)t1
where number<=50
group by user_id
;