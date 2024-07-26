--odps sql
--********************************************************************--
--author:xufeng_17
--create time:2024-04-20 17:35:13
--********************************************************************--
create table if not exists user_id_number (
    user_id string,
    number bigint
)LIFECYCLE 60;

INSERT OVERWRITE TABLE user_id_number
select user_id, ROW_NUMBER() OVER(ORDER BY rnd) AS number
from (
    select user_id, RAND() as rnd
    from (
        select DISTINCT user_id
        from user_item_beh_log
    )t1
)t1
;