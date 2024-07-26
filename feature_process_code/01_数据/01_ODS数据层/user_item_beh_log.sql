create table if not exists user_item_beh_log (
    item_id string ,
    user_id string ,
    action string,
    vtime string
) LIFECYCLE 90;