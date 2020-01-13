#!/usr/bin/env bash
#
## user_profile
#sqoop import \
#        --connect jdbc:mysql://192.168.19.137/toutiao \
#        --username root \
#        --password password \
#        --table user_profile \
#        --m 4 \
#        --target-dir /user/hive/warehouse/toutiao.db/user_profile \
#        --incremental lastmodified \
#        --check-column update_time \
#        --merge-key user_id \
#        --last-value "2018-01-01 00:00:00"
#
#
#

# 多个文章相似导入
time=`date +"%Y-%m-%d" -d "-1day"`
declare -A check
check=([user_profile]=update_time [user_basic]=last_login [news_channel]=update_time)
declare -A merge
merge=([user_profile]=user_id [user_basic]=user_id [news_channel]=channel_id)

for k in ${!check[@]}
do
    sqoop import \
        --connect jdbc:mysql://192.168.19.137/toutiao \
        --username root \
        --password password \
        --table $k \
        --m 4 \
        --target-dir /user/hive/warehouse/toutiao.db/$k \
        --incremental lastmodified \
        --check-column ${check[$k]} \
        --merge-key ${merge[$k]} \
        --last-value ${time}
done

# news_article_basic
sqoop import \
    --connect jdbc:mysql://192.168.19.137/toutiao?tinyInt1isBit=false \
    --username root \
    --password password \
    --m 4 \
    --query 'select article_id, user_id, channel_id, REPLACE(REPLACE(REPLACE(title, CHAR(13),""),CHAR(10),""), ",", " ") title, status, update_time from news_article_basic WHERE $CONDITIONS' \
    --split-by user_id \
    --target-dir /user/hive/warehouse/toutiao.db/news_article_basic \
    --incremental lastmodified \
    --check-column update_time \
    --merge-key article_id \
    --last-value ${time}


# 全量导入表
sqoop import \
    --connect jdbc:mysql://192.168.19.137/toutiao \
    --username root \
    --password password \
    --table news_article_content \
    --m 4 \
    --hive-home /root/bigdata/hive \
    --hive-import \
    --hive-drop-import-delims \
    --hive-table toutiao.news_article_content \
    --hive-overwrite