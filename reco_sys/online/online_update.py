import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from online import stream_sc, SIMILAR_DS
import json
import time
from datetime import datetime
import logging

logger = logging.getLogger('online')


class OnlineRecall(object):
    """在线计算部分
    1、在线内容召回，实时写入用户点击或者操作文章的相似文章
    2、在线新文章召回
    3、在线热门文章召回
    """
    def __init__(self):
        pass

    def _update_content_recall(self):
        """
        在线内容召回计算
        :return:
        """
        # {"actionTime":"2019-04-10 21:04:39","readTime":"","channelId":18,
        # "param":{"action": "click", "userId": "2", "articleId": "116644", "algorithmCombine": "C2"}}
        # x [,'json.....']
        def get_similar_online_recall(rdd):
            """
            解析rdd中的内容，然后进行获取计算
            :param rdd:
            :return:
            """
            # rdd---> 数据本身
            # [row(1,2,3), row(4,5,6)]----->[[1,2,3], [4,5,6]]
            import happybase
            # 初始化happybase连接
            pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)
            for data in rdd.collect():

                # 进行data字典处理过滤
                if data['param']['action'] in ["click", "collect", "share"]:

                    logger.info(
                        "{} INFO: get user_id:{} action:{}  log".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        data['param']['userId'], data['param']['action']))

                    # 读取param当中articleId，相似的文章
                    with pool.connection() as conn:

                        sim_table = conn.table("article_similar")

                        # 根据用户点击流日志涉及文章找出与之最相似文章(基于内容的相似)，选取TOP-k相似的作为召回推荐结果
                        _dic = sim_table.row(str(data["param"]["articleId"]).encode(), columns=[b"similar"])
                        _srt = sorted(_dic.items(), key=lambda obj: obj[1], reverse=True)  # 按相似度排序
                        if _srt:

                            topKSimIds = [int(i[0].split(b":")[1]) for i in _srt[:10]]

                            # 根据历史推荐集过滤，已经给用户推荐过的文章
                            history_table = conn.table("history_recall")

                            _history_data = history_table.cells(
                                b"reco:his:%s" % data["param"]["userId"].encode(),
                                b"channel:%d" % data["channelId"]
                            )
                            # print("_history_data: ", _history_data)

                            # history = []
                            # if len(data) >= 2:
                            #     for l in data[:-1]:
                            #         history.extend(eval(l))
                            # else:
                            #     history = []
                            history = []
                            if len(_history_data) > 1:
                                for l in _history_data:
                                    history.extend(l)

                            # 根据历史召回记录，过滤召回结果
                            recall_list = list(set(topKSimIds) - set(history))

                            # 如果有推荐结果集，那么将数据添加到cb_recall表中，同时记录到历史记录表中
                            logger.info(
                                "{} INFO: store online recall data:{}".format(
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(recall_list)))

                            if recall_list:

                                recall_table = conn.table("cb_recall")

                                recall_table.put(
                                    b"recall:user:%s" % data["param"]["userId"].encode(),
                                    {b"online:%d" % data["channelId"]: str(recall_list).encode()}
                                )

                                history_table.put(
                                    b"reco:his:%s" % data["param"]["userId"].encode(),
                                    {b"channel:%d" % data["channelId"]: str(recall_list).encode()}
                                )
                        conn.close()

        # x可以是多次点击行为数据，同时拿到多条数据
        SIMILAR_DS.map(lambda x: json.loads(x[1])).foreachRDD(get_similar_online_recall)


if __name__ == '__main__':
    import setting.logging as lg
    lg.create_logger()
    ore = OnlineRecall()
    ore._update_content_recall()
    stream_sc.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        pass
