import os
import sys
# 如果当前代码文件运行测试需要加入修改路径，否则后面的导包出现问题
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from offline import SparkSessionBase
# from offline.utils import textrank, segmentation
import happybase
import pyspark.sql.functions as F
from datetime import datetime
from datetime import timedelta
import time
import gc


class FeaturePlatform(SparkSessionBase):
    """特征更新平台
    """
    SPARK_APP_NAME = "featureCenter"
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        # _create_spark_session
        # _create_spark_hbase用户spark sql 操作hive对hbase的外部表
        self.spark = self._create_spark_hbase()

    def update_user_ctr_feature_to_hbase(self):
        """
        :return:
        """
        clr.spark.sql("use profile")

        user_profile_hbase = self.spark.sql(
            "select user_id, information.birthday, information.gender, article_partial, env from user_profile_hbase")

        # 特征工程处理
        # 抛弃获取值少的特征
        user_profile_hbase = user_profile_hbase.drop('env', 'birthday', 'gender')

        def get_user_id(row):
            return int(row.user_id.split(":")[1]), row.article_partial

        user_profile_hbase_temp = user_profile_hbase.rdd.map(get_user_id)

        from pyspark.sql.types import *

        _schema = StructType([
            StructField("user_id", LongType()),
            StructField("weights", MapType(StringType(), DoubleType()))
        ])

        user_profile_hbase_schema = self.spark.createDataFrame(user_profile_hbase_temp, schema=_schema)

        def frature_preprocess(row):

            from pyspark.ml.linalg import Vectors

            channel_weights = []
            for i in range(1, 26):
                try:
                    _res = sorted([row.weights[key] for key
                                   in row.weights.keys() if key.split(':')[0] == str(i)])[:10]
                    channel_weights.append(_res)
                except:
                    channel_weights.append([])

            return row.user_id, channel_weights

        res = user_profile_hbase_schema.rdd.map(frature_preprocess).collect()

        # 批量插入Hbase数据库中
        pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)
        with pool.connection() as conn:
            ctr_feature = conn.table('ctr_feature_user')
            with ctr_feature.batch(transaction=True) as b:
                for i in range(len(res)):
                    for j in range(25):
                        b.put("{}".format(res[i][0]).encode(),
                              {"channel:{}".format(j + 1).encode(): str(res[i][1][j]).encode()})
            conn.close()

    def update_article_ctr_feature_to_hbase(self):
        """
        :return:
        """
        # 文章特征中心
        self.spark.sql("use article")
        article_profile = self.spark.sql("select * from article_profile")

        def article_profile_to_feature(row):
            try:
                weights = sorted(row.keywords.values())[:10]
            except Exception as e:
                weights = [0.0] * 10
            return row.article_id, row.channel_id, weights

        article_profile = article_profile.rdd.map(article_profile_to_feature).toDF(
            ['article_id', 'channel_id', 'weights'])

        article_vector = self.spark.sql("select * from article_vector")
        article_feature = article_profile.join(article_vector, on=['article_id'], how='inner')

        def feature_to_vector(row):
            from pyspark.ml.linalg import Vectors
            return row.article_id, row.channel_id, Vectors.dense(row.weights), Vectors.dense(row.articlevector)

        article_feature = article_feature.rdd.map(feature_to_vector).toDF(
            ['article_id', 'channel_id', 'weights', 'articlevector'])

        # 保存特征数据
        cols2 = ['article_id', 'channel_id', 'weights', 'articlevector']
        # 做特征的指定指定合并
        article_feature_two = VectorAssembler().setInputCols(cols2[1:4]).setOutputCol("features").transform(
            article_feature)

        # 保存到特征数据库中
        def save_article_feature_to_hbase(partition):
            import happybase
            pool = happybase.ConnectionPool(size=10, host='hadoop-master')
            with pool.connection() as conn:
                table = conn.table('ctr_feature_article')
                for row in partition:
                    table.put('{}'.format(row.article_id).encode(),
                              {'article:{}'.format(row.article_id).encode(): str(row.features).encode()})

        article_feature_two.foreachPartition(save_article_feature_to_hbase)