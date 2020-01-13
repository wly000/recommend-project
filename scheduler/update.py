from offline.update_article import UpdateArticle
from offline.update_user import UpdateUserProfile
from offline.update_recall import UpdateRecall
from offline.update_feature import FeaturePlatform


def update_article_profile():
    """
    定时更新文章画像的运行逻辑
    :return:
    """
    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    if sentence_df.rdd.collect():
        textrank_keywords_df, keywordsIndex = ua.generate_article_label()
        articleProfile = ua.get_article_profile(textrank_keywords_df, keywordsIndex)
        ua.compute_article_similar(articleProfile)


def update_user_profile():
    """
    定时更新用户画像的逻辑
    :return:
    """
    up = UpdateUserProfile()
    if up.update_user_action_basic():
        up.update_user_label()
        up.update_user_info()


def update_user_recall():
    """
    用户的频道推荐召回结果更新逻辑
    :return:
    """
    ur = UpdateRecall(500)
    ur.update_als_recall()
    ur.update_content_recall()


def update_ctr_feature():
    """
    特征中心平台（用户特征中心、文章特征中心）更新逻辑
    :return:
    """
    fp = FeaturePlatform()
    fp.update_user_ctr_feature_to_hbase()
    fp.update_article_ctr_feature_to_hbase()