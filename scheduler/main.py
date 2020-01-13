import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from scheduler.update import update_article_profile, update_user_profile, update_user_recall, update_ctr_feature
import setting.logging as lg
lg.create_logger()

# 创建scheduler，多进程执行
executors = {
    'default': ProcessPoolExecutor(3)
}

scheduler = BlockingScheduler(executors=executors)

# 添加一个定时运行文章画像更新的任务， 每隔1个小时运行一次
scheduler.add_job(update_article_profile, trigger='interval', hours=1)
# 添加一个定时运行用户画像更新的任务， 每隔2个小时运行一次
scheduler.add_job(update_user_profile, trigger='interval', hours=2)
# 添加一个定时运行用户召回更新的任务，每隔3小时运行一次
scheduler.add_job(update_user_recall, trigger='interval', hours=3)
# 添加一个定时运行特征中心平台的任务，每隔4小时更新一次
scheduler.add_job(update_ctr_feature, trigger='interval', hours=4)

scheduler.start()

