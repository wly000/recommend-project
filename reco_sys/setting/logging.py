import logging
import logging.handlers
import os

logging_file_dir = '/root/logs/'


def create_logger():
    """
    设置日志
    :param app:
    :return:
    """

    # 离线处理更新打印日志
    trace_file_handler = logging.FileHandler(
        os.path.join(logging_file_dir, 'offline.log')
    )
    trace_file_handler.setFormatter(logging.Formatter('%(message)s'))
    log_trace = logging.getLogger('offline')
    log_trace.addHandler(trace_file_handler)
    log_trace.setLevel(logging.INFO)

    # 在线日志打印
    # 离线处理更新打印日志
    trace_file_handler = logging.FileHandler(
        os.path.join(logging_file_dir, 'online.log')
    )
    trace_file_handler.setFormatter(logging.Formatter('%(message)s'))
    log_trace = logging.getLogger('online')
    log_trace.addHandler(trace_file_handler)
    log_trace.setLevel(logging.INFO)