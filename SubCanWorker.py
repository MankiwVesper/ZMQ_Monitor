# -*- coding: utf-8 -*-
from PyQt5.QtCore import QObject, pyqtSignal

from ZMQSub import ZMQSuber


class SubCANWorker(QObject):
    """订阅CAN类型的ZMQ消息"""
    stop_sub_message = pyqtSignal()  # 定义停止订阅信息的信号

    def __init__(self, sub_points, can_zmq_theme=None):
        super().__init__()

        if can_zmq_theme is None:
            self.can_zmq_theme = b'CanServerData'
        else:
            self.can_zmq_theme = can_zmq_theme

        self.zmq_suber = ZMQSuber(sub_points, zmq_sub_theme=self.can_zmq_theme)

    def run(self):
        """启动保存CAN数据和消费CAN数据的子线程"""
        self.zmq_suber.start_sub()

        # start_sub()函数执行完毕，说明ZMQ订阅线程已经结束，抛出停止订阅的信号
        self.stop_sub_message.emit()
