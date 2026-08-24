<<<<<<< HEAD
# -*- coding: utf-8 -*-
import msgpack

from log_setting import get_logger

data_transform_logger = get_logger("DataTransform", "./log")


class CANData:
    def __init__(self, frame_id=0, channel_id=0, data_len=9, data=b''):
        """
        func: 定义一个类，用来保存ZMQ总线上的CAN数据
        :param frame_id:    帧ID，int型，十进制
        :param channel_id:        CAN通道ID，int型, 十进制
        :param data_len:    CAN数据长度，int型, 十进制
        :param data:          CAN数据，字节流(bytes)，长度为128字节
        """
        self.frame_id = frame_id
        self.channel_id = channel_id
        self.data_len = data_len
        self.data = data

    def __repr__(self):
        return f"帧ID：{self.frame_id}\t设备ID：{self.channel_id}\t数据长度：{self.data_len}\t数据：{self.data}"

    def get_can_data(self, zmq_message):
        # 从接收到的ZMQ总线数据中（序列化之后的数据），恢复出CAN数据
        try:
            # 反序列化，得到一个包含四个元素的列表
            unpacked_can_data = msgpack.unpackb(zmq_message, raw=False)

            # 提取数据
            self.frame_id, self.channel_id, self.data_len, data = unpacked_can_data
            self.data = list(data)[:self.data_len]  # data中的前 data_len 位 为有效数据

            return unpacked_can_data
        except Exception as e:
            data_transform_logger.error("解析和获取CAN数据异常")
            data_transform_logger.error(e)
=======
# -*- coding: utf-8 -*-
import msgpack

from log_setting import get_logger

data_transform_logger = get_logger("DataTransform", "./log")


class CANData:
    def __init__(self, frame_id=0, channel_id=0, data_len=9, data=b''):
        """
        func: 定义一个类，用来保存ZMQ总线上的CAN数据
        :param frame_id:    帧ID，int型，十进制
        :param channel_id:        CAN通道ID，int型, 十进制
        :param data_len:    CAN数据长度，int型, 十进制
        :param data:          CAN数据，字节流(bytes)，长度为128字节
        """
        self.frame_id = frame_id
        self.channel_id = channel_id
        self.data_len = data_len
        self.data = data

    def __repr__(self):
        return f"帧ID：{self.frame_id}\t设备ID：{self.channel_id}\t数据长度：{self.data_len}\t数据：{self.data}"

    def get_can_data(self, zmq_message):
        # 从接收到的ZMQ总线数据中（序列化之后的数据），恢复出CAN数据
        try:
            # 反序列化，得到一个包含四个元素的列表
            unpacked_can_data = msgpack.unpackb(zmq_message, raw=False)

            # 提取数据
            self.frame_id, self.channel_id, self.data_len, data = unpacked_can_data
            self.data = list(data)[:self.data_len]  # data中的前 data_len 位 为有效数据

            return unpacked_can_data
        except Exception as e:
            data_transform_logger.error("解析和获取CAN数据异常")
            data_transform_logger.error(e)
>>>>>>> 321902d3dc73b29524d2c699db34dc86494efe35
