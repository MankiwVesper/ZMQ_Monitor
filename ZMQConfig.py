<<<<<<< HEAD
# -*- coding: utf-8 -*-

import json
import os.path
import sys

from log_setting import get_logger

zmq_config_logger = get_logger("ZMQConfig", './log')


class ConfigProcess:
    def __init__(self, config_file=None):
        if config_file is None:
            self.config_file = r'.\ZMQPoint.json'
            zmq_config_logger.warning("没有指定ZMQ节点的配置文件，使用默认文件！")
        if not os.path.exists(self.config_file):
            zmq_config_logger.critical("ZMQ配置文件不存在，请检查！")
            sys.exit()

    def get_zmq_point(self):
        """
        func: 将zmq配置文件的参数转换为字典，并返回
        :return: 包含了zmq配置参数的字典
        """
        with open(self.config_file, 'r') as file:
            zmq_config = json.load(file)

        return zmq_config

    def write_zmq_sub_point(self, zmq_paras):
        """
        func: 将传入的数据写入 ZMQPoint.json文件
        :param zmq_paras: 一个列表，保存了多个zmq节点的信息，每个节点的信息也是一个包含两个元素的列表 -> [["tcp://192.168.0.10:65231", False], ["tcp://192.168.0.20:10025", True]]
        """

        # 定义一个空列表，用来保存更新之后的zmq参数
        new_para = list()
        # 根据提供的数据生成最新的zmq配置参数
        for para in zmq_paras:
            # 修改最新设置的参数
            if len(para) != 2:
                zmq_config_logger.error("提供的ZMQ配置参数错误，请检查！")
                continue

            zmq_json_keys = ["addr", "sub"]
            zmq_json_data = dict(zip(zmq_json_keys, para))
            new_para.append(zmq_json_data)

        try:
            # 获取配置文件中原来的参数
            zmq_para = self.get_zmq_point()
            # 将最新的参数写入配置文件
            zmq_para['SUB'] = new_para
            with open(self.config_file, 'w') as file:
                json.dump(zmq_para, file, indent=4)

            zmq_config_logger.info(f"ZMQ配置文件{self.config_file}更新完成！")
        except Exception as e:
            zmq_config_logger.error("更新ZMQ参数失败，请检查！")
            zmq_config_logger.error(e)
=======
# -*- coding: utf-8 -*-

import json
import os.path
import sys

from log_setting import get_logger

zmq_config_logger = get_logger("ZMQConfig", './log')


class ConfigProcess:
    def __init__(self, config_file=None):
        if config_file is None:
            self.config_file = r'.\ZMQPoint.json'
            zmq_config_logger.warning("没有指定ZMQ节点的配置文件，使用默认文件！")
        if not os.path.exists(self.config_file):
            zmq_config_logger.critical("ZMQ配置文件不存在，请检查！")
            sys.exit()

    def get_zmq_point(self):
        """
        func: 将zmq配置文件的参数转换为字典，并返回
        :return: 包含了zmq配置参数的字典
        """
        with open(self.config_file, 'r') as file:
            zmq_config = json.load(file)

        return zmq_config

    def write_zmq_sub_point(self, zmq_paras):
        """
        func: 将传入的数据写入 ZMQPoint.json文件
        :param zmq_paras: 一个列表，保存了多个zmq节点的信息，每个节点的信息也是一个包含两个元素的列表 -> [["tcp://192.168.0.10:65231", False], ["tcp://192.168.0.20:10025", True]]
        """

        # 定义一个空列表，用来保存更新之后的zmq参数
        new_para = list()
        # 根据提供的数据生成最新的zmq配置参数
        for para in zmq_paras:
            # 修改最新设置的参数
            if len(para) != 2:
                zmq_config_logger.error("提供的ZMQ配置参数错误，请检查！")
                continue

            zmq_json_keys = ["addr", "sub"]
            zmq_json_data = dict(zip(zmq_json_keys, para))
            new_para.append(zmq_json_data)

        try:
            # 获取配置文件中原来的参数
            zmq_para = self.get_zmq_point()
            # 将最新的参数写入配置文件
            zmq_para['SUB'] = new_para
            with open(self.config_file, 'w') as file:
                json.dump(zmq_para, file, indent=4)

            zmq_config_logger.info(f"ZMQ配置文件{self.config_file}更新完成！")
        except Exception as e:
            zmq_config_logger.error("更新ZMQ参数失败，请检查！")
            zmq_config_logger.error(e)
>>>>>>> 321902d3dc73b29524d2c699db34dc86494efe35
