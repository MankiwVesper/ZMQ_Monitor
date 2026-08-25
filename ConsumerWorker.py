# -*- coding: utf-8 -*-
from PyQt5.QtCore import QObject, pyqtSignal
import threading

from DataTransform import *


class ConsumerWorker(QObject):
    can_message = pyqtSignal(list)  # 定义信号，用来抛出订阅到的CAN数据
    stop_get_message = pyqtSignal()  # 定义停止获取信息的信号

    def __init__(self, buffer):
        super().__init__()
        self.buffer = buffer
        self.stop_event = threading.Event()

    @staticmethod
    def _bytes2can(bytes_message):
        """
        func: 将ZMQ总线数据转换为CAN格式的数据
        :param bytes_message: ZMQ总线数据
        :return: 元组，(通道ID，数据长度，源地址，目的地址，帧ID, CID，数据)，每个字段均为字符串
        """
        can_data = CANData()
        frame_id, channel_id, data_len, data = can_data.get_can_data(bytes_message)
        # data部分原来是一个列表，其中的元素为十进制整数
        frame_id_bin = bin(frame_id)  # 首先计算帧ID的二进制形式
        # 通过帧ID计算源地址、目的地址、CID
        cid = hex(int(frame_id_bin[-5:-1], 2))[2:].upper().rjust(2, '0')
        smac = hex(int(frame_id_bin[-13:-5], 2))[2:].upper().rjust(2, '0')
        dmac = hex(int(frame_id_bin[-21:-13], 2))[2:].upper().rjust(2, '0')

        # 计算帧ID的十六进制形式
        frame_id = hex(frame_id)[2:].upper()

        # 现在将每个元素转换为16进制，并用空格将所有元素组合成一整个字符串，方便后续写入和用户查看
        # 转成16进制时，右对齐，不足两位的高位填充 0；且16进制前面不带 0x,，字母大写，不包括CID
        data = ' '.join([hex(item)[2:].upper().rjust(2, '0') for item in list(data)[1:data_len]])
        data_len = data_len - 1  # 只包含CAN数据的长度，不包含CID

        return [str(channel_id), str(data_len), smac, dmac, frame_id, cid, data]

    def run(self):
        while not self.stop_event.is_set():
            try:
                message = self.buffer.wait_get(self.stop_event)
                if message is None:
                    break  # 如果返回None，说明stop_event被设置了，退出循环

                # 将ZMQ总线数据转换为CAN格式的数据，并通过信号发送出去
                self.can_message.emit(
                    [
                        message['time'], 
                        message['sub_addr'], 
                        message['theme'].decode('utf-8'), 
                        *(self._bytes2can(message['message']))
                    ]
                )
            except Exception as e:
                print("获取数据出现异常：", e)

        self.stop_get_message.emit()

    def stop(self):
        self.stop_event.set()
        self.buffer.wake_all()
