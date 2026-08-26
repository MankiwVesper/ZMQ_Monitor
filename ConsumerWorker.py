# -*- coding: utf-8 -*-
from PyQt5.QtCore import QObject, pyqtSignal
import threading

from DataTransform import *


class ConsumerWorker(QObject):
    can_messages = pyqtSignal(list)  # 定义信号，用来抛出订阅到的CAN数据
    stop_get_message = pyqtSignal()  # 定义停止获取信息的信号

    MAX_BATCH_SIZE = 50     # 一次最多包含50条数据

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
                # 阻塞等待第一条数据
                first_message = self.buffer.wait_get(self.stop_event)
                if first_message is None:
                    break

                # 保存这一批原始数据
                raw_messages = [first_message]

                # 第一条已经拿到了，后面的数据只取当前Buffer中已经存在的，不再等待凑满一批
                while len(raw_messages) < self.MAX_BATCH_SIZE:
                    message = self.buffer.get()
                    if message is None:
                        break
                    raw_messages.append(message)

                # 保存转换完成、准备发给GUI数据
                messages = []
                for message in raw_messages:
                    try:
                        messages.append(
                            [
                                message['time'],
                                message['sub_addr'],
                                message['theme'].decode('utf-8'),
                                *(self._bytes2can(message['message']))
                            ]
                        )
                    except Exception as e:
                        print("解析数据出异常：", e)

                # 一次发送这一批数据
                if messages:
                    self.can_messages.emit(messages)

            except Exception as e:
                print("获取数据出现异常：", e)

        self.stop_get_message.emit()

    def stop(self):
        self.stop_event.set()
        self.buffer.wake_all()
