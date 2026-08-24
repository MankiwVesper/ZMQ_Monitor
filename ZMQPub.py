# -*- coding: utf-8 -*-
import threading
import time
from collections import deque
from datetime import datetime
import msgpack
import zmq


class CanServerData:
    def __init__(self, data_id=0, dev_id=0, data_len=9, data=b''):
        self.data_id = data_id
        self.dev_id = dev_id
        self.data_len = data_len
        self.data = data


context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://10.100.24.106:8081")

# 初始化数据
data = CanServerData(0x1405A435, 0, 9, data=b''.ljust(128, b'\0'))

# 序列化数据
packed_data = msgpack.packb([data.data_id, data.dev_id, data.data_len, data.data])

while True:
    time.sleep(1)
    print('发送。。。。')
    # socket.send(b'557CanServerData', zmq.SNDMORE)
    # socket.send(packed_data)
    socket.send_multipart([b'557CanServerData', packed_data])
    print(packed_data)
