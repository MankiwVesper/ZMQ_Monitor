# -*- coding: utf-8 -*-
from collections import deque
import threading


class DataBuffer:
    def __init__(self, max_size=50000):
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.max_size = max_size
        self.drop_count = 0

    def put(self, data):
        with self.condition:
            if len(self.buffer) == self.max_size:
                self.drop_count += 1

            self.buffer.append(data)

            # 通知等待数据的消费者线程：现在有数据了
            self.condition.notify()

    def get(self):
        with self.lock:
            if len(self.buffer) == 0:
                return None

            return self.buffer.popleft()

    def wait_get(self, stop_event):
        with self.condition:
            while not self.buffer and not stop_event.is_set():
                self.condition.wait()

            if stop_event.is_set():
                return None
            
            return self.buffer.popleft()

    def wake_all(self):
        with self.condition:
            self.condition.notify_all()

    def size(self):
        with self.lock:
            return len(self.buffer)

    def resize(self, new_max_size):
        if new_max_size <= 0:
            raise ValueError("缓存大小必须大于0")

        with self.lock:
            if new_max_size < len(self.buffer):
                self.drop_count += len(self.buffer) - new_max_size

            # 如果新的最大长度小于当前队列长度，则会丢弃多余的数据，只保留最新的部分
            self.buffer = deque(
                self.buffer,
                maxlen=new_max_size
            )

            self.max_size = new_max_size

    def clear(self):
        with self.lock:
            self.buffer.clear()

    def get_drop_count(self):
        return self.drop_count
