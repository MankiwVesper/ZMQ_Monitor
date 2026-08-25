import threading
import time
from datetime import datetime

import zmq

from DataBuffer import DataBuffer
from log_setting import get_logger

# 设置ZMQ接收数据时的超时时间（单位：毫秒）
ZMQ_RCV_TIMEOUT = 2000

# 此处采用双端队列deque，为的是能够在原来队列基础上调整其最大长度（普通队列 deque 创建之后，就不能修改其最大长度）
# 设置队列的最大长度为 50000
data_buffer = DataBuffer(max_size=50000)

zmq_sub_logger = get_logger("ZMQSub", "./log")


class ZMQSubFlag:
    """定义一个类，用来保存变量，这些类变量的值可以在不同的Python脚本之间共享"""
    sub_flag = True  # 表示是否订阅的标志位


class SubPointStates:
    """定义一个类，用来保存变量，这些类变量的值可以在不同的Python脚本之间共享"""
    bad_points = set()  # 保存订阅数据异常的zmq节点地址
    good_points = set()  # 保存订阅数据正常的zmq节点地址


class ZMQSuber:
    def __init__(self, sub_points, zmq_sub_theme):
        """
        :param sub_points: 一个列表，保存了多个zmq节点的信息，每个节点的信息也是一个包含两个元素的列表 -> [["tcp://192.168.0.10:8082", True], ["tcp://192.168.0.20:5556", False]]
        :param zmq_sub_theme: 需要订阅的ZMQ主题
        """

        self.zmq_sub_theme = zmq_sub_theme  # 保存ZMQ订阅节点的主题

        # 保存用户希望的节点订阅状态
        # key = ZMQ节点地址，value = Bool, 表示该ZMQ节点的数据是否被订阅
        self.sub_states = {point[0]: bool(point[1]) for point in sub_points}

        # GUI线程可能修改sub_states,
        # ZMQ接收线程会读取sub_states,
        # 所以这里使用一把锁保护
        self.states_lock = threading.Lock()

        # 下面这些对象都将在真正开始订阅之后创建
        self.context = None
        self.poller = None

        # addr -> socket
        self.sockets = dict()  # key = ZMQ节点地址，value = ZMQ SUB socket

        # socket -> addr
        self.socket_to_addr = dict()  # key = ZMQ SUB socket，value = ZMQ节点地址

    def _create_sub_socket(self, addr):
        """为一个ZMQ节点创建SUB Socket"""
        socket = self.context.socket(zmq.SUB)

        # 接收全部ZMQ主题的数据，设置订阅主题为空字符串
        socket.setsockopt_string(zmq.SUBSCRIBE, '')

        # 设置接收超时时间
        socket.setsockopt(zmq.RCVTIMEO, ZMQ_RCV_TIMEOUT)  # 设置ZMQ接收数据的超时时间为 2000ms

        # Subscriber 连接 Publisher
        socket.connect(addr)

        # 注册到Poller
        self.poller.register(socket, zmq.POLLIN)

        # 保存Socket和ZMQ节点地址的映射关系
        self.sockets[addr] = socket
        self.socket_to_addr[socket] = addr

        zmq_sub_logger.info(f"创建了地址为【{addr}】的ZMQ Subscribe Socket, 超时时间为 {ZMQ_RCV_TIMEOUT}ms")

    def _close_sub_socket(self, addr):
        """关闭指定ZMQ节点的SUB Socket"""

        socket = self.sockets.pop(addr, None)

        if socket is None:
            zmq_sub_logger.warning(f"尝试关闭ZMQ节点【{addr}】的SUB Socket，但该Socket不存在！")
            return

        try:
            self.poller.unregister(socket)
        except KeyError:
            zmq_sub_logger.warning(f"尝试注销ZMQ节点【{addr}】的SUB Socket，但该Socket未注册！")

        # 删除socket_to_addr中的映射关系
        self.socket_to_addr.pop(socket, None)

        socket.close(linger=0)  # 立即关闭socket，不等待未发送的消息

        zmq_sub_logger.info(f"关闭了地址为【{addr}】的ZMQ Subscribe Socket")

    def _sync_sub_sockets(self):
        """根据GUI设置的订阅状态，创建或关闭SUB Socket"""

        with self.states_lock:
            states = self.sub_states.copy()

        for addr, is_subbing in states.items():
            # 用户要求订阅，但是Socket不存在，则创建Socket
            if is_subbing and addr not in self.sockets:
                self._create_sub_socket(addr)

            # 用户要求不订阅，但是Socket存在，则关闭Socket
            elif not is_subbing and addr in self.sockets:
                self._close_sub_socket(addr)

    def get_update_sub_info(self, zmq_point_name, is_sub):
        """
        func: 更新某个ZMQ节点是否需要订阅
        :param zmq_point_name: ZMQ节点地址
        :param is_sub:  Bool, 表示该ZMQ节点的数据是否被订阅
        """
        with self.states_lock:
            self.sub_states[zmq_point_name] = bool(is_sub)

        zmq_sub_logger.info(f"更新ZMQ节点【{zmq_point_name}】的订阅状态为: {bool(is_sub)}。")

    def start_sub(self):
        """在当前线程中监听所有需要订阅的ZMQ节点"""

        # Context可以在进程内共享
        self.context = zmq.Context.instance()

        # 创建Poller对象
        self.poller = zmq.Poller()

        zmq_sub_logger.info("ZMQ订阅开始运行 ...")

        try:
            while ZMQSubFlag.sub_flag:
                # 根据GUI设置的订阅状态，创建或关闭SUB Socket
                self._sync_sub_sockets()

                # 如果没有任何需要订阅的ZMQ节点，则等待一段时间后继续循环
                if not self.sockets:
                    time.sleep(0.05)
                    continue

                # 最多等待100ms
                events = dict(self.poller.poll(timeout=100))

                for socket, event in events.items():
                    if not event & zmq.POLLIN:
                        continue

                    addr = self.socket_to_addr.get(socket, None)
                    if addr is None:
                        zmq_sub_logger.warning("收到未知Socket的事件，忽略该事件！")
                        continue

                    self._recieve_message(socket, addr)
        finally:
            # 无论正常退出还是发生异常，都要关闭所有的SUB Socket
            for addr in list(self.sockets.keys()):
                self._close_sub_socket(addr)

            zmq_sub_logger.info("ZMQ订阅线程退出，所有SUB Socket已关闭。")

    def _recieve_message(self, socket, addr):
        """从指定ZMQ Socket接收一条业务消息"""

        try:
            # 第一包：主题
            # 收到数据的主题，主题没有经过序列化，无需反序列化
            zmq_message = socket.recv()
            # print(f'收到CAN数据的主题：{zmq_message.decode("utf-8")}')
        except zmq.error.Again:
            SubPointStates.bad_points.add(addr) if addr not in SubPointStates.bad_points else None
            SubPointStates.good_points.discard(addr)
            zmq_sub_logger.warning(f"ZMQ节点【{addr}】接收主题超时，请检查是否有主题发送到ZMQ总线！")
            return

        if self.zmq_sub_theme not in zmq_message:
            return

        # 第二包：真正的数据
        # 下一包数据就是序列化之后的数据
        try:
            next_zmq_message = socket.recv()
        except zmq.error.Again:
            SubPointStates.bad_points.add(addr) if addr not in SubPointStates.bad_points else None
            SubPointStates.good_points.discard(addr)
            zmq_sub_logger.warning(f"ZMQ节点【{addr}】接收数据超时，请检查是否有数据发送到ZMQ总线！")
            return


        data_buffer.put({
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            'sub_addr': addr,
            'theme': zmq_message,
            'message': next_zmq_message})
        
        SubPointStates.good_points.add(addr) if addr not in SubPointStates.good_points else None
        SubPointStates.bad_points.discard(addr)
