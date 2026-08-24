import threading
import time
from collections import deque
from datetime import datetime

import zmq

from log_setting import get_logger

# 设置ZMQ接收数据时的超时时间（单位：毫秒）
ZMQ_RCV_TIMEOUT = 2000
# 设置队列的最大长度
MAX_DEQUE_SIZE = 50000
# 全局队列，用来保存订阅到的数据，并可以用来在多个函数之间共享
# 此处采用双端队列deque，为的是能够在原来队列基础上调整其最大长度（普通队列 deque 创建之后，就不能修改其最大长度）
data_deque = deque(maxlen=MAX_DEQUE_SIZE)

# 所有线程访问 data_deque 时共用一把锁，防止多线程同时访问时出现数据错乱
data_lock = threading.Lock()

zmq_sub_logger = get_logger("ZMQSub", "./log")


class ZMQSubFlag:
    """定义一个类，用来保存变量，这些类变量的值可以在不同的Python脚本之间共享"""
    sub_flag = True  # 表示是否订阅的标志位
    get_flag = True  # 表示是否订阅的标志位


class SubPointStates:
    """定义一个类，用来保存变量，这些类变量的值可以在不同的Python脚本之间共享"""
    bad_points = set()  # 保存订阅数据异常的zmq节点地址
    good_points = set()  # 保存订阅数据正常的zmq节点地址

'''
class ZMQSubWorker:
    def __init__(self, sub_addr='tcp://localhost:8082', is_subbing=True):
        self.sub_addr = sub_addr
        self.is_subbing = is_subbing
        self.lock = threading.Lock()

    def sub_theme_data(self, theme=b'', filter_string=b''):
        """
        func: 订阅指定主题的ZMQ总线消息
        :param theme:   指定的主题，字符串
        :param filter_string:  指定的过滤器，只定于包含指定字符串的消息，字符串
        """
        zmq_sub_logger.info(f"ZMQ节点【{self.sub_addr}】开始订阅数据, 主题为 {theme.decode('utf-8')} ...")
        global data_deque

        # 创建ZMQ上下文和订阅套接字 - 2026/8/24
        # 之前并没有在子线程中创建socket，而是直接在主线程中创建socket，不是合理的设计模式
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, '')
        socket.setsockopt(zmq.RCVTIMEO, ZMQ_RCV_TIMEOUT)  # 设置ZMQ接收数据的超时时间为 2000ms
        socket.connect(self.sub_addr)
        zmq_sub_logger.info(f"创建了地址为【{self.sub_addr}】的ZMQ Subscribe Socket, 超时时间为 {ZMQ_RCV_TIMEOUT}ms")

        while ZMQSubFlag.sub_flag:
            while self.is_subbing:  # 该标志位作用于 当用户通过ZMQ节点前面的复选框勾选，来控制定于数据的循环是否执行
                if not ZMQSubFlag.sub_flag:  # 该标志位作用于 当没有通过ZMQ节点前面的复选框来控制订阅与否，而是只是实现点击【重置按钮】，可以跳出订阅数据的循环
                    zmq_sub_logger.warning(f"订阅被用户重置，ZMQ节点【{self.sub_addr}】停止订阅数据！")
                    break

                # 当用户通过界面修改了缓存的长度之后，在原有data_deque的基础上修改data_deque的长度
                if MAX_DEQUE_SIZE != data_deque.maxlen:
                    with self.lock:
                        data_deque = deque(data_deque, maxlen=MAX_DEQUE_SIZE)
                    zmq_sub_logger.info(f"缓存区大小调整为{MAX_DEQUE_SIZE}。")

                # 订阅字节流
                try:
                    zmq_message = socket.recv()
                except zmq.error.Again:
                    SubPointStates.bad_points.add(self.sub_addr) if self.sub_addr not in SubPointStates.bad_points else None
                    SubPointStates.good_points.discard(self.sub_addr)
                    zmq_sub_logger.warning(f"ZMQ节点【{self.sub_addr}】接收主题超时，请检查是否有主题发送到ZMQ总线！")
                    continue

                # 订阅指定的主题
                if theme in zmq_message and filter_string in zmq_message:
                    # 收到数据的主题，主题没有经过序列化，无需反序列化
                    # print(f'收到CAN数据的主题：{zmq_message.decode("utf-8")}')
                    # 下一包数据就是序列化之后的数据
                    try:
                        next_zmq_message = socket.recv()
                    except zmq.error.Again:
                        SubPointStates.bad_points.add(self.sub_addr) if self.sub_addr not in SubPointStates.bad_points else None
                        SubPointStates.good_points.discard(self.sub_addr)
                        zmq_sub_logger.warning(f"ZMQ节点【{self.sub_addr}】接收数据超时，请检查是否有数据发送到ZMQ总线！")
                        continue

                    if len(data_deque) != MAX_DEQUE_SIZE:
                        with self.lock:
                            data_deque.append({'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'sub_addr': self.sub_addr, 'theme': zmq_message,
                                               'message': next_zmq_message})
                        SubPointStates.good_points.add(self.sub_addr) if self.sub_addr not in SubPointStates.good_points else None
                        SubPointStates.bad_points.discard(self.sub_addr)
                    else:
                        zmq_sub_logger.warning("缓存已满，跳过添加该条数据！")
                        continue
            # 增加时间间隙，让系统切换到其他线程，否则当停止订阅时陷入死循环，来不及操作GUI界面来控制数据订阅流程，打破循环
            time.sleep(0.001)
        socket.close()
        zmq_sub_logger.info(f"地址为【{self.sub_addr}】的ZMQ Subscribe Socket 被关闭！")
'''

'''
class ZMQSubThread(threading.Thread):
    def __init__(self, sub_point, sub_theme):
        super().__init__()
        self.sub_point = sub_point
        self.sub_theme = sub_theme
        self.sub_worker = ZMQSubWorker(sub_addr=self.sub_point[0], is_subbing=self.sub_point[1])
        self.daemon = True  # 设置子线程的 daemon=True，当主线程退出时，子线程立刻退出
        zmq_sub_logger.info(f"ZMQ节点【{self.sub_point}】的订阅子线程创建完成，订阅主题为 {self.sub_theme.decode('utf-8')}。")

    def run(self) -> None:
        self.sub_worker.sub_theme_data(theme=self.sub_theme)
'''

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

        if len(data_deque) != MAX_DEQUE_SIZE:
            with data_lock:
                data_deque.append({
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 
                    'sub_addr': addr, 
                    'theme': zmq_message,
                    'message': next_zmq_message})
            SubPointStates.good_points.add(addr) if addr not in SubPointStates.good_points else None
            SubPointStates.bad_points.discard(addr)
        else:
            zmq_sub_logger.warning(f"缓存已满，跳过添加数据{next_zmq_message}！")
