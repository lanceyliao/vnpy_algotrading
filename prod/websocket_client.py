import asyncio
import json
import sys
import traceback
import platform
from datetime import datetime
from types import coroutine
from threading import Thread
from asyncio import (
    get_running_loop,
    new_event_loop,
    set_event_loop,
    run_coroutine_threadsafe,
    AbstractEventLoop,
    set_event_loop_policy,
    TimeoutError
)

# [Add] 增加对自定义本地IP绑定的支持
# https://github.com/vnpy/vnpy_websocket/commit/dc086f6d1701b4a95e2bb201baef32a385efca1e
from aiohttp import ClientSession, ClientWebSocketResponse, TCPConnector
from aiohttp.client_exceptions import ClientProxyConnectionError


# 在Windows系统上必须使用Selector事件循环，否则可能导致程序崩溃
if platform.system() == 'Windows':
    from asyncio import WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())


class WebsocketClient:
    """
    针对各类Websocket API的异步客户端

    * 重载unpack_data方法来实现数据解包逻辑
    * 重载on_connected方法来实现连接成功回调处理
    * 重载on_disconnected方法来实现连接断开回调处理
    * 重载on_packet方法来实现数据推送回调处理
    * 重载on_error方法来实现异常捕捉回调处理
    """

    def __init__(self):
        """Constructor"""
        self._active: bool = False
        self._host: str = ""
        self._connecting: bool = False  # 添加连接状态标志

        self._session: ClientSession = None
        self._ws: ClientWebSocketResponse = None
        self._loop: AbstractEventLoop = None

        self._proxy: str = None
        self._ping_interval: int = 10       # 秒
        self._header: dict = {}
        self._receive_timeout: int = 60     # 秒

        self._last_sent_text: str = ""
        self._last_received_text: str = ""

        # [Add] 增加对自定义本地IP绑定的支持
        # https://github.com/vnpy/vnpy_websocket/commit/dc086f6d1701b4a95e2bb201baef32a385efca1e
        self._local_host: str = ""

    def init(
        self,
        host: str,
        proxy_host: str = "",
        proxy_port: int = 0,
        ping_interval: int = 10,
        receive_timeout: int = 60,
        header: dict = None
    ):
        """
        初始化客户端
        """
        self._host = host
        self._ping_interval = ping_interval
        self._receive_timeout = receive_timeout

        if header:
            self._header = header

        if proxy_host and proxy_port:
            self._proxy = f"http://{proxy_host}:{proxy_port}"
            self.write_log(f"使用代理：{self._proxy}")

        self.write_log(f"WebSocket客户端初始化，地址：{host}")

    def start(self):
        """
        启动客户端

        连接成功后会自动调用on_connected回调函数，

        请等待on_connected被调用后，再发送数据包。
        """
        if self._connecting:  # 如果正在连接中，直接返回
            self.write_log("WebSocket客户端正在连接中，忽略重复启动请求")
            return

        self._active = True
        self.write_log("WebSocket客户端启动")

        try:
            self._loop = get_running_loop()
        except RuntimeError:
            self._loop = new_event_loop()
            self.write_log("创建新的事件循环")

        if not self._loop.is_running():
            start_event_loop(self._loop, self)

        run_coroutine_threadsafe(self._run(), self._loop)

    def stop(self):
        """
        停止客户端。
        """
        self._active = False
        self._connecting = False

        # [Mod] 调整退出机制，解决关闭客户端时的阻塞问题
        # https://github.com/vnpy/vnpy_websocket/commit/61bcfb4ad2125bcdae8e13bfc54112c99fc07a96
        # if self._session:
        #     coro = self._session.close()
        #     run_coroutine_threadsafe(coro, self._loop).result()
        #
        # if self._ws:
        #     coro = self._ws.close()
        #     run_coroutine_threadsafe(coro, self._loop).result()

        self.write_log("WebSocket客户端停止中")

        if self._loop and self._loop.is_running():
            self._loop.stop()
            self.write_log("事件循环已停止")
        else:
            self.write_log("事件循环未运行")

    def join(self):
        """
        等待后台线程退出。
        """
        pass

    def send_packet(self, packet: dict):
        """
        发送数据包字典到服务器。

        如果需要发送非json数据，请重载实现本函数。
        """
        if self._ws:
            text: str = json.dumps(packet)
            self._record_last_sent_text(text)
            # self.write_log(f"发送数据：{text[:100]}...")  # 只记录前100个字符

            coro: coroutine = self._ws.send_str(text)
            run_coroutine_threadsafe(coro, self._loop)

    def unpack_data(self, data: str):
        """
        对字符串数据进行json格式解包

        如果需要使用json以外的解包格式，请重载实现本函数。
        """
        return json.loads(data)

    def on_connected(self):
        """连接成功回调"""
        pass

    # def on_disconnected(self):
    #     """连接断开回调"""
    #     pass
    def on_disconnected(self, status_code: int = None, msg: str = None):
        """
        连接断开回调
        status_code: 断开状态码
        msg: 断开原因描述
        """
        pass

    def on_packet(self, packet: dict):
        """收到数据回调"""
        pass

    def write_log(self, msg: str) -> None:
        """
        输出日志
        子类需要实现此方法
        """
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] {msg}"
        print(formatted_msg)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb
    ) -> None:
        """触发异常回调"""
        try:
            self.write_log("WebsocketClient on error" + "-" * 10)
            self.write_log(self.exception_detail(exception_type, exception_value, tb))
        except Exception:
            traceback.print_exc()

    def exception_detail(
        self,
        exception_type: type,
        exception_value: Exception,
        tb
    ) -> str:
        """异常信息格式化"""
        text = "[{}]: Unhandled WebSocket Error:{}\n".format(
            datetime.now().isoformat(), exception_type
        )
        text += "LastSentText:\n{}\n".format(self._last_sent_text)
        text += "LastReceivedText:\n{}\n".format(self._last_received_text)
        text += "Exception trace: \n"
        text += "".join(
            traceback.format_exception(exception_type, exception_value, tb)
        )
        return text

    async def _run(self):
        """
        在事件循环中运行的主协程
        """
        # [Add] 增加对自定义本地IP绑定的支持
        # https://github.com/vnpy/vnpy_websocket/commit/dc086f6d1701b4a95e2bb201baef32a385efca1e
        # self._session = ClientSession()
        self.write_log("开始运行WebSocket客户端")
        
        if self._local_host:
            conn: TCPConnector = TCPConnector(local_addr=(self._local_host, 0))
            self._session: ClientSession = ClientSession(connector=conn, trust_env=True)
            self.write_log(f"使用本地地址创建会话：{self._local_host}")
        else:
            self._session = ClientSession()
            self.write_log("创建默认会话")

        while self._active:
            # 捕捉运行过程中异常
            try:
                # 发起Websocket连接
                self._connecting = True  # 标记开始连接
                self.write_log(f"尝试连接到服务器：{self._host}")
                self._ws = await self._session.ws_connect(
                    self._host,
                    proxy=self._proxy,
                    verify_ssl=False,
                    # [Mod] 发起ws连接时传入心跳间隔参数
                    # https://github.com/vnpy/vnpy_websocket/commit/59e2708effb2dcf1beea253d684394b7f29c2d7d
                    heartbeat=self._ping_interval,
                    receive_timeout=self._receive_timeout
                )

                # 调用连接成功回调
                self._connecting = False  # 连接完成
                self.write_log("WebSocket连接建立成功")
                self.on_connected()

                # 持续处理收到的数据
                async for msg in self._ws:
                    text: str = msg.data
                    self._record_last_received_text(text)
                    # self.write_log(f"收到数据：{text[:100]}...")  # 只记录前100个字符

                    data: dict = self.unpack_data(text)
                    self.on_packet(data)

                # 移除Websocket连接对象
                self._ws = None
                self.write_log("WebSocket连接正常关闭")
                self.on_disconnected(1000, "Connection closed normally")

            # 接收数据超时重连
            except TimeoutError:
                # 关闭当前websocket连接
                self.write_log("WebSocket连接超时")
                if self._ws:
                    await self._ws.close()
                    self._ws = None

                # 超时断开的情况
                self.on_disconnected(1002, "Connection timeout")
            # 处理捕捉到的异常                
            # 代理连接错误
            except ClientProxyConnectionError as e:
                self.write_log(f"代理连接失败：{str(e)}")
                if self._ws:
                    await self._ws.close()
                    self._ws = None
                self.on_disconnected(1006, f"Proxy connection failed: {str(e)}")
                
            # 处理捕捉到的其他异常
            except Exception:
                et, ev, tb = sys.exc_info() # et: exception type, ev: exception value, tb: traceback
                self.write_log(f"WebSocket连接异常：{str(ev)}")
                self.on_error(et, ev, tb)

                # 确保关闭ws连接
                if self._ws is not None:
                    await self._ws.close()
                    self._ws = None
                    
                # 异常断开的情况
                self.on_disconnected(1006, f"Connection error: {str(ev)}")

            finally:
                self._connecting = False  # 确保连接标志被重置
                self.write_log("WebSocket连接重置状态")

                # 在重试之前添加短暂延迟
                if self._active:
                    await asyncio.sleep(1)

    def _record_last_sent_text(self, text: str):
        """记录最近发出的数据字符串"""
        self._last_sent_text = text[:1000]

    def _record_last_received_text(self, text: str):
        """记录最近收到的数据字符串"""
        self._last_received_text = text[:1000]


def start_event_loop(loop: AbstractEventLoop, client: WebsocketClient = None) -> AbstractEventLoop:
    """启动事件循环"""
    if client:
        client.write_log("启动事件循环后台线程")
    
    # 如果事件循环未运行，则创建后台线程来运行
    if not loop.is_running():
        thread = Thread(target=run_event_loop, args=(loop, client))
        thread.daemon = True
        thread.start()


def run_event_loop(loop: AbstractEventLoop, client: WebsocketClient = None) -> None:
    """运行事件循环"""
    if client:
        client.write_log("事件循环开始运行")
    set_event_loop(loop)
    loop.run_forever()
