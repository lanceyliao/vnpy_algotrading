import json
import ssl
import sys
import traceback
from threading import Thread

import websocket  # type: ignore


class WebsocketClient:
    """
    Websocket API

    After creating the client object, use start() to run worker thread.
    The worker thread connects websocket automatically.

    Use stop to stop threads and disconnect websocket before destroying the client
    object (especially when exiting the programme).

    Default serialization format is json.

    Callbacks to overrides:
    * on_connected
    * on_disconnected
    * on_packet
    * on_error

    If you want to send anything other than JSON, override send_packet.
    """

    def __init__(self) -> None:
        """Constructor"""
        self.active: bool = False
        self.host: str = ""

        self.wsapp: websocket.WebSocketApp | None = None
        self.thread: Thread | None = None

        self.proxy_host: str | None = None
        self.proxy_port: int | None = None
        self.header: dict | None = None
        self.ping_interval: int = 0
        self.receive_timeout: int = 0

        self.trace: bool = False

    def write_log(self, msg: str) -> None:
        """输出日志"""
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] {msg}"
        print(formatted_msg)

    def init(
        self,
        host: str,
        proxy_host: str = "",
        proxy_port: int = 0,
        ping_interval: int = 10,
        receive_timeout: int = 60,
        header: dict | None = None,
        trace: bool = False
    ) -> None:
        """
        :param host:
        :param proxy_host:
        :param proxy_port:
        :param header:
        :param ping_interval: unit: seconds, type: int
        """
        self.host = host
        self.ping_interval = ping_interval  # seconds
        self.receive_timeout = receive_timeout

        if header:
            self.header = header

        if proxy_host and proxy_port:
            self.proxy_host = proxy_host
            self.proxy_port = proxy_port
            self.write_log(f"使用代理：http://{proxy_host}:{proxy_port}")

        websocket.enableTrace(trace)
        websocket.setdefaulttimeout(receive_timeout)
        self.write_log(f"WebSocket客户端初始化，地址：{host}")

    def start(self) -> None:
        """
        Start the client and on_connected function is called after webscoket
        is connected succesfully.

        Please don't send packet untill on_connected fucntion is called.
        """
        self.active = True
        self.thread = Thread(target=self.run)
        self.thread.start()
        self.write_log("WebSocket客户端启动")

    def stop(self) -> None:
        """
        Stop the client.
        """
        if not self.active:
            return
        self.active = False

        if self.wsapp:
            self.wsapp.close()
        self.write_log("WebSocket客户端停止中")

    def join(self) -> None:
        """
        Wait till all threads finish.

        This function cannot be called from worker thread or callback function.
        """
        if self.thread:
            self.thread.join()

    def send_packet(self, packet: dict) -> None:
        """
        Send a packet (dict data) to server

        override this if you want to send non-json packet
        """
        text: str = json.dumps(packet)
        if self.wsapp:
            self.wsapp.send(text)

    def run(self) -> None:
        """
        Keep running till stop is called.
        """
        def on_open(wsapp: websocket.WebSocketApp) -> None:
            self.write_log("WebSocket连接建立成功")
            self.on_connected()

        def on_close(wsapp: websocket.WebSocketApp, status_code: int, msg: str) -> None:
            self.write_log(f"WebSocket连接断开，状态码：{status_code}，原因：{msg}")
            self.on_disconnected(status_code, msg)

        def on_error(wsapp: websocket.WebSocketApp, e: Exception) -> None:
            self.write_log(f"WebSocket连接异常：{str(e)}\n{traceback.format_exc()}")
            self.on_error(e)

        def on_message(wsapp: websocket.WebSocketApp, message: str) -> None:
            self.on_message(message)

        self.write_log(f"尝试连接到服务器：{self.host}")
        
        self.wsapp = websocket.WebSocketApp(
            url=self.host,
            header=self.header,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message
        )

        proxy_type: str | None = None
        if self.proxy_host:
            proxy_type = "http"

        self.wsapp.run_forever(
            sslopt={"cert_reqs": ssl.CERT_NONE},
            ping_interval=self.ping_interval,
            http_proxy_host=self.proxy_host,
            http_proxy_port=self.proxy_port,
            proxy_type=proxy_type,
            reconnect=1
        )

    def on_message(self, message: str) -> None:
        """
        Callback when weboscket app receives new message
        """
        self.on_packet(json.loads(message))

    def on_connected(self) -> None:
        """
        Callback when websocket is connected successfully.
        """
        pass

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """
        Callback when websocket connection is closed.
        """
        pass

    def on_packet(self, packet: dict) -> None:
        """
        Callback when receiving data from server.
        """
        pass

    def on_error(self, e: Exception) -> None:
        """
        Callback when exception raised.
        """
        try:
            self.write_log("WebsocketClient on error" + "-" * 10)
            self.write_log(str(e))
        except Exception:
            traceback.print_exc()