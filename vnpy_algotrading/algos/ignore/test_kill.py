import signal
import time
import sys


class SimpleApp:
    def __init__(self):
        self._active = True  # 添加运行状态标记

        # 注册信号处理器
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        # 如果是非Windows系统，额外处理SIGTSTP
        if sys.platform != 'win32':
            signal.signal(signal.SIGTSTP, signal.SIG_IGN)

    def _signal_handler(self, signum, frame):
        """信号处理函数"""
        sig_name = signal.Signals(signum).name
        print(f"\n收到信号: {sig_name}，准备关闭应用...")
        self._active = False

    def run(self):
        """运行主循环"""
        print("应用已启动，进程ID:", os.getpid())
        print("可以使用 kill -15 <pid> 或 Ctrl+C 来测试信号处理")

        counter = 0
        while self._active:
            print(f"\r运行中... {counter}", end="", flush=True)
            counter += 1
            time.sleep(1)

        self.close()

    def close(self):
        """清理资源"""
        print("\n正在清理资源...")
        time.sleep(2)  # 模拟清理过程
        print("应用已安全关闭")


if __name__ == "__main__":
    import os

    app = SimpleApp()
    app.run()