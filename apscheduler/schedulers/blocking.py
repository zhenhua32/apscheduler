from __future__ import absolute_import

from threading import Event

from apscheduler.schedulers.base import BaseScheduler, STATE_STOPPED
from apscheduler.util import TIMEOUT_MAX


class BlockingScheduler(BaseScheduler):
    """
    A scheduler that runs in the foreground
    (:meth:`~apscheduler.schedulers.base.BaseScheduler.start` will block).
    """
    _event = None

    def start(self, *args, **kwargs):
        # 定义一个线程事件, 如果没有事件实例, 或者这个实例已经是触发状态了
        if self._event is None or self._event.is_set():
            self._event = Event()

        super(BlockingScheduler, self).start(*args, **kwargs)
        self._main_loop()

    def shutdown(self, wait=True):
        super(BlockingScheduler, self).shutdown(wait)
        self._event.set()

    def _main_loop(self):
        wait_seconds = TIMEOUT_MAX
        while self.state != STATE_STOPPED:
            # 等待事件触发, 也就是有人调用了 wakeup 方法
            self._event.wait(wait_seconds)
            self._event.clear()
            # 处理任务, 并且返回下一次任务要等待的时间
            wait_seconds = self._process_jobs()

    def wakeup(self):
        # 触发事件
        self._event.set()
