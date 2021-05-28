from abc import abstractmethod
import concurrent.futures

from apscheduler.executors.base import BaseExecutor, run_job

try:
    from concurrent.futures.process import BrokenProcessPool
except ImportError:
    BrokenProcessPool = None


class BasePoolExecutor(BaseExecutor):
    @abstractmethod
    def __init__(self, pool):
        super(BasePoolExecutor, self).__init__()
        self._pool = pool

    def _do_submit_job(self, job, run_times):
        def callback(f):
            # 添加成功或失败后的回调函数
            # 获取 exception 和 traceback
            exc, tb = (f.exception_info() if hasattr(f, 'exception_info') else
                       (f.exception(), getattr(f.exception(), '__traceback__', None)))
            if exc:
                self._run_job_error(job.id, exc, tb)
            else:
                self._run_job_success(job.id, f.result())

        # 往池中提交函数, 如果池子坏掉了就重新建一个再提交一次
        try:
            f = self._pool.submit(run_job, job, job._jobstore_alias, run_times, self._logger.name)
        except BrokenProcessPool:
            self._logger.warning('Process pool is broken; replacing pool with a fresh instance')
            self._pool = self._pool.__class__(self._pool._max_workers)
            f = self._pool.submit(run_job, job, job._jobstore_alias, run_times, self._logger.name)

        # 添加完成后的回调函数
        f.add_done_callback(callback)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)


# 线程池和进程池
class ThreadPoolExecutor(BasePoolExecutor):
    """
    An executor that runs jobs in a concurrent.futures thread pool.

    Plugin alias: ``threadpool``

    :param max_workers: the maximum number of spawned threads.
    """

    def __init__(self, max_workers=10):
        pool = concurrent.futures.ThreadPoolExecutor(int(max_workers))
        super(ThreadPoolExecutor, self).__init__(pool)


class ProcessPoolExecutor(BasePoolExecutor):
    """
    An executor that runs jobs in a concurrent.futures process pool.

    Plugin alias: ``processpool``

    :param max_workers: the maximum number of spawned processes.
    """

    def __init__(self, max_workers=10):
        pool = concurrent.futures.ProcessPoolExecutor(int(max_workers))
        super(ProcessPoolExecutor, self).__init__(pool)
