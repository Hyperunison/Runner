from typing import Dict, List
import logging
from concurrent.futures import ThreadPoolExecutor

from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.ThreadsManager.ThreadInfo import ThreadInfo
from src.ThreadsManager.Worker import Worker


class ThreadsManager:
    threads: Dict[str, Dict[str, ThreadInfo]] = None
    threads_config: Dict[str, Dict] = None
    config: Dict[str, Dict] = None
    executor: ThreadPoolExecutor = None
    manager: ConsoleApplicationManager = None

    def __init__(self, config: Dict[str, Dict], manager: ConsoleApplicationManager):
        self.threads = {}
        self.config = config
        self.manager = manager
        self.threads_config = config["threads"]

        for key in config["threads"]["queues"].keys():
            self.threads[key] = {}

        self.executor = ThreadPoolExecutor(max_workers=self.get_full_max_threads_count())

    def run_next_threads(self):
        logging.info("Starting the next chunk of threads")
        queues = self.get_available_threads()

        for queue in queues:
            self.remove_old_threads(queue)
            self.run_next_thread(queue)

    def run_next_thread(self, queue: str):
        max_count = self.get_max_threads_count(queue)
        count = len(self.threads[queue].keys())
        if count >= max_count:
            logging.info(f"The queue {queue} has {count} threads.")
            return

        worker = Worker(queue, self.config, self.manager)
        future = self.executor.submit(worker.run)
        self.threads[queue][str(worker.native_id)] = ThreadInfo(worker.native_id)
        future.add_done_callback(self._on_done)

    def _on_done(self, future):
        worker = future.result()
        self.remove_thread(worker.queue, worker.native_id)

    def remove_old_threads(self, queue: str):
        timeout = self.threads_config["queues"][queue]["timeout"]
        for pid in list(self.threads[queue].keys()):
            if self.threads[queue][pid].get_created_at_diff() >= timeout:
                pass

    def remove_thread(self, queue: str, pid: str):
        pass

    def get_available_threads(self) -> List[str]:
        return list(self.threads.keys())

    def get_max_threads_count(self, queue: str) -> int:
        return self.threads_config["queues"][queue]["threads_max_count"]

    def get_full_max_threads_count(self) -> int:
        result = 0
        queues = self.get_available_threads()

        for queue in queues:
            result += self.get_max_threads_count(queue)

        return result