from typing import Dict, List
import logging
from concurrent.futures import ThreadPoolExecutor

from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.ThreadsManager.PipelineWorker import PipelineWorker
from src.ThreadsManager.ThreadInfo import ThreadInfo
from src.ThreadsManager.Worker import Worker
from src.auto.auto_api_client.configuration import Configuration

class ThreadsManager:
    threads: Dict[str, Dict[str, ThreadInfo]] = None
    threads_config: Dict[str, Dict] = None
    config: Dict[str, Dict] = None
    executor: ThreadPoolExecutor = None
    manager: ConsoleApplicationManager = None
    configuration: Configuration = None

    def __init__(self, config: Dict[str, Dict], manager: ConsoleApplicationManager):
        self.threads = {}
        self.config = config
        self.manager = manager
        self.threads_config = config["threads"]

        for key in config["threads"]["queues"].keys():
            self.threads[key] = {}

        self.executor = ThreadPoolExecutor(max_workers=self.get_full_max_threads_count())
        self.configuration = self.manager.initialize(self.config)

    def send_pipelines(self):
        worker = PipelineWorker(
            self.config,
            self.configuration,
        )
        worker.run()

    def run_next_threads(self):
        logging.debug("Starting the next chunk of threads")
        queues = self.get_available_threads()

        for queue in queues:
            self.remove_old_threads(queue)
            self.run_next_thread(queue)

    def run_next_thread(self, queue: str):
        max_count = self.get_max_threads_count(queue)
        count = len(self.threads[queue].keys())
        logging.debug("The number of threads in queue {} is {}".format(queue, count))
        if count >= max_count:
            logging.debug(f"The queue {queue} has {count} threads.")
            return

        worker = Worker(
            queue,
            self.config,
            self.configuration,
            self._on_start,
            self._on_finish
        )
        worker.start()

    def _on_finish(self, worker: Worker):
        self.remove_thread(worker.queue, str(worker.pid))

    def _on_start(self, worker: Worker):
        logging.debug("The worker has started, queue: {}, pid: {}".format(worker.queue, worker.pid))
        self.threads[worker.queue][str(worker.pid)] = ThreadInfo(
            worker.pid,
            worker
        )

    def remove_old_threads(self, queue: str):
        timeout = self.threads_config["queues"][queue]["timeout"]
        for pid in list(self.threads[queue].keys()):
            if self.threads[queue][pid].get_created_at_diff() >= timeout:
                self.threads[queue][pid].worker.terminate()

    def remove_thread(self, queue: str, pid: str):
        if queue not in self.threads:
            return

        if pid not in self.threads[queue]:
            return

        del self.threads[queue][pid]

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