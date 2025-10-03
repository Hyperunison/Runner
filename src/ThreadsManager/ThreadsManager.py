from typing import Dict, List
import logging
from concurrent.futures import ThreadPoolExecutor

from src.Api import Api
from src.Message.BaseMessage import BaseMessage
from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.ThreadsManager.PipelineWorker import PipelineWorker
from src.ThreadsManager.ThreadInfo import ThreadInfo
from src.ThreadsManager.Worker import Worker
from src.auto.auto_api_client.api_client import ApiClient
from src.auto.auto_api_client.configuration import Configuration
from src.auto.auto_api_client.api import agent_api
from src.auto.auto_api_client.model.runner_message import RunnerMessage
import psutil

class ThreadsManager:
    threads: Dict[str, Dict[str, ThreadInfo]] = None
    threads_config: Dict[str, Dict] = None
    config: Dict[str, Dict] = None
    executor: ThreadPoolExecutor = None
    manager: ConsoleApplicationManager = None
    configuration: Configuration = None
    agent_id: int = None

    workers: List[Worker] = []

    def __init__(self, config: Dict[str, Dict], manager: ConsoleApplicationManager):
        self.threads = {}
        self.config = config
        self.manager = manager
        self.threads_config = config["threads"]

        for key in config["threads"]["queues"].keys():
            self.threads[key] = {}

        self.executor = ThreadPoolExecutor(max_workers=self.get_full_max_threads_count())
        self.configuration = self.manager.initialize(self.config)
        self.agent_id = self.get_agent_id()



    def get_agent_id(self):
        with ApiClient(self.configuration) as api_client:
            api_instance = agent_api.AgentApi(api_client)
            api = Api(api_instance, self.config['api_version'], self.config['agent_token'], self.config['api_request_cookie'])

            try:
                return api.get_agent_id()
            except Exception as e:
                if e.__class__.__name__ == "NotFoundException":
                    return None
                raise e

    def send_pipelines(self):
        worker = PipelineWorker(
            self.config,
            self.configuration,
        )
        worker.run()

    def get_not_depleted_queues(self) -> List[str]:
        counts: Dict[str, int] = {}
        for queue in self.get_queues():
            counts[queue] = 0
        for worker in self.workers:
            counts[worker.queue] += 1
        result: List[str] = []
        for queue in self.get_queues():
            max_count = self.get_max_threads_count(queue)
            workers_count = counts[queue]
            if max_count > workers_count:
                result.append(queue)
            else:
                logging.info("Queue {queue} is depleted".format(queue=queue))

        return result

    def terminate_timeouted_threads(self):
        queues = self.get_queues()

        for queue in queues:
            timeout = self.threads_config["queues"][queue]["timeout"]
            for pid in list(self.threads[queue].keys()):
                if self.threads[queue][pid].get_created_at_diff() >= timeout:
                    self.threads[queue][pid].worker.terminate()

    def execute_task(self, response: RunnerMessage, task: BaseMessage):
        worker = Worker(
            response.queue,
            response,
            task,
            self.config,
            self.configuration,
            self.manager,
            self.agent_id,
            self._on_start,
            self._on_finish
        )
        worker.start()

        self.workers.append(worker)

    def update_workers_status(self):
        for worker in self.workers[:]:
            worker.join(timeout=1)
            if pid_exists(worker.pid):
                logging.info("Worker {} is alive {}".format(worker.pid, worker.exitcode))
                continue
            logging.info("The worker has finished, queue: {}, pid: {}".format(worker.queue, worker.pid))
            self.workers.remove(worker)


    def _on_finish(self, worker: Worker):
        # logging.info("The worker has finished2, queue: {}, pid: {}".format(worker.queue, worker.pid))
        # self.remove_thread(worker.queue, str(worker.pid))
        pass

    def _on_start(self, worker: Worker):
        logging.info("The worker has started, queue: {}, pid: {}".format(worker.queue, worker.pid))
        self.threads[worker.queue][str(worker.pid)] = ThreadInfo(
            worker.pid,
            worker
        )

    def remove_thread(self, queue: str, pid: str):
        # if queue not in self.threads:
        #     return
        #
        # if pid not in self.threads[queue]:
        #     return

        # del self.threads[queue][pid]
        pass

    def get_queues(self) -> List[str]:
        return self.config["threads"]["queues"].keys()

    def get_max_threads_count(self, queue: str) -> int:
        return self.threads_config["queues"][queue]["threads_max_count"]

    def get_full_max_threads_count(self) -> int:
        result = 0
        queues = self.get_queues()

        for queue in queues:
            result += self.get_max_threads_count(queue)

        return result

def pid_exists(pid):
    return psutil.pid_exists(pid) and psutil.Process(pid).status() != psutil.STATUS_ZOMBIE