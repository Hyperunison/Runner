from src.Message.CreateFolder import CreateFolder
from src.Message.NextflowRun import NextflowRun


class BaseAdapter:
    def process_nextflow_run(self, message: NextflowRun) -> bool:
        pass

    def process_create_folder(self, message: CreateFolder) -> bool:
        pass

    def type(self):
        pass
