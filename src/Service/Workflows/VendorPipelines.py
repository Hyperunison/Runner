import os
from typing import Optional, List

from src import Api
from src.Service.Workflows import PipelineExecutor
from src.Service.Workflows.External.ExternalPipeline import ExternalPipeline
from src.UCDM import DataSchema


class VendorPipelines:
    pipelines_dir: str = 'vendor/pipelines'

    def __init__(self, api: Api, pipeline_executor: PipelineExecutor, schema: DataSchema):
        self.api = api
        self.pipeline_executor = pipeline_executor
        self.schema = schema

    def __scan_pipeline_versions(self) -> List[str]:
        result: List[str] = []
        vendors = os.listdir(self.pipelines_dir)
        for folder in vendors:
            if not os.path.isdir("{}/{}".format(self.pipelines_dir, folder)):
                continue
            pipelines = os.listdir("{}/{}".format(self.pipelines_dir, folder))
            for pipeline in pipelines:
                if not os.path.isdir("{}/{}/{}".format(self.pipelines_dir, folder, pipeline)):
                    continue
                versions = os.listdir("{}/{}/{}".format(self.pipelines_dir, folder, pipeline))
                for version in versions:
                    if not os.path.isdir("{}/{}/{}/{}".format(self.pipelines_dir, folder, pipeline, version)):
                        continue
                    print("{} {} {}".format(folder, pipeline, version))
                    result.append("{}/{}:{}".format(folder, pipeline, version))

        return result

    def sync_pipeline_list_with_backend(self):
        pipelines = self.__scan_pipeline_versions()
        self.api.send_installed_pipelines(pipelines)
        pass

    def find_pipeline(self, name: str, version: str) -> Optional[ExternalPipeline]:
        filename = "{}/{}/{}/main.py".format(self.pipelines_dir, name, version)
        if os.path.isfile(filename):
            return ExternalPipeline(self.api, self.pipeline_executor, self.schema, filename)

        return None
