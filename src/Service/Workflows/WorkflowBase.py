import os
from typing import List, Dict
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.UCDMResolver import UCDMResolver
from src.Message.StartWorkflow import StartWorkflow
from src.Api import Api
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Service.Workflows import PipelineExecutor
from src.Service.Csv.CsvToMappingTransformer import CsvToMappingTransformer
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator
from src.UCDM.DataSchema import DataSchema, VariableMapper
from src.Service.UCDMConvertedField import UCDMConvertedField

class WorkflowBase:
    mapping_file_name: str = "var/mapping-values.csv"
    cdm_concept_file_name: str = "var/concept.csv"
    cdm_vocabulary_file_name: str = "var/vocabulary.csv"
    api: Api
    pipeline_executor: PipelineExecutor
    schema: DataSchema

    def __init__(self, api: Api, pipeline_executor: PipelineExecutor, schema: DataSchema):
        self.api = api
        self.pipeline_executor = pipeline_executor
        self.schema = schema

    def execute(self, message: StartWorkflow, api: Api):
        pass

    def download_mapping(self, cdm_id: int):
        response = self.api.export_mapping()

        with open(os.path.abspath(self.mapping_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

    def download_cdm_concept(self, cdm_id: str):
        response = self.api.export_cdm_concept(cdm_id)

        with open(os.path.abspath(self.cdm_concept_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

    def download_cdm_vocabulary(self, cdm_id: str):
        response = self.api.export_cdm_vocabulary(cdm_id)

        with open(os.path.abspath(self.cdm_vocabulary_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

    def get_sql_final(
            self,
            cohort_definition: CohortDefinition,
            add_participant_id: bool = True
    ) -> str:
        mapper = VariableMapper(cohort_definition.fields)

        return self.schema.build_cohort_definition_sql_query(
            mapper,
            cohort_definition,
            False,
            add_participant_id
        )

    def get_ucdm(self, message: StartWorkflow) -> List[Dict[str, UCDMConvertedField]]:
        self.api.add_log_chunk(message.run_id, "Downloading mappings from Unison platform\n")
        self.download_mapping()
        query = CohortDefinition(message.cohort_definition)
        sql_final = self.get_sql_final(query)
        self.api.add_log_chunk(message.run_id, "Executing SQL query marmonizing the result\n{}\n".format(sql_final))
        csv_transformer = CsvToMappingTransformer()
        csv_mapping = csv_transformer.transform_with_file_path(os.path.abspath(self.mapping_file_name))
        ucdm_mapping_resolver = UCDMMappingResolver(csv_mapping)
        resolver = UCDMResolver(self.schema, ucdm_mapping_resolver)
        self.api.add_log_chunk(message.run_id, "Executing SQL query and harmonization result\n{}\n".format(sql_final))
        ucdm = resolver.get_ucdm_result(sql_final, StrToIntGenerator(), {}, message.automation_strategies_map)

        return ucdm