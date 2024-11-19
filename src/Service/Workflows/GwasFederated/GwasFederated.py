import logging
from typing import List, Dict

from src import Api
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Message.StartWorkflow import StartWorkflow
from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.UCDM.DataSchema import VariableMapper


class GwasFederated(WorkflowBase):
    def execute(self, message: StartWorkflow, api: Api):
        logging.info("Workflow execution task")
        logging.info(message)
        logging.info("Parameters: {}".format(message.parameters))
        self.api.set_run_status(message.run_id, 'deploy')
        variables: List[str] = message.parameters['variables']
        ucdm = self.get_ucdm(message)
        self.api.add_log_chunk(message.run_id, "Building phenotype.txt\n")
        csv_content = self.build_phenotype(ucdm, variables)
        nextflow_config = self.get_nextflow_config(variables, bool(message.parameters['isBinary']))

        cmd = "sudo chown -R nextflow .; nextflow run genepi/nf-gwas -r v1.0.4 -c nextflow-gwas.config -c nextflow.config -name {} -with-report report.html -with-weblog {} -with-trace -ansi-log".format(
            message.run_name,
            message.weblog_url,
        )

        self.pipeline_executor.run_nextflow_run_abstract(
            message.run_id,
            cmd,
            message.dir,
            {
                "phenotype.txt": csv_content,
                "nextflow.config": file_get_contents('nextflow.config'),
                "nextflow-gwas.config": nextflow_config
            },
            {
                ".nextflow.log": "/basic/.nextflow.log",
                "nextflow-gwas.config": "/nextflow-gwas.config",
                "trace-*.txt": "/basic/",
                "output/": "/output/",
            },
            message.s3_path,
            message.aws_id,
            message.aws_key
        )

    def build_phenotype(self, ucdm: List[Dict[str, UCDMConvertedField]], variables: List[str]) -> str:
        if len(ucdm) == 0:
            return ''
        content = "FID IID " + (" ".join(variables)) + "\n"
        i = 1
        for row in ucdm:
            content += "{} {}".format(i, i)
            for key in variables:
                content += " " + self.convert_value(row[key].export_value)
            content += "\n"
            i += 1

        return content

    def convert_value(self, variable) -> str:
        if isinstance(variable, bool):
            return str(int(variable))

        return str(variable)

    def get_nextflow_config(self, variables: List[str], is_binary: bool) -> str:
        if is_binary:
            is_binary_str = 'true'
        else:
            is_binary_str = 'false'

        config = "params {\n"
        config += "  project                       = 'Unison_GWAS'\n"
        config += "  genotypes_prediction          = '/data/nextflow/data/example.{bim,bed,fam}'\n"
        config += "  genotypes_association         = '/data/nextflow/data/example.vcf.gz'\n"
        config += "  genotypes_build               = 'hg19'\n"
        config += "  genotypes_association_format  = 'vcf'\n"
        config += "  phenotypes_filename           = 'phenotype.txt'\n"
        config += "  phenotypes_columns            = '{}'\n".format(",".join(variables))
        config += "  phenotypes_binary_trait       = {}\n".format(is_binary_str)
        config += "  regenie_test                  = 'additive'\n"
        config += "  annotation_min_log10p         = 2\n"
        config += "  rsids_filename                = '/data/nextflow/data/rsids.tsv.gz'\n"
        config += "}\n"
        config += "\n"
        config += "process {\n"
        config += "   withName: '.*' {\n"
        config += "       cpus = 8\n"
        config += "       memory = 40.GB\n"
        config += "   }\n"
        config += "}\n"

        return config

    def get_sql_final(self, cohort_definition: CohortDefinition) -> str:
        mapper = VariableMapper(cohort_definition.fields)

        return self.schema.build_cohort_definition_sql_query(
            mapper,
            cohort_definition,
            False,
            False,
        )


def file_get_contents(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read()
