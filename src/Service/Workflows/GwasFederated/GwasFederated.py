import json
import logging
from typing import List, Dict

from src.Message.StartWorkflow import StartWorkflow
from src.Service.UCDMResolver import UCDMResolver
from src.Service.Workflows.WorkflowBase import WorkflowBase


class GwasFederated(WorkflowBase):
    def execute(self, message: StartWorkflow):
        logging.info("Workflow execution task")
        logging.info(message)

        resolver = UCDMResolver(self.api, self.schema)
        ucdm = resolver.get_ucdm_result(message.cohort_definition)
        csv_content = self.build_phenotype(ucdm)
        nextflow_config = self.get_nextflow_config(ucdm)

        self.adapter.run_nextflow_run_abstract(
            message.run_id,
            "pwd; sleep 10; nextflow run genepi/nf-gwas -r v1.0.4 -c nextflow.config -name {} -with-report report.html -with-weblog {} -with-trace -resume -ansi-log".format(
                message.run_name,
                message.weblog_url,
            ),
            message.dir,
            message.s3_path,
            {
                "phenotype.txt": csv_content,
                "nextflow.config": nextflow_config,
                'aws_config': "[default]\nregion = eu-central-1\n",
                'aws_credentials': "[default]\n" +
                                   "aws_access_key_id={}\n".format(message.aws_id) +
                                   "aws_secret_access_key={}\n".format(message.aws_key),
            },
            {
                ".nextflow.log": "/basic/",
                "trace-*.txt": "/basic/",
                "output/": "/output/",
            }
        )

    def build_phenotype(self, ucdm: List[Dict[str, str]]) -> str:
        if len(ucdm) == 0:
            return ''
        keys = [key for key, value in ucdm[0].items() if key != "participant_id"]

        content = "FID IID " + (" ".join(keys)) + "\n"
        i = 1
        for row in ucdm:
            content += "{} {}".format(i, i)
            for key in keys:
                content += " " + self.convert_value(row[key])
            content += "\n"
            i += 1

        return content

    def convert_value(self, variable) -> str:
        if isinstance(variable, bool):
            return str(int(variable))

        return str(variable)

    def get_nextflow_config(self, ucdm: List[Dict[str, str]]) -> str:
        keys = [key for key, value in ucdm[0].items() if key != "participant_id"]
        config =  "params {\n"
        config += "  project                       = 'Unison_GWAS_VCF_3'\n"
        config += "  genotypes_prediction          = '/data/nextflow/data/example.{bim,bed,fam}'\n"
        config += "  genotypes_association         = '/data/nextflow/data/example.vcf.gz'\n"
        config += "  genotypes_build               = 'hg19'\n"
        config += "  genotypes_association_format  = 'vcf'\n"
        config += "  phenotypes_filename           = 'phenotype.txt'\n"
        config += "  phenotypes_columns            = '{}'\n".format(",".join(keys))
        config += "  phenotypes_binary_trait       = false\n"
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