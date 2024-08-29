from typing import List, Dict
from typing import Optional

from src.Api import Api
from src.Service.ApiLogger import ApiLogger
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator

class UCDMResolverSimple:
    api: Api
    ucdm_mapping_resolver: UCDMMappingResolver

    def __init__(self, api: Api, ucdm_mapping_resolver: UCDMMappingResolver):
        self.api = api
        self.ucdm_mapping_resolver = ucdm_mapping_resolver

    def get_ucdm_result(
            self,
            sql: str,
            api_logger: Optional[ApiLogger],
            message_id: Optional[int],
            str_to_int: StrToIntGenerator
    ) -> List[Dict[str, UCDMConvertedField]]:
        pass