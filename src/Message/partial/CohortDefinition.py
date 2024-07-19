from typing import Dict, List, Optional


class CohortDefinition:
    key: str
    participant_table: str
    participant_id_field: str
    fields: List[Dict[str, str]]
    joins: List[Dict[str, str]]
    where: List[Dict[str, any]]
    export: List[Dict[str, any]]
    cte: List[Dict[str, str]]
    limit: Optional[int]
    with_tables: Dict[str, List[any]]

    def __init__(self, cohort_definition: Dict[str, any]):
        self.key = cohort_definition['key']
        self.participant_table = cohort_definition['participantTableName']
        self.participant_id_field = cohort_definition['participantIdField']
        self.fields = cohort_definition['fields']
        self.joins = cohort_definition['join']
        self.where = cohort_definition['where']
        self.export = cohort_definition['export']
        self.cte = cohort_definition['cte']
        self.limit = cohort_definition['limit']

        self.with_tables = {}
        for table_name in cohort_definition['withTables']:
            if not table_name in self.with_tables:
                self.with_tables[table_name] = []
            for val in cohort_definition['withTables'][table_name]:
                new_def = CohortDefinition(val)
                self.with_tables[table_name].append(new_def)
