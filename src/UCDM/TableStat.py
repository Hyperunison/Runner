from typing import List


class TableStat:
    table_name: str
    column_name: str
    unique_count: int = ''
    nulls_count: int = ''
    min_value: any = ''
    max_value: any = ''
    avg_value: any = ''
    median12_value: any = ''
    median25_value: any = ''
    median37_value: any = ''
    median50_value: any = ''
    median63_value: any = ''
    median75_value: any = ''
    median88_value: any = ''
    abandoned: bool
    values: List[str] = []
