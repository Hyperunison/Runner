import csv
from typing import List, Dict

class CsvToMappingTransformer:
    columns_mapping = {
        'varName': 'var_name',
        'fieldName': 'field_name',
        'automationStrategy': 'automation_strategy',
        'sourceCode': 'biobank_value',
        'conceptId': 'export_value',
        'isConceptId': 'is_concept_id',
        'unisonBridgeId': 'unison_bridge_id',
    }

    def transform_with_file_path(self, csv_file_path, table = None) -> List[Dict[str, str]]:
        with open(csv_file_path, newline='') as csvfile:
            result = []
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            columns_index = self.build_columns_index(next(reader))

            if len(columns_index) != len(self.columns_mapping):
                raise Exception('Invalid CSV file')

            for row in reader:
                if not table is None:
                    if not self.is_table(row, columns_index, table):
                        continue

                result_item = {}

                for key, value in columns_index.items():
                    result_item[key] = row[value]
                result.append(result_item)

            return result

    def build_columns_index(self, fields) -> Dict[str, int]:
        result = {}
        fields_dict = {field: index for index, field in enumerate(fields)}

        for key, value in fields_dict.items():
            if not key in self.columns_mapping:
                continue
            result[self.columns_mapping[key]] = value

        return result

    def is_table(self, row, fields, table) -> bool:
        field_name = row[fields['field_name']]
        table_name = field_name.split('.')[0]

        return table_name == table
