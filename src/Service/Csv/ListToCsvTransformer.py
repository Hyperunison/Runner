import csv
import re

class ListToCsvTransformer:

    def convert(self, rows, file_path):
        field_names = rows[0].attribute_map.values()
        key_names = rows[0].attribute_map.keys()
        dict_list = []

        for row in rows:
            item = {}

            for key in key_names:
                value = row.get(key)
                item[self.snake_to_camel(key)] = value

            dict_list.append(item)

        with open(file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(dict_list)

    def snake_to_camel(self, snake_str):
        return re.sub(r'_([a-z])', lambda x: x.group(1).upper(), snake_str)