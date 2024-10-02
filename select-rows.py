import sys
import os
import json

from src.Database.DML.SelectRows import SelectRows
from src.Database.Transformers.MessageQueryToDtoTransformer import MessageQueryToDtoTransformer
from src.Message.partial.CohortDefinition import CohortDefinition

print("Select rows from table")
argv = sys.argv

if len(argv) != 3:
    print('Invalid arguments count!')
    print('Correct format is: python select-rows.py "connection string" "path to json file with query"')
    exit(1)

if not os.path.isfile(argv[2]):
    print('File with query in JSON does not exist!')
    exit(1)

with open(argv[2]) as json_data:
    query = json.load(json_data)

transformer = MessageQueryToDtoTransformer()
select_dto = transformer.transform(CohortDefinition(query["query"]))
select_rows = SelectRows(
    connection_string=argv[1],
)
rows = select_rows.execute(select_dto)
print(rows)