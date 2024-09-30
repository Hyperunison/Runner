import sys
import os
import json

from src.Database.DML.InsertRows import InsertRows
from src.Database.DTO.InsertRowsDTO import InsertRowsDTO

print("Insert rows into table")
argv = sys.argv

if len(argv) != 4:
    print('Invalid arguments count!')
    print('Correct format is: python insert-rows.py "connection string" "table name" "path to rows in json file"')
    exit(1)

if not os.path.isfile(argv[3]):
    print('File with rows in JSON does not exist!')
    exit(1)

with open(argv[3]) as json_data:
    rows = json.load(json_data)

dto = InsertRowsDTO(
    table_name=argv[2],
)
dto.rows = rows

insert_rows = InsertRows(
    connection_string=argv[1],
)
insert_rows.execute(dto)
print("Rows were inserted")