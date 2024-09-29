import sys
import os
import json

from src.Database.DDL.CreateTable import CreateTable
from src.Database.DTO.CreateTableColumnDTO import CreateTableColumnDTO
from src.Database.DTO.CreateTableDTO import CreateTableDTO

print("Create table")
argv = sys.argv

if len(argv) != 4:
    print('Invalid arguments count!')
    print('Correct format is: python create-table.py "connection string" "table name" "path to json schema"')
    exit(1)

if not os.path.isfile(argv[3]):
    print('File with JSON schema does not exist!')
    exit(1)

with open(argv[3]) as json_data:
    columns_schema = json.load(json_data)

table_dto = CreateTableDTO(
    table_name=argv[2]
)

for column in columns_schema['columns']:
    table_dto.columns.append(
        CreateTableColumnDTO(
            name=column['name'],
            type=column['type'],
            nullable=column['nullable'],
        )
    )

create_table = CreateTable(
    connection_string=argv[1],
)
create_table.execute(
    table=table_dto
)
print("Table created successfully!")

# postgresql://root:password@pgsql:5432/export