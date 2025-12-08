import csv
import logging
from typing import List, Dict

from src.Message.StartCreateSQLViewsWorkflow import StartCreateSQLViewsWorkflow
from src.Service.SqlBuilder import get_field_type
from src.Service.Workflows import PipelineExecutor
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Api import Api
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Service.UCDMResolver import UCDMResolver
from src.UCDM.DataSchema import DataSchema, escape_string
from src.Service.ApiLogger import ApiLogger
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator

class CreateSQLViews(WorkflowBase):
    resolver: UCDMResolver
    api: Api

    def execute(self, message: StartCreateSQLViewsWorkflow, api: Api):
        views_schema: str = 'rosetta'

        api_logger = ApiLogger(self.api)
        length = len(message.queries.items())
        step = 0
        cdm_id = message.cdm_id

        self.schema.execute_sql("create schema if not exists {}".format(views_schema))
        self.fill_str_to_int_table(self.schema, views_schema)
        self.fill_semantic_mapping_table(self.schema, cdm_id, views_schema)

        for table_name, val in message.queries.items():
            try:
                api_logger.write(message.id, "Start exporting {}".format(table_name))
                query = CohortDefinition(val['query'])
                fields_map = val['fieldsMap']

                step += 1
                sql_source = self.get_sql_final(query, False)

                sql = "WITH __unison_source AS ({})\n\n".format(sql_source)
                sql += 'SELECT\n'
                joins: List[JoinDefinition] = []
                select: List[str] = []
                index = 0
                for var_name, field in fields_map.items():
                    index += 1
                    field_name = field['name']
                    is_required = field['isRequired']
                    data_type = get_field_type(field['type'])
                    strategies: Dict[str, Strategy] = {}
                    for bridge_id, var_settings in message.automation_strategies_map.items():
                        if not var_name in var_settings or not int(bridge_id) in val['bridgeIds']:
                            continue
                        setting: Dict[str, str] = var_settings[var_name]

                        strategy_name: str = 'conceptId' if setting['valueMappingType'] == 'conceptId' else setting['automationStrategy']
                        if not strategy_name in strategies:
                            strategies[strategy_name] = Strategy(strategy_name)
                        strategies[strategy_name].bridge_ids.append(bridge_id)

                    for strategy_name, strategy in strategies.items():
                        if strategy_name == 'conceptId':
                            alias = "__unison_concept_" + str(index)
                            joins.append(JoinDefinition(
                                "{}.__semantic_mapping".format(views_schema),
                                alias,
                                '{a}.bridge_id="c.__bridge_id" AND {a}.field_name=\'{field}\' AND {a}.source_value="{var}"'.format(
                                    field=escape_string(table_name + "." + field_name),
                                    a=escape_string(alias),
                                    var=escape_string(var_name),
                                ),
                                is_required
                            ))
                            strategy.select = '{a}.mapped_value'.format(
                                field_name=escape_string(field_name),
                                var=escape_string(var_name),
                                a=alias
                            )
                        elif strategy_name == 'none':
                            strategy.select = '"{var}"'.format(
                                field_name=escape_string(field_name),
                                var=escape_string(var_name),
                            )
                        elif strategy_name == 'serial':
                            strategy.select = 'row_number() over (order by "{var}")'.format(
                                field_name=escape_string(field_name),
                                var=escape_string(var_name),
                            )
                        else:
                            alias = 's2i_' + str(index)
                            joins.append(JoinDefinition(
                                inner=False,
                                tbl='{}.__str_to_int'.format(views_schema),
                                alias=alias,
                                on='"{var}"::varchar = {alias}.str'.format(
                                    alias=escape_string(alias),
                                    var=escape_string(var_name)
                                ),
                            ))
                            strategy.select = 'COALESCE({alias}.num, get_or_create_number("{var}"::varchar))'.format(
                                field_name=escape_string(field_name),
                                var=escape_string(var_name),
                                alias=escape_string(alias)
                            )
                    if len(strategies) == 1:
                        select.append(next(iter(strategies.values())).select + "::{} AS {}".format(data_type, field_name))
                    elif len(strategies) == 0:
                        select.append("NULL::{} AS {}".format(data_type, field_name))
                    else:
                        s = '(CASE\n'
                        for name, strategy in strategies.items():
                            s += ' WHEN "c.__bridge_id" IN ({ids}) THEN {s}\n'.format(
                                ids=', '.join([str(val) for val in strategy.bridge_ids]),
                                s=strategy.select,
                            )
                        s += 'END)::{} AS {}'.format(data_type, field_name)
                        select.append(s)

                sql += ',\n\t'.join(select) + '\n'
                sql += 'FROM __unison_source\n'
                for join in joins:
                    if not join.inner:
                        sql += 'LEFT '
                    sql += 'JOIN {} AS {} ON {}\n'.format(join.tbl, join.alias, join.on)

                self.schema.execute_sql('DROP VIEW IF EXISTS {}.{}'.format(views_schema, table_name))
                view_sql = 'CREATE OR REPLACE VIEW {}.{} AS {}'.format(views_schema, table_name, sql)
                self.schema.execute_sql(view_sql)
                logging.debug("View for table {} created".format(table_name))

            except Exce1ption as e:
                logging.debug("Can't create view for table {}".format(table_name))
                api_logger.write(message.id, "ERROR: Can't create view for table {}, sending error {}".format(
                    table_name,
                    ','.join(e.args)
                ))
                self.send_notification_to_api(message.id, length, step, 'error')
                raise e

    def create_semantic_mapping_table(self, schema: DataSchema, cdm_id: int, views_schema: str):
        logging.info("Creating table for semantic mappings")
        sql = "CREATE TABLE IF NOT EXISTS {schema}.__semantic_mapping (\n".format(schema=views_schema)
        sql += " bridge_id bigint not null,"
        sql += " field_name varchar(64) not null,"
        sql += " source_value varchar(1024),"
        sql += " mapped_value varchar(1024)"
        sql += ")"

        schema.execute_sql(sql)
        sql = "CREATE INDEX IF NOT EXISTS __semantic_mapping_uniq on {schema}.__semantic_mapping (bridge_id, field_name, source_value)".format(schema=views_schema)
        schema.execute_sql(sql)

    def fill_str_to_int_table(self, schema: DataSchema, views_schema: str):
        sql = 'CREATE SEQUENCE IF NOT EXISTS {}.__str_to_int_seq START 1'.format(views_schema)
        schema.execute_sql(sql)

        sql = 'CREATE TABLE IF NOT EXISTS {}.__str_to_int (str varchar(200) primary key, num bigint)'.format(views_schema)
        schema.execute_sql(sql)
        
        sql = 'CREATE OR REPLACE FUNCTION get_or_create_number(value TEXT) RETURNS INTEGER LANGUAGE plpgsql AS $$\n'
        sql += 'DECLARE result INTEGER; BEGIN\n'
        sql += 'IF value IS NULL THEN RETURN NULL; END IF;\n'
        sql += 'SELECT num INTO result FROM {}.__str_to_int WHERE str = value;\n'.format(views_schema)
        sql += 'IF result IS NOT NULL THEN RETURN result; END IF;\n'
        sql += "result := nextval('{}.__str_to_int_seq');\n".format(views_schema)
        sql += 'BEGIN\n'
        sql += 'INSERT INTO {}.__str_to_int (str, num) VALUES (value, result); EXCEPTION WHEN unique_violation THEN\n'.format(views_schema)
        sql += ' SELECT num INTO result FROM {}.__str_to_int WHERE str = value;\n'.format(views_schema)
        sql += 'END;\n'
        sql += 'return result;\n'
        sql += 'END; $$;\n'

        schema.execute_sql(sql)

        logging.debug('Table __str_to_int and function get_or_create_number created, filling data from csv')
        str_to_int = StrToIntGenerator()
        str_to_int.load_from_file()
        for s, v in str_to_int.map.items():
            schema.execute_sql("SELECT get_or_create_number('{}')".format(escape_string(s)))

        logging.debug('Data is imported to the __str_to_int')


    def fill_semantic_mapping_table(self, schema: DataSchema, cdm_id: int, views_schema: str):
        self.create_semantic_mapping_table(schema, cdm_id, views_schema)
        response = self.api.export_mapping(cdm_id)
        with open(response.name, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            header = next(reader)

            schema.execute_sql("BEGIN TRANSACTION;")

            schema.execute_sql("TRUNCATE TABLE {}.__semantic_mapping".format(views_schema))
            to_insert = []
            for line in reader:
                row = dict(zip(header, line))
                if row['exportValue'] == '':
                    # Skip not mapped values
                    continue

                to_insert.append({
                    "bridge_id": row['unisonBridgeId'],
                    "field_name": row['fieldName'],
                    "source_value": row['sourceCode'],
                    "mapped_value": row['exportValue'],
                })
                if len(to_insert) >= 1000:
                    self.insert_into_samantic(to_insert, schema, views_schema)
                    to_insert = []
            if len(to_insert) > 0:
                self.insert_into_samantic(to_insert, schema, views_schema)

            schema.execute_sql("COMMIT")

    def insert_into_samantic(self, to_insert: List[Dict[str, str]], schema: DataSchema, views_schema: str) -> None:
        logging.debug("inserting next {} rows into {}.__semantic_mapping".format(len(to_insert), views_schema))
        sql = "INSERT INTO {}.__semantic_mapping VALUES \n".format(views_schema)
        first = True
        for row in to_insert:
            if not first:
                sql += ",\n"
            first = False
            sql += "('{}', '{}', '{}', '{}')".format(
                escape_string(row['bridge_id']),
                escape_string(row['field_name']),
                escape_string(row['source_value']),
                escape_string(row['mapped_value'])
            )
        sql += ""

        schema.execute_sql(sql)



    def send_notification_to_api(self, id: int, length: int, step: int, state: str):
        percent = int(round(step / length * 100, 0))
        self.api.set_job_state(run_id=str(id), state=state, percent=percent, path='')


class JoinDefinition:
    tbl: str
    alias: str
    on: str
    inner: bool
    def __init__(self, tbl: str, alias: str, on: str, inner: False):
        self.tbl = tbl
        self.alias = alias
        self.on = on
        self.inner = inner


class Strategy:
    name: str
    bridge_ids: List[int] = []
    select: str
    def __init__(self, name: str):
        self.name = name