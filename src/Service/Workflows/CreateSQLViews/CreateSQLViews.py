import csv
import logging
from typing import List, Dict, Optional, Set

from src.Message.StartCreateSQLViewsWorkflow import StartCreateSQLViewsWorkflow
from src.Service.SqlBuilder import get_field_type
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Api import Api
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Service.UCDMResolver import UCDMResolver
from src.UCDM.DataSchema import DataSchema, VariableMapper, escape_string
from src.Service.ApiLogger import ApiLogger
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator

class CreateSQLViews(WorkflowBase):
    resolver: UCDMResolver
    api: Api

    def execute(self, message: StartCreateSQLViewsWorkflow, api: Api):
        views_schema: str = message.schema

        api_logger = ApiLogger(self.api)
        if isinstance(message.queries, List) and len(message.queries) == 0:
            api_logger.write(message.id, "No queries provided")
            return
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
                bridge_queries = val.get('bridgeQueries') or []
                serial_fields = self.get_bridge_union_serial_fields(message, val)
                if bridge_queries and serial_fields is not None:
                    sql = self.build_bridge_union_sql(message, table_name, val, serial_fields)
                else:
                    sql_source = self.get_sql_final(query, False)
                    sql = self.build_view_sql(message, table_name, fields_map, val['bridgeIds'], sql_source)

                self.schema.execute_sql('DROP VIEW IF EXISTS {}.{}'.format(views_schema, table_name))
                view_sql = 'CREATE OR REPLACE VIEW {}.{} AS {}'.format(views_schema, table_name, sql)
                self.schema.execute_sql(view_sql)
                logging.debug("View for table {} created".format(table_name))

            except Exception as e:
                logging.debug("Can't create view for table {}".format(table_name))
                api_logger.write(message.id, "ERROR: Can't create view for table {}, sending error {}".format(
                    table_name,
                    ','.join(e.args)
                ))
                self.send_notification_to_api(message.id, length, step, 'error')
                raise e

    def build_view_sql(
            self,
            message: StartCreateSQLViewsWorkflow,
            table_name: str,
            fields_map: Dict[str, Dict[str, any]],
            bridge_ids: List[int],
            sql_source: str,
            column=lambda var_name: '"{}"'.format(escape_string(var_name)),
            bridge_ref: str = '"c.__bridge_id"',
            raw_vars: Set[str] = frozenset(),
    ) -> str:
        sql = "WITH __unison_source AS ({})\n\n".format(sql_source)
        sql += 'SELECT\n'
        joins: List[JoinDefinition] = []
        select: List[str] = []
        where_filters: List[str] = []
        index = 0
        for var_name, field in fields_map.items():
            index += 1
            field_name = field['name']
            is_required = field['isRequired']
            data_type = get_field_type(field['type'])
            col = column(var_name)
            if var_name in raw_vars:
                select.append('{} AS {}'.format(col, field_name))
                continue
            strategies = self.get_strategies(message, var_name, bridge_ids)

            for strategy_name, strategy in strategies.items():
                if strategy_name == 'conceptId':
                    alias = "__unison_concept_" + str(index)
                    bridge_ids_sql = ', '.join([str(bid) for bid in strategy.bridge_ids])
                    # Use LEFT JOIN when multiple strategies exist: rows from plain/none/serial
                    # bridges have no entry in __semantic_mapping and would be dropped by INNER JOIN
                    use_inner = is_required and len(strategies) == 1
                    joins.append(JoinDefinition(
                        "{}.__semantic_mapping".format(message.schema),
                        alias,
                        '{a}.bridge_id={bridge} AND {a}.field_name=\'{field}\' AND {a}.source_value={col} AND {bridge}::bigint IN ({bridge_ids})'.format(
                            field=escape_string(table_name + "." + field_name),
                            a=escape_string(alias),
                            col=col,
                            bridge=bridge_ref,
                            bridge_ids=bridge_ids_sql,
                        ),
                        use_inner
                    ))
                    # LEFT JOIN is used for mixed strategies, but is_required semantics
                    # must still filter unmapped conceptId rows
                    if is_required and not use_inner:
                        where_filters.append(
                            '({bridge}::bigint NOT IN ({bridge_ids}) OR {a}.mapped_value IS NOT NULL)'.format(
                                bridge=bridge_ref,
                                bridge_ids=bridge_ids_sql,
                                a=alias,
                            )
                        )
                    strategy.select = '{a}.mapped_value'.format(
                        field_name=escape_string(field_name),
                        var=escape_string(var_name),
                        a=alias
                    )
                elif strategy_name == 'none':
                    strategy.select = col
                elif strategy_name == 'serial':
                    strategy.select = 'row_number() over (order by {col})'.format(col=col)
                else:
                    alias = 's2i_' + str(index)
                    joins.append(JoinDefinition(
                        inner=False,
                        tbl='{}.__str_to_int'.format(message.schema),
                        alias=alias,
                        on='{col}::varchar = {alias}.str'.format(
                            alias=escape_string(alias),
                            col=col
                        ),
                    ))
                    strategy.select = 'COALESCE({alias}.num, get_or_create_number({col}::varchar))'.format(
                        col=col,
                        alias=escape_string(alias)
                    )
            if len(strategies) == 1:
                select.append(next(iter(strategies.values())).select + "::{} AS {}".format(data_type, field_name))
            elif len(strategies) == 0:
                select.append("NULL::{} AS {}".format(data_type, field_name))
            else:
                s = '(CASE\n'
                for name, strategy in strategies.items():
                    s += ' WHEN {bridge} IN ({ids}) THEN ({s})::{data_type}\n'.format(
                        bridge=bridge_ref,
                        ids=', '.join([str(val) for val in strategy.bridge_ids]),
                        s=strategy.select,
                        data_type=data_type,
                    )
                s += 'END)::{} AS {}'.format(data_type, field_name)
                select.append(s)

        sql += ',\n\t'.join(select) + '\n'
        sql += 'FROM __unison_source\n'
        for join in joins:
            if not join.inner:
                sql += 'LEFT '
            sql += 'JOIN {} AS {} ON {}\n'.format(join.tbl, join.alias, join.on)
        if where_filters:
            sql += 'WHERE ' + ' AND '.join(where_filters) + '\n'


        return sql

    def get_strategies(self, message: StartCreateSQLViewsWorkflow, var_name: str, bridge_ids: List[int]) -> Dict[str, 'Strategy']:
        strategies: Dict[str, Strategy] = {}
        for bridge_id, var_settings in message.automation_strategies_map.items():
            if not var_name in var_settings or not int(bridge_id) in bridge_ids:
                continue
            setting: Dict[str, str] = var_settings[var_name]

            strategy_name: str = 'conceptId' if setting['valueMappingType'] == 'conceptId' else setting['automationStrategy']
            if not strategy_name in strategies:
                strategies[strategy_name] = Strategy(strategy_name)
            strategies[strategy_name].bridge_ids.append(bridge_id)
        return strategies

    def get_bridge_union_serial_fields(self, message: StartCreateSQLViewsWorkflow, val: Dict[str, any]) -> Optional[Dict[str, Dict[str, any]]]:
        """Fields numbered by serial strategy (var_name => field). None when per-bridge UNION is not applicable."""
        serial_fields = {}
        for var_name, field in val['fieldsMap'].items():
            names = set(self.get_strategies(message, var_name, val['bridgeIds']).keys())
            if 'serial' not in names:
                continue
            if len(names) > 1:
                # cut-corner: serial mixed with other strategies in one field falls back to the single-query view — upgrade: number serial bridges in the outer query by bridge id
                return None
            serial_fields[var_name] = field
        return serial_fields

    def build_bridge_union_sql(
            self,
            message: StartCreateSQLViewsWorkflow,
            table_name: str,
            val: Dict[str, any],
            serial_fields: Dict[str, Dict[str, any]],
    ) -> str:
        """
        Same rows as build_view_sql over the multi-bridge source, but UNION ALL is on top:
        each bridge is a plain query with its own constant strategies (no CASE by bridge, no union subquery to join).
        DISTINCT per bridge equals the global one, as rows of different bridges never coincide (__bridge_id differs).
        Serial numbering has to span all bridges, so it is computed over the union.
        Source CTEs are shared by all bridges and declared once on top, so they are not evaluated per bridge.
        """
        branches: List[str] = []
        ctes: Dict[str, str] = {}
        for bridge_query in val['bridgeQueries']:
            bridge_id = int(bridge_query['bridgeId'])
            query = CohortDefinition(bridge_query['query'])
            query.limit = None
            sql_source, query_ctes = self.schema.build_cohort_definition_sql_query_internal(
                VariableMapper(query.fields),
                query,
                False,
            )
            ctes.update(query_ctes)
            branches.append('(\n{}\n)'.format(self.build_view_sql(
                message,
                table_name,
                val['fieldsMap'],
                [bridge_id],
                self.schema.transform_sql_to_specific_database(sql_source),
                column=lambda var_name: '"{}"'.format(escape_string(var_name.split('.', 1)[-1])),
                bridge_ref=str(bridge_id),
                raw_vars=set(serial_fields.keys()),
            )))
        sql = '\nUNION ALL\n'.join(branches)
        if serial_fields:
            select: List[str] = []
            for var_name, field in val['fieldsMap'].items():
                if var_name in serial_fields:
                    select.append('row_number() over (order by {name})::{data_type} AS {name}'.format(
                        name=field['name'],
                        data_type=get_field_type(field['type']),
                    ))
                else:
                    select.append(field['name'])
            sql = 'SELECT\n\t{}\nFROM (\n{}\n) __unison_bridges\n'.format(',\n\t'.join(select), sql)

        cte_sql = self.schema.get_cte_sql(ctes)
        return '{}\n{}'.format(cte_sql, sql) if cte_sql else sql

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

        def chunks(iterable, size):
            for i in range(0, len(iterable), size):
                yield iterable[i:i + size]

        items = list(str_to_int.map.keys())

        for chunk in chunks(items, 1000):
            params = {"v{}".format(i): s for i, s in enumerate(chunk)}
            values_sql = ", ".join("(:v{})".format(i) for i in range(len(chunk)))

            sql = f"""
                SELECT get_or_create_number(v.val)
                FROM (VALUES {values_sql}) AS v(val)
            """

            schema.execute_sql_params(sql, params)

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
        sql = (
            "INSERT INTO {}.__semantic_mapping "
            "VALUES (:bridge_id, :field_name, :source_value, :mapped_value)".format(views_schema)
        )
        schema.execute_sql_params(sql, to_insert)



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
    bridge_ids: List[int]
    select: str
    def __init__(self, name: str):
        self.name = name
        self.bridge_ids = []
