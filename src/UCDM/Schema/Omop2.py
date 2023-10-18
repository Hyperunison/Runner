import json
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base

from src.UCDM.Schema.Omop import Omop, SQLQuery, VariableMapper

Base = declarative_base()


class Omop2(Omop):
    def resolve_cohort_definition(self, sql: str):
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        return result

    def build_cohort_definition_sql_query(self, where, export, distribution: bool) -> str:
        logging.info("Cohort request got: {}".format(json.dumps(where)))
        query = SQLQuery()
        mapper = VariableMapper()

        for exp in where:
            query.conditions.append(self.build_sql_expression(exp, query, mapper))

        if len(query.conditions) > 0:
            where = "(" + (")\nAND (".join(query.conditions)) + ")"
        else:
            where = "true"

        select_array: list[str] = []
        for exp in export:
            alias = exp['as'] if 'as' in exp else  query.select[exp['name']]
            query.select[alias] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[alias], alias))

        select_string = ", ".join(select_array)

        sql = "SELECT {}, ".format(select_string)

        if distribution:
            sql += " count(distinct patient.person_id) as count\n"
        else:
            sql += mapper.convert_var_name("patient_id")+"\n"

        sql +="FROM(\n" + \
              "   SELECT person_id, year_of_birth, birth_datetime, \n" + \
              "   (SELECT concept.concept_code FROM concept WHERE concept_id = gender_concept_id) AS gender,\n" + \
              "   (SELECT concept.concept_code FROM concept WHERE concept_id = ethnicity_concept_id) AS ethnicity,\n" + \
              "   (SELECT concept.concept_code FROM concept WHERE concept_id = race_concept_id) AS race\n" + \
              "   FROM person\n" + \
              ") as patient\n"

        for j in query.joins:
            sql += "JOIN {} as {} ON {} \n".format(j.table, j.alias, j.condition)

        sql += "WHERE {}\n".format(where)

        if distribution:
            sql += "GROUP BY {} \n".format(", ".join(map(str, range(1, len(select_array) + 1)))) + \
               "HAVING COUNT(*) >= {}\n".format(self.min_count) + \
               "ORDER BY {}\n".format(", ".join(map(str, range(1, len(select_array) + 1))))

        return sql

