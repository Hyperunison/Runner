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

    def resolve_cohort_definition_sql_query(self, where, export) -> str:
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
            query.select[exp['name']] = self.build_sql_expression(exp, query, mapper)
            select_array.append('{} as "{}"'.format(query.select[exp['name']], exp['name']))

        select_string = ", ".join(select_array)

        sql = "SELECT {}, count(distinct patient.person_id) as count\n".format(select_string) + \
              "FROM(\n" + \
              "   SELECT person_id, year_of_birth, birth_datetime, \n" + \
              "   (SELECT concept.concept_code FROM concept WHERE concept_id = gender_concept_id) AS gender,\n" + \
              "   (SELECT concept.concept_code FROM concept WHERE concept_id = ethnicity_concept_id) AS ethnicity,\n" + \
              "   (SELECT concept.concept_code FROM concept WHERE concept_id = race_concept_id) AS race\n" + \
              "   FROM person\n" + \
              ") as patient\n"

        for j in query.joins:
            sql += "JOIN {} as {} ON {} \n".format(j.table, j.alias, j.condition)

        sql += "WHERE {}\n".format(where) + \
               "GROUP BY {} \n".format(", ".join(map(str, range(1, len(select_array) + 1)))) + \
               "HAVING COUNT(*) >= {}\n".format(self.min_count) + \
               "ORDER BY {}\n".format(", ".join(map(str, range(1, len(select_array) + 1))))

        return sql

