import random
from collections import defaultdict
from datetime import datetime

import toposort

from .analyser import Analyser
from .data_type_generators import get_generator


def flatten_dict_list(table_statements):
    result = []
    for values in table_statements.values():
        result.extend(values)
    return result


def write_results_to_file(statements, dest="output.sql", should_truncate=False, pre_face=""):
    if not isinstance(statements, dict):
        raise TypeError(f"statements should be a dict, not {type(statements)}.")

    if should_truncate:
        pre_face = "\n".join(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;" for table in statements)
    statements = flatten_dict_list(statements)

    now = format(datetime.now(), "%b %d %Y at %H:%M:%S")
    with open(dest, "w") as f:
        f.write("-- GENERATED AUTOMATICALLY. DO NOT ALTER THESE MANUALLY! --\n")
        f.write(f"-- This file was generated on {now}. --\n\n")
        f.write(pre_face + "\n\n")
        f.write("\n".join(statements))


class Generator:
    def __init__(self, connection, data_type_generators=None, column_generators=None, statement_prefix=""):
        self.analyser = Analyser(connection)
        self.statement_prefix = statement_prefix
        # Custom generators for data types and columns.
        self.data_type_generators = data_type_generators or {}
        self.column_generators = column_generators
        # Table references for foreign key relations.
        self.id_refs = defaultdict(list)
        self.tables = [self.analyser.get_table_info(table) for table in
                       toposort.toposort_flatten(self.analyser.generate_dependency_graph())]

    def format_sql_statement_for_row(self, table, data):
        columns = ", ".join(data.keys())
        values = ", ".join(map(repr, data.values()))
        return f"{self.statement_prefix}INSERT INTO {table.name} ({columns}) OVERRIDING SYSTEM VALUE VALUES ({values});"

    def generate_single_table_data(self, table, curr_id=1):
        data = {}
        for column in table.columns:
            if column.name == "id":
                data["id"] = curr_id
                # Add the id to our set of references.
                self.id_refs[table.name].append(curr_id)
                continue
            data[column.name] = self.generate_column_data(column)

        # This does not take unique constraints into consideration.
        for fk_column in table.foreign_keys:
            try:
                foreign_ids = self.id_refs[fk_column.foreign_table]
                assert len(foreign_ids) > 0
            except (KeyError, AssertionError):
                # Oh no!
                fmt = f"FATAL: NO FOREIGN KEY ID FOR FK COLUMN {fk_column.column_name}" \
                      f" of {table} (foreign table {fk_column.foreign_table})"
                print(fmt)
                exit(1)
            else:
                data[fk_column.column_name] = random.choice(foreign_ids)
        return data

    def generate_column_data(self, column):
        d_type = column.data_type.lower()
        try:
            # Check for special generators first.
            converter = self.column_generators.get(column.name) or self.data_type_generators[d_type]
        except KeyError:
            try:
                converter = get_generator(d_type)
            except KeyError:
                print(f"Unsupported data type `{d_type}` for column `{column.name}`.")
                print("Please use a custom generator for this.")
                # Make pycharm happy.
                return exit(1)

        return converter(column)

    def generate_table_data(self, table, amount=1):
        formatted_data = []
        for row_id in range(1, amount + 1):
            table_data = self.generate_single_table_data(table, row_id)
            formatted_data.append(self.format_sql_statement_for_row(table, table_data))

        if table.has_id_column:
            next_id = len(formatted_data) + 1
            formatted_data.append(f"ALTER SEQUENCE {table}_id_seq RESTART WITH {next_id};")

        formatted_data[-1] = formatted_data[-1] + "\n"
        return formatted_data

    def generate_table_data_for_all(self, amount_per_table):
        generated_table_data = {}
        for table in self.tables:
            amount = amount_per_table[table.name]
            generated_table_data[table.name] = self.generate_table_data(table, amount)

        print(f"Done - Generated {sum(amount_per_table.values())} statements for {len(self.tables)} tables!")
        return generated_table_data
