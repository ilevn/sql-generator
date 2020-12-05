import random
from collections import defaultdict

import toposort

from analyser import Analyser
from generators import password_generator, first_name_generator, last_name_generator, text_generator, email_generator, \
    phone_generator, get_converter


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

    from datetime import datetime
    now = format(datetime.now(), "%b %d %Y at %H:%M:%S")
    with open(dest, "w") as f:
        f.write("-- GENERATED AUTOMATICALLY. DO NOT ALTER THESE MANUALLY! --\n")
        f.write(f"-- This file was generated on {now}. --\n\n")
        f.write(pre_face + "\n\n")
        f.write("\n".join(statements))


class Generator:
    def __init__(self, connection, password_tables=None, custom_converters=None):
        self.analyser = Analyser(connection)
        self.password_tables = password_tables or ()
        self.converters = {
            'password_hash': password_generator,
            'first_name': first_name_generator,
            'last_name': last_name_generator,
            'character varying': text_generator,
            'email': email_generator,
            'phone': phone_generator,
            **(custom_converters or {})
        }
        # Table references for foreign key relations.
        self.id_refs = defaultdict(list)
        self.tables = [self.analyser.get_table_info(table) for table in
                       toposort.toposort_flatten(self.analyser.generate_dependency_graph())]

    def format_insert_statement_for_row(self, table, data):
        """INSERT INTO table (...) VALUES (...)"""
        columns = ", ".join(data.keys())
        values = ", ".join(map(repr, data.values()))
        extra = ""
        if table.name in self.password_tables:
            # Display their clear-text password.
            extra = f"\n-- Clear-text password: {data['password_hash'].extra} --\n"

        return f"{extra}INSERT INTO {table.name} ({columns}) OVERRIDING SYSTEM VALUE VALUES ({values});"

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
        try:
            # Check for special converters first.
            converter = self.converters.get(column.name) or self.converters[column.data_type]
        except KeyError:
            converter = get_converter(column.data_type)

        return converter(column)

    def generate_table_data(self, table, amount=1):
        # Manually keep track of our IDs.
        i = 1
        data = []
        while i < amount + 1:
            data.append(self.generate_single_table_data(table, i))
            i += 1

        formatted_data = [self.format_insert_statement_for_row(table, d) for d in data]
        if table.has_id_column:
            formatted_data.append(f"ALTER SEQUENCE {table}_id_seq RESTART WITH {i};")

        formatted_data[-1] = formatted_data[-1] + "\n"
        return formatted_data

    def generate_table_data_for_all(self, amount_per_table):
        generated_table_data = {}
        for table in self.tables:
            amount = amount_per_table[table.name]
            generated_table_data[table.name] = self.generate_table_data(table, amount)

        total_entries = __import__("functools").reduce(lambda a, b: a + b, amount_per_table.values())
        print(f"Done - Generated {total_entries} statements for {len(self.tables)} tables!")
        return generated_table_data
