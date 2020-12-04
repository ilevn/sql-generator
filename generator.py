from collections import defaultdict

import psycopg2

from analyser import Analyser
from generators import password_generator, first_name_generator, last_name_generator, text_generator, email_generator, \
    phone_generator, get_converter


def write_results_to_file(statements, dest="output.sql"):
    with open(dest, "w") as f:
        f.write("\n".join(statements))


def format_insert_statement_for_row(table, data):
    """INSERT INTO table (...) VALUES (...)"""
    columns = ", ".join(data.keys())
    values = ", ".join(map(repr, data.values()))
    return f"INSERT INTO {table.name} ({columns}) OVERRIDING SYSTEM VALUE VALUES ({values});"


class Generator:
    def __init__(self, connection):
        self.analyser = Analyser(connection)
        self.converters = {
            'password_hash': password_generator,
            'first_name': first_name_generator,
            'last_name': last_name_generator,
            'character varying': text_generator,
            'email': email_generator,
            'phone': phone_generator
        }
        _tables = [self.analyser.get_table_info(t.table_name) for t in self.analyser.get_tables()]
        self.tables = {table.name: table for table in _tables}
        self.id_refs = defaultdict(set)
        # covered == in id_refs

    def generate_single_table_data(self, table, curr_id=1):
        data = {}
        for column in table.columns:
            if column.name == "id":
                data["id"] = curr_id
                # Add the id to our set of references.
                self.id_refs[table.name].add(curr_id)
                continue
            data[column.name] = self.generate_column_data(column)
        return data

    def generate_column_data(self, column):
        try:
            # Check for special converters first.
            converter = self.converters.get(column.name) or self.converters[column.data_type]
        except KeyError:
            converter = get_converter(column.data_type)

        return converter(column)

    def generate_table_data(self, table, amount=1):
        data = [self.generate_single_table_data(table, i) for i in range(1, amount + 1)]
        return [format_insert_statement_for_row(table, d) for d in data]

    def get_transitive_dependencies(self, table):
        for fkey in table.foreign_keys:
            f_table = self.tables[fkey.foreign_table]


if __name__ == '__main__':
    conn = psycopg2.connect("dbname=tickify host='localhost' user='postgres' password='mypassword' port='5433'")
    g = Generator(conn)
    #write_results_to_file(g.generate_table_data(g.tables["buyer"], 5))
    print()
