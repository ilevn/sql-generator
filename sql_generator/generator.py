"""
The MIT License (MIT)

Copyright (c) 2020 Nils T.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""
import logging
import random
from collections import defaultdict

import toposort

from .analyser import Analyser, Table
from .data_type_generators import get_generator

log = logging.getLogger(__name__)


class Generator:
    def __init__(self, connection, data_type_generators=None, column_generators=None):
        self.analyser = Analyser(connection)
        # Custom generators for data types and columns.
        self.data_type_generators = data_type_generators or {}
        self.column_generators = column_generators or {}
        # Table references for foreign key relations.
        self.refs = defaultdict(list)
        self.unique_values = defaultdict(set)
        self.tables = [self.analyser.get_table_info(table) for table in
                       toposort.toposort_flatten(self.analyser.generate_dependency_graph())]

    def _handle_reg_columns(self, columns, curr_id):
        col_data = {}
        for column in columns:
            col_value = self._generate_column_data(column)
            if column.is_sequence:
                col_value = curr_id
            elif column.is_unique:
                # Ensure value is unique.
                while col_value in self.unique_values[str(column)]:
                    col_value = self._generate_column_data(column)
                self.unique_values[str(column)].add(col_value)
            # Add foreign key values to lookup cache.
            if column.has_ref:
                self.refs[str(column)].append(col_value)

            col_data[column.name] = col_value
        return col_data

    def generate_row_data(self, table: Table, curr_id=1):
        data = self._handle_reg_columns(table.columns, curr_id)
        # Also handle foreign key columns.
        for fk_column in table.foreign_columns:
            try:
                foreign_values = self.refs[f"{fk_column.foreign_table}.{fk_column.foreign_column}"]
                assert len(foreign_values) > 0
            except (KeyError, AssertionError):
                # Oh no!
                fmt = f"FATAL: NO FOREIGN KEY ID FOR FK COLUMN {fk_column.column_name}" \
                      f" of {table} (foreign table {fk_column.foreign_table})"
                log.critical(fmt)
                exit(1)
            else:
                data[fk_column.column_name] = random.choice(foreign_values)
        return data

    def _generate_column_data(self, column):
        d_type = column.data_type.lower()
        try:
            # Check for special generators first.
            generator = self.column_generators.get(column.name) or self.data_type_generators[d_type]
        except KeyError:
            try:
                generator = get_generator(d_type)
            except KeyError:
                log.critical(f"Unsupported data type `{d_type}` for column `{column.name}`. "
                             "Please use a custom generator for this.")
                # Make pycharm happy.
                return exit(1)

        return generator(column)

    def generate_table_data(self, table, amount=1):
        data = []
        for row_id in range(1, amount + 1):
            data.append(self.generate_row_data(table, row_id))
        return data

    def generate_table_data_for_all(self, amount_per_table):
        generated_table_data = {}
        for table in self.tables:
            amount = amount_per_table[table.name]
            generated_table_data[table] = self.generate_table_data(table, amount)

        log.info(f"Done - Generated {sum(amount_per_table.values())} statements for {len(self.tables)} tables!")
        return generated_table_data
