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
from graphlib import TopologicalSorter as Sorter
from typing import Optional

from psycopg2.extensions import connection as con

from . import Result
from .analyser import Analyser, Table
from .data_type_generators import get_generator
from .utils import GEN_FUNC

# Type aliases.
GEN_DICT = Optional[dict[str, GEN_FUNC]]
ROW_DICT = Optional[dict[str, type]]
TABLE_DATA = tuple[dict[str, Result]]

log = logging.getLogger(__name__)


def _init_generator_class(generator: type, columns):
    if not issubclass(generator, RowGenerator):
        raise TypeError(f"Unsupported row generator {generator}")
    return generator(columns)


class RowGenerator:
    def __init__(self, columns):
        self.columns = columns

    def generate(self, curr_id) -> dict[str, Result]:
        return NotImplemented

    def compute_not_covered(self, covered):
        return {col for col in self.columns if col.name not in covered}


class Generator:
    """
    The main generator for PostgreSQL statements.
    """

    def __init__(self, connection: con, schema: str = "public", data_type_generators: GEN_DICT = None,
                 column_generators: GEN_DICT = None,
                 row_generators: ROW_DICT = None,
                 seq_access: list[str] = None
                 ):
        """
        :param connection: The psycopg2 database connection.
        :param schema: The database schema.
        :param data_type_generators: A dict of data type generators.
        :param column_generators: A dict of column generators.
        """
        self.analyser = Analyser(connection)
        self.schema = schema
        # Custom generators for data types and columns.
        self.data_type_generators = data_type_generators or {}
        self.column_generators = column_generators or {}
        self.row_generators = row_generators or {}
        self.seq_access = seq_access or []
        self._cached_generators = {}
        # Table references for foreign key relations.
        self.refs = defaultdict(list)
        self.unique_values = defaultdict(set)
        self.tables = [self.analyser.get_table_info(table, schema) for table in
                       Sorter(self.analyser.generate_dependency_graph()).static_order()]

    def _reset_state(self):
        for storage in (self.refs, self.unique_values, self._cached_generators):
            storage.clear()

    def _handle_reg_columns(self, columns, curr_id):
        col_data = {}
        for column in columns:
            col_value = Result(curr_id) if column.is_sequence else self._generate_column_data(column)
            if column.is_unique:
                # Ensure value is unique.
                while col_value in self.unique_values[str(column)]:
                    col_value = self._generate_column_data(column)
                self.unique_values[str(column)].add(col_value)
            # Add foreign key values to lookup cache.
            if column.has_ref:
                self.refs[str(column)].append(col_value)

            col_data[column.name] = col_value
        return col_data

    def _get_external(self, table_name, columns) -> RowGenerator:
        try:
            return self._cached_generators[table_name]
        except KeyError:
            if (generator := self.row_generators.get(table_name)) is not None:
                self._cached_generators[table_name] = instance = _init_generator_class(generator, columns)
                return instance

    def _generate_row_data(self, table_name, columns, curr_id) -> dict[str, Result]:
        if (row_gen := self._get_external(table_name, columns)) is not None:
            partial_data: dict = row_gen.generate(curr_id)
            # Generate data for columns that are not covered by the row generator.
            partial_data.update(self._handle_reg_columns(row_gen.compute_not_covered(partial_data.keys()), curr_id))
            return partial_data

        return self._handle_reg_columns(columns, curr_id)

    def generate_row_data(self, table: Table, curr_id: int = 1) -> dict[str, Result]:
        """
        Generate row data for each column of a table.

        :param table: The table to generate a row for.
        :param curr_id: Sequence ID of the new row.
        :return: New row data.
        """
        data = self._generate_row_data(table.name, table.columns, curr_id)
        # Also handle foreign key columns.
        for fk_column in table.foreign_columns:
            c_name = f"{fk_column.foreign_table}.{fk_column.foreign_column}"
            try:
                foreign_values = self.refs[c_name]
                assert len(foreign_values) > 0
            except (KeyError, AssertionError):
                # Oh no!
                fmt = f"FATAL: NO FOREIGN KEY ID FOR FK COLUMN {fk_column.column_name}" \
                      f" of {table} (foreign table {fk_column.foreign_table})"
                log.critical(fmt)
                exit(1)
            else:
                name = table.name + "__" + c_name
                chosen = foreign_values[curr_id - 1] if name in self.seq_access else random.choice(foreign_values)
                data[fk_column.column_name] = chosen
        return data

    def _get_column_generator(self, column):
        try:
            # Attempt to use the fully qualified table name.
            return self.column_generators[str(column)]
        except KeyError:
            return self.column_generators.get(column.name)

    def _generate_column_data(self, column):
        d_type = column.data_type.lower()
        try:
            # Check for special generators first.
            generator = self._get_column_generator(column) or self.data_type_generators[d_type]
        except KeyError:
            try:
                generator = get_generator(d_type)
            except KeyError:
                log.critical(f"Unsupported data type `{d_type}` for column `{column}`. "
                             "Please use a custom generator for this.")
                # Make pycharm happy.
                return exit(1)

        return generator(column)

    def generate_table_data(self, table: Table, amount: int = 1) -> TABLE_DATA:
        """
        Generate statements for a table.

        :param table: The specific table.
        :param amount: Number of statements to generate.
        :return: The resulting statement data for one table.
        """
        return tuple(self.generate_row_data(table, row_id) for row_id in range(1, amount + 1))

    def __get_table_name(self, table_name, ignore_schema):
        return table_name.removeprefix(self.schema + ".") if ignore_schema else table_name

    def generate_table_data_for_all(self, num_per_table: dict[str, int], ignore_schema: bool = True) -> \
            dict[Table, TABLE_DATA]:
        """
        Generate table data for all available tables in the selected database.

        :param num_per_table: Number of statements per table.
        :param ignore_schema: Whether to ignore the full qualified name of a table
                              (e.g 'a' instead of 'public.a').
        :return: The resulting statement data for all tables.
        """
        # Flush pre-existing data.
        self._reset_state()

        generated_table_data = {}
        for table in self.tables:
            # Process table name, this is important when it comes to generators.
            table_name = self.__get_table_name(table.name, ignore_schema)
            amount = num_per_table[table_name]
            generated_table_data[table] = self.generate_table_data(table, amount)

        log.info(f"Done - Generated {sum(num_per_table.values())} statements for {len(self.tables)} tables!")
        return generated_table_data
