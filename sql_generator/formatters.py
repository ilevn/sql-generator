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

from datetime import datetime

from sql_generator.analyser import Table

__all__ = ("InsertFormatter", "write_statements_as_insert", "write_statements_as_copy", "write_statements_as")
_S = dict[Table, list[dict]]


def _format_insert_statement_for_row(table, data, *_):
    columns = ", ".join(data.keys())
    values = ", ".join(map(repr, data.values()))
    return f"INSERT INTO {table} ({columns}) OVERRIDING SYSTEM VALUE VALUES ({values});"


def _format_copy_statement_for_row(table, data, row_id, *_):
    fmt = ""
    if row_id == 0:
        columns = ", ".join(data.keys())
        fmt += f"COPY {table.name} ({columns}) FROM stdin;\n"

    values = "\t".join(str(getattr(r, "raw", r)) for r in data.values())
    return fmt + values


def _format_table(table, data, func, seq_fmt, end_pad, *args):
    row_data = []
    for row_id, row in enumerate(data):
        row_data.append((func(table, row, row_id, *args)))

    row_data[-1] = row_data[-1] + end_pad
    next_id = len(row_data) + 1
    for col in table.columns:
        # Columns could have multiple sequences.
        if col.is_sequence:
            row_data.append(seq_fmt.format(next_id=next_id, seq_name=col.sequence))
    return row_data


class InsertFormatter:
    def __init__(self, should_truncate: bool, statements: _S):
        self.should_truncate = should_truncate
        self.statements = statements

    def format_statements(self, preface: str = ""):
        formatted_data = []
        if self.should_truncate:
            table_names = self.statements.keys()
            preface += "\n".join(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;" for table in table_names)

        for table, rows in self.statements.items():
            seq_fmt = "ALTER SEQUENCE {seq_name} RESTART WITH {next_id};\n"
            data = _format_table(table, rows, _format_insert_statement_for_row, seq_fmt, end_pad="\n")
            formatted_data.extend(data)

        return preface, formatted_data


class CopyFormatter:
    def __init__(self, statements: _S):
        self.statements = statements

    @staticmethod
    def get_security_headers():
        fmt = (
            "SET statement_timeout = 0;\n"
            "SET lock_timeout = 0;\n"
            "SET idle_in_transaction_session_timeout = 0;\n"
            "SET client_encoding = 'UTF8';\n"
            "SET standard_conforming_strings = on;\n"
            "SELECT pg_catalog.set_config('search_path', '', false);\n"
            "SET check_function_bodies = false;\n"
            "SET xmloption = content;\n"
            "SET client_min_messages = warning;\n"
            "SET row_security = off;\n"
        )
        return fmt

    def format_statements(self, preface: str = ""):
        formatted_data = [self.get_security_headers()]

        for table, rows in self.statements.items():
            seq_fmt = "SELECT pg_catalog.setval('{seq_name}', {next_id}, false);\n"
            data = _format_table(table, rows, _format_copy_statement_for_row, seq_fmt, end_pad="\n\\.\n")
            formatted_data.extend(data)

        return preface, formatted_data


def _write_to_file(statements: list[str], dest="output.sql", preface: str = "") -> None:
    now = format(datetime.now(), "%b %d %Y at %H:%M:%S")
    with open(dest, "w") as f:
        f.write("/**\n")
        f.write("  GENERATED AUTOMATICALLY. DO NOT ALTER THESE MANUALLY!\n")
        f.write(f"  This file was generated on {now}. \n")
        f.write("  sql-generator (https://github.com/ilevn/sql-generator)\n")
        f.write("*/\n\n")
        f.write(preface + "\n")
        f.write("\n".join(statements))


def write_statements_as_insert(statements: _S, dest: str = "output.sql", should_truncate: bool = False) -> None:
    formatter = InsertFormatter(should_truncate, statements)
    preface, data = formatter.format_statements()
    _write_to_file(data, dest, preface)


def write_statements_as_copy(statements: _S, dest: str = "output.sql") -> None:
    formatter = CopyFormatter(statements)
    preface, data = formatter.format_statements()
    _write_to_file(data, dest, preface)


AVAILABLE_FORMATTERS = {"INSERT": write_statements_as_insert, "COPY": write_statements_as_copy}


def write_statements_as(format_, statements: _S, dest: str = "output.sql", **kwargs):
    try:
        formatter = AVAILABLE_FORMATTERS[format_]
    except KeyError as e:
        raise NotImplementedError(f"Format '{format_}' is not supported!") from e
    return formatter(statements, dest, **kwargs)
