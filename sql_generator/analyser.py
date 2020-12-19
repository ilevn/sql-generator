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

import psycopg2
import psycopg2.extras


class Column:
    __slots__ = (
        "name", "nullable",
        "data_type", "max_length",
        "table_name", "default_value",
        "udt_name", "has_ref", "is_unique", "sequence"
    )

    def __init__(self, record):
        # WIP.
        for attr in record._fields:
            setattr(self, attr, getattr(record, attr))

    @property
    def is_sequence(self):
        return self.data_type in ("integer", "bigint") and self.sequence is not None

    def __str__(self):
        return f"{self.table_name}.{self.name}"


class Table:
    def __init__(self, name, columns, foreign_keys):
        self.name: str = name
        self.foreign_columns = foreign_keys
        self.columns: list[Column] = [col for col in columns if
                                      col.name not in [x.column_name for x in self.foreign_columns]]

    @property
    def num_fkeys(self):
        return len(self.foreign_columns)

    def __repr__(self):
        return f"<{self.name} - {self.num_fkeys} fkeys>"

    def __str__(self):
        return self.name


class Analyser:
    def __init__(self, connection):
        self.connection = connection

    def get_table_info(self, table) -> Table:
        columns = [Column(x) for x in self.get_columns(table)]
        foreign_columns = self._get_foreign_keys_for(table)
        return Table(table, columns, foreign_columns)

    def execute_cursor(self, stmt, args=None):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(stmt, args)
        columns = cursor.fetchall()
        # Close the cursor.
        cursor.close()
        return columns

    def get_columns(self, table_name, schema="public"):
        stmt = """WITH u_refs AS (SELECT attname
                FROM pg_attribute a
                         JOIN pg_constraint c ON a.attrelid = c.conrelid AND ARRAY [a.attnum] <@ c.conkey
                WHERE c.conrelid = %(table_name)s::regclass
                  AND c.contype = 'u')

               , refs AS (SELECT confrelid::regclass,
                                 af.attname AS fcol,
                                 conrelid::regclass,
                                 a.attname  AS col
                          FROM pg_attribute af,
                               pg_attribute a,
                               (SELECT conrelid, confrelid, conkey[i] AS conkey, confkey[i] AS confkey
                                FROM (SELECT conrelid,
                                             confrelid,
                                             conkey,
                                             confkey,
                                             GENERATE_SERIES(1, ARRAY_UPPER(conkey, 1)) AS i
                                      FROM pg_constraint
                                      WHERE contype = 'f') ss) ss2
                          WHERE af.attnum = confkey
                            AND af.attrelid = confrelid
                            AND a.attnum = conkey
                            AND a.attrelid = conrelid
                            AND confrelid::regclass = %(table_name)s::regclass
            )
            
            SELECT column_name                                          AS name,
                   is_nullable::bool                                    AS nullable,
                   data_type,
                   character_maximum_length                             AS max_length,
                   table_name,
                   column_default                                       AS default_value,
                   udt_name::regtype,
                   (SELECT column_name IN (SELECT fcol FROM refs))      AS has_ref,
                   (SELECT column_name IN (SELECT attname FROM u_refs)) AS is_unique,
                   (SELECT PG_GET_SERIAL_SEQUENCE(table_name, column_name)) AS sequence
            FROM information_schema.columns
            WHERE table_schema = %(table_schema)s
              AND table_name = %(table_name)s
            ORDER BY ordinal_position;"""

        return self.execute_cursor(stmt, {"table_schema": schema, "table_name": table_name})

    def _get_foreign_keys_for(self, table):
        stmt = """SELECT tc.constraint_name,
                  kcu.column_name,
                  ccu.table_name  AS foreign_table,
                  ccu.column_name AS foreign_column
                  FROM information_schema.table_constraints AS tc
                  JOIN information_schema.key_column_usage AS kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                  JOIN information_schema.constraint_column_usage AS ccu
                   ON ccu.constraint_name = tc.constraint_name
                   AND ccu.table_schema = tc.table_schema
                  WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_name =  %(table_name)s;"""

        return self.execute_cursor(stmt, {"table_name": table})

    def get_tables(self):
        stmt = """SELECT table_schema, table_name
                  FROM information_schema.tables
                  WHERE table_schema != 'pg_catalog'
                  AND table_schema != 'information_schema'
                  AND table_type='BASE TABLE'
                  ORDER BY table_schema, table_name;"""

        return self.execute_cursor(stmt)

    def get_table_deps(self):
        stmt = """WITH fkeys AS (
                    SELECT c.conrelid          AS table_id,
                           c_fromtable.relname AS tablename,
                           c.confrelid         AS parent_id,
                           c_totable.relname   AS parent_tablename
                    FROM pg_constraint c
                             JOIN pg_namespace n ON n.oid = c.connamespace
                             JOIN pg_class c_fromtable ON c_fromtable.oid = c.conrelid
                             JOIN pg_namespace c_fromtablens ON c_fromtablens.oid = c_fromtable.relnamespace
                             JOIN pg_class c_totable ON c_totable.oid = c.confrelid
                             JOIN pg_namespace c_totablens ON c_totablens.oid = c_totable.relnamespace
                    WHERE c.contype = 'f'
                )
                
                SELECT t.tablename,
                       ARRAY_AGG(parent_tablename) FILTER ( WHERE parent_tablename IS NOT NULL ) p_tables
                FROM pg_tables t
                         LEFT JOIN fkeys ON t.tablename = fkeys.tablename
                WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
                GROUP BY t.tablename
            ORDER BY 2 NULLS FIRST"""

        return self.execute_cursor(stmt)

    def generate_dependency_graph(self):
        nodes = {}
        for dep in self.get_table_deps():
            if dep.p_tables is None:
                nodes[dep.tablename] = set()
            else:
                nodes[dep.tablename] = set(dep.p_tables)
        return nodes
