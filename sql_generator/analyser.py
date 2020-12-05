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


class Table:
    def __init__(self, name, columns, foreign_keys):
        self.name = name
        self.columns = columns
        self.foreign_keys = foreign_keys
        self.has_id_column = "id" in [c.name for c in self.columns]

    @property
    def num_fkeys(self):
        return len(self.foreign_keys)

    def __repr__(self):
        return f"<{self.name} - {self.num_fkeys} fkeys>"

    def __str__(self):
        return self.name


class Analyser:
    def __init__(self, connection):
        self.connection = connection

    def get_table_info(self, table):
        columns = self.get_columns(table)
        foreign_columns = self._get_foreign_keys_for(table)
        return Table(table, columns, foreign_columns)

    def execute_cursor(self, stmt, args=None):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(stmt, args)
        columns = cursor.fetchall()
        # Close the cursor.
        cursor.close()
        return columns

    def get_columns(self, table_name):
        stmt = """SELECT column_name AS name, CASE is_nullable 
                  WHEN 'NO' THEN FALSE ELSE TRUE END AS nullable,
                  data_type, character_maximum_length, table_name
                  FROM information_schema.columns
                  WHERE table_schema = %(table_schema)s
                  AND table_name   = %(table_name)s
                  ORDER BY ordinal_position;"""

        return self.execute_cursor(stmt, {"table_schema": "public", "table_name": table_name})

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
                       array_agg(parent_tablename) FILTER ( WHERE parent_tablename IS NOT NULL ) p_tables
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
