import psycopg2

import psycopg2.extras


class Table:
    def __init__(self, name, columns, foreign_keys, referenced_by):
        self.name = name
        self.columns = columns
        self.foreign_keys = foreign_keys
        self.referenced_by = referenced_by
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
        referenced_by = self.get_table_references(table)
        return Table(table, columns, foreign_columns, referenced_by)

    def execute_cursor(self, stmt, args=None):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(stmt, args)
        columns = cursor.fetchall()
        # Close the cursor.
        cursor.close()
        return columns

    def get_table_references(self, table):
        stmt = """SELECT
                  (SELECT r.relname FROM pg_class r WHERE r.oid = c.conrelid) AS ref_table,
                  (SELECT array_agg(attname) FROM pg_attribute
                   WHERE attrelid = c.conrelid AND ARRAY[attnum] <@ c.conkey) AS col
                   FROM pg_constraint c
                   WHERE c.confrelid = (SELECT oid FROM pg_class WHERE relname = %(table_name)s);"""
        return self.execute_cursor(stmt, {"table_name": table})

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
