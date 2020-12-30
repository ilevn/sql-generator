sql-generator
=============

This tool lets you generate postgresql insert and copy statements for common data types.
It's pretty minimalistic at the moment but offers support for basic statements 
and foreign key relations.

In addition to that, `sql-generator` supports custom generators for table columns
and data types.

Installation
------------

**Python 3.9 or higher is required**

To install the module, run the following command:

.. code:: sh

    # Linux/macOS
    python3 -m pip install -U sql-generator
    
    # Windows
    py -3 -m pip install -U sql-generator
    
    # Poetry
    poetry add sql-generator

Usage
-----

.. code:: py

    import logging
    import psycopg2
    from sql_generator import Generator, write_statements_as_copy, write_statements_as_insert

    logging.basicConfig(level=logging.INFO)


    # Custom generator for all columns with `custom_a` as name.
    def gen_column_a(column):
        return f"My data type is {column.data_type}!"


    conn = psycopg2.connect("your dsn")
    gen = Generator(conn, column_generators={"column_a": gen_column_a})

    # Amount of inserts per table.
    amounts = {"table_a": 50, "table_b": 25, "table_c": 100}
    generated_statements: dict = gen.generate_table_data_for_all(amounts)

    # Write generated statements as INSERTs to file.
    # You can optionally truncate your db with `should_truncate=True`.
    write_statements_as_insert(generated_statements, dest="insert_output.sql")

    # Write generated statements as COPYs to file.
    write_statements_as_copy(generated_statements, dest="copy_output.sql")
