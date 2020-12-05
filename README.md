# sql-generator

This tool lets you generate postgresql insert statements for common data types.  
It's pretty minimalistic at the moment but offers support for basic statements 
and foreign key relations.

In addition to that, `sql-generator` supports custom generators for table columns
and data types.

## Usage

```python
from generator import Generator, write_results_to_file
import psycopg2

# Custom converter for all columns or data types with
# `custom_a` as name.
def gen_column_a(column):
    return f"My data type is {column.data_type}!"

conn = psycopg2.connect("your dsn")
gen = Generator(conn, custom_converters={"column_a": gen_column_a})
# Amount of inserts per table.
amounts = {"table_a": 50, "table_b": 25, "table_c": 100}
generated_statements: dict = gen.generate_table_data_for_all(amounts)
# Write generated statements to file.
# You can optionally truncate your db with `should_truncate=True`.
write_results_to_file(generated_statements, dest="your_output.sql")
```
