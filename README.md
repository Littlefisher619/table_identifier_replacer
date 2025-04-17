# Table Identifier Replacer

A Python utility for replacing table identifiers in Spark SQL queries. This tool is particularly useful for modifying table references in SQL queries while preserving the query structure and handling various table identifier formats.

## Features

- Replace table identifiers in Spark SQL queries
- Support for different table reference formats:
  - `db.table`
  - `catalog.db.table`
  - Backtick-quoted identifiers
- Preserves query structure and formatting
- Configurable table identifier replacement logic
- Detailed logging for debugging

## Installation

Just Copy the `table_identifier_replacer.py` file to your project

## Usage

```python
from table_identifier_replacer import TableIdentifierReplacer

# Define a handler function that processes table identifiers
def table_handler(catalog, db, table_name):
    # Example: Add a prefix to all table names
    return catalog, db, f"new_prefix_{table_name}"

# Create a replacer instance
replacer = TableIdentifierReplacer(table_handler)

# Replace table identifiers in a SQL query
sql = "SELECT * FROM db1.table1 JOIN db2.table2 ON table1.id = table2.id"
new_sql = replacer.replace(sql)
print(new_sql)
```

## Handler Function

The handler function should accept three parameters:
- `catalog`: The catalog name (if present)
- `db`: The database name
- `table_name`: The table name

And return a tuple of three values:
- New catalog name (or None to keep original)
- New database name (or None to keep original)
- New table name (or None to keep original)

## Example

```python
def table_handler(catalog, db, table_name):
    # Example: Move all tables to a new database
    return catalog, "new_database", table_name

replacer = TableIdentifierReplacer(table_handler)
sql = "SELECT * FROM old_db.table1"
new_sql = replacer.replace(sql)
# Result: "SELECT * FROM new_database.table1"
```

## Development

### Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Testing

```bash
pip install -r requirements.dev.txt
python -m pytest tests/
```

## Dependencies

- sqlglot: For SQL parsing and transformation
- logging: For debugging and error tracking

## License

[Add your license here]

## Contributing

[Add contribution guidelines here] 
