# Table Identifier Replacer

A Python utility for replacing table identifiers in SQL queries. This tool is particularly useful for modifying table references in SQL queries while preserving the query structure and handling various table identifier formats.

## Quick Examples

Here are some simple examples of what this tool can do:

1. **Add a prefix to all table names:**
```python
def add_prefix(catalog, db, table_name):
    return catalog, db, f"new_{table_name}"

replacer = TableIdentifierReplacer(add_prefix)
sql = "SELECT * FROM db1.users"
new_sql = replacer.replace(sql)
# Result: "SELECT * FROM db1.new_users"
```

2. **Move all tables to a new database:**
```python
def change_db(catalog, db, table_name):
    return catalog, "new_database", table_name

replacer = TableIdentifierReplacer(change_db)
sql = "SELECT * FROM old_db.users JOIN old_db.orders"
new_sql = replacer.replace(sql)
# Result: "SELECT * FROM new_database.users JOIN new_database.orders"
```

3. **Handle complex queries with multiple tables:**
```python
def add_suffix(catalog, db, table_name):
    return catalog, db, f"{table_name}_2024"

replacer = TableIdentifierReplacer(add_suffix)
sql = """
    SELECT u.name, o.amount 
    FROM db1.users u 
    JOIN db1.orders o ON u.id = o.user_id
    WHERE o.date > '2024-01-01'
"""
new_sql = replacer.replace(sql)
# Result: 
# SELECT u.name, o.amount 
# FROM db1.users_2024 u 
# JOIN db1.orders_2024 o ON u.id = o.user_id
# WHERE o.date > '2024-01-01'
```

4. **Handle catalog references:**
```python
def add_catalog(catalog, db, table_name):
    return "my_catalog", db, table_name

replacer = TableIdentifierReplacer(add_catalog)
sql = "SELECT * FROM db1.users"
new_sql = replacer.replace(sql)
# Result: "SELECT * FROM my_catalog.db1.users"
```

## Features

- Replace table identifiers in SQL queries
- Support for different table reference formats:
  - `db.table`
  - `catalog.db.table`
  - Backtick-quoted identifiers
- Configurable table identifier replacement logic
- Default implementation for Spark SQL dialect
- Easy to customize for other SQL dialects

## Limitations

The current implementation has some limitations:

1. **Table Names Without Database/Catalog Context**
   - The tool will skip processing table names that appear without a database or catalog context
   - This means these tables will remain unchanged in the output query
   - This affects cases like:
     - Common Table Expressions (CTEs)
     - Table aliases in complex queries
     - References to tables in the current database context
   - Example:
     ```sql
     WITH temp_table AS (
         SELECT * FROM users  -- 'users' will be skipped (unchanged)
     )
     SELECT * FROM temp_table
     ```
   - The query will still work, but the skipped tables won't be modified by your handler

2. **Workarounds**
   - If you need to modify these tables, you can modify the code to handle them:
     ```python
     # In _process_table method, remove or modify this check:
     if db is None or name is None:
         logger.debug("Skipping table - no database or name specified")
         return table
     ```
   - However, be careful as this might affect query semantics in some cases

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

## Customizing for Different SQL Dialects

The current implementation uses Spark SQL dialect by default. To use a different SQL dialect, you can modify the code in two places:

1. In the `replace` method, change the dialect parameter:
```python
# Change from:
expression = sqlglot.parse_one(sql, dialect=Spark)

# To your desired dialect, for example MySQL:
from sqlglot.dialects import MySQL
expression = sqlglot.parse_one(sql, dialect=MySQL)
```

2. In the final SQL generation:
```python
# Change from:
return expression.sql(dialect=Spark, normalize=True, pretty=True)

# To your desired dialect:
return expression.sql(dialect=MySQL, normalize=True, pretty=True)
```

Supported dialects include:
- MySQL
- PostgreSQL
- SQLite
- Snowflake
- BigQuery
- And many more (see [SQLGlot documentation](https://github.com/tobymao/sqlglot) for full list)

## Development

### Setup

1. Clone the repository
2. Install development dependencies:
   ```bash
   pip install -r requirements.dev.txt
   ```

### Testing

```bash
python -m pytest tests/
```

## Dependencies

- sqlglot: For SQL parsing and transformation
- logging: For debugging and error tracking

