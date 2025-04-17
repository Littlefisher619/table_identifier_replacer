import pytest
import sqlglot
from sqlglot.dialects import Spark

from table_identifier_replacer import TableIdentifierReplacer


def normalize_sql(sql: str) -> str:
    """Normalize SQL formatting for comparison."""
    # Parse and regenerate SQL to normalize formatting
    try:
        expr = sqlglot.parse_one(sql, dialect=Spark)
        # Generate SQL with consistent formatting
        return expr.sql(dialect=Spark, normalize=True)
    except:
        return sql.strip()


def get_handler():
    def handler(catalog, db, table_name):
        if db is None:
            return catalog, db, table_name
        return catalog, f"new_{db}", table_name

    return handler


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT * FROM table",
            "SELECT * FROM table",
        ),
        (
            "SELECT * FROM db.table",
            "SELECT * FROM new_db.table",
        ),
    ],
    ids=["simple table without database", "database-qualified table"],
)
def test_simple_table_replacement(sql, expected):
    replacer = TableIdentifierReplacer(get_handler())
    result = replacer.replace(sql)
    assert normalize_sql(result) == normalize_sql(expected)


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT * FROM `table`",
            "SELECT * FROM `table`",
        ),
        (
            "SELECT * FROM `db`.`table`",
            "SELECT * FROM `new_db`.`table`",
        ),
        (
            "SELECT * FROM `catalog`.`db`.`table`",
            "SELECT * FROM `catalog`.`new_db`.`table`",
        ),
    ],
    ids=[
        "quoted table name",
        "quoted database and table",
        "quoted catalog, database and table",
    ],
)
def test_quoted_identifiers(sql, expected):
    replacer = TableIdentifierReplacer(get_handler())
    result = replacer.replace(sql)
    assert normalize_sql(result) == normalize_sql(expected)


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
            "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
        ),
        (
            "WITH cte AS (SELECT * FROM t) SELECT * FROM cte LEFT JOIN my_db.my_table AS t ON cte.id = t.id",
            "WITH cte AS (SELECT * FROM t) SELECT * FROM cte LEFT JOIN new_my_db.my_table AS t ON cte.id = t.id",
        ),
        (
            "WITH `cte` AS (SELECT * FROM `t`) SELECT * FROM `cte` LEFT JOIN `my_db`.`my_table` AS t ON `cte`.id = t.id",
            "WITH `cte` AS (SELECT * FROM `t`) SELECT * FROM `cte` LEFT JOIN `new_my_db`.`my_table` AS t ON `cte`.id = t.id",
        ),
    ],
    ids=["simple CTE", "CTE with table reference", "CTE with quoted identifiers"],
)
def test_cte_replacement(sql, expected):
    replacer = TableIdentifierReplacer(get_handler())
    result = replacer.replace(sql)
    assert normalize_sql(result) == normalize_sql(expected)


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT * FROM (SELECT * FROM x.t) AS subq",
            "SELECT * FROM (SELECT * FROM new_x.t) AS subq",
        ),
        (
            "SELECT * FROM (SELECT * FROM `x`.`t`) AS `subq`",
            "SELECT * FROM (SELECT * FROM `new_x`.`t`) AS `subq`",
        ),
    ],
    ids=["subquery with table reference", "subquery with quoted identifiers"],
)
def test_subquery_replacement(sql, expected):
    replacer = TableIdentifierReplacer(get_handler())
    result = replacer.replace(sql)
    assert normalize_sql(result) == normalize_sql(expected)


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            """WITH cte AS (SELECT * FROM t1) SELECT cte.*, t2.name, t3.address FROM cte LEFT JOIN db1.t2 ON cte.id = t2.id LEFT JOIN db2.t3 ON t2.id = t3.id WHERE t2.name IN (SELECT name FROM db3.t4)""",
            """WITH cte AS (SELECT * FROM t1) SELECT cte.*, t2.name, t3.address FROM cte LEFT JOIN new_db1.t2 ON cte.id = t2.id LEFT JOIN new_db2.t3 ON t2.id = t3.id WHERE t2.name IN (SELECT name FROM new_db3.t4)""",
        ),
        (
            """WITH `cte` AS (SELECT * FROM `t1`) SELECT `cte`.*, `t2`.`name`, `t3`.`address` FROM `cte` LEFT JOIN `db1`.`t2` ON `cte`.`id` = `t2`.`id` LEFT JOIN `db2`.`t3` ON `t2`.`id` = `t3`.`id` WHERE `t2`.`name` IN (SELECT `name` FROM `db3`.`t4`)""",
            """WITH `cte` AS (SELECT * FROM `t1`) SELECT `cte`.*, `t2`.`name`, `t3`.`address` FROM `cte` LEFT JOIN `new_db1`.`t2` ON `cte`.`id` = `t2`.`id` LEFT JOIN `new_db2`.`t3` ON `t2`.`id` = `t3`.`id` WHERE `t2`.`name` IN (SELECT `name` FROM `new_db3`.`t4`)""",
        ),
    ],
    ids=[
        "complex query with multiple joins and subqueries",
        "complex query with quoted identifiers",
    ],
)
def test_complex_queries(sql, expected):
    replacer = TableIdentifierReplacer(get_handler())
    result = replacer.replace(sql)
    assert normalize_sql(result) == normalize_sql(expected)


def get_catalog_handler():
    def handler(catalog, db, table_name):
        if db is None:
            return catalog, db, table_name
        # Add catalog if not present, modify if present
        new_catalog = "new_catalog" if catalog is None else f"new_{catalog}"
        return new_catalog, f"new_{db}", table_name

    return handler


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            """SELECT *
FROM db.table
WHERE id > 10
""",
            """SELECT *
FROM new_db.table
WHERE id > 10""",
        ),
        (
            """WITH cte AS (
    SELECT *
    FROM db1.table1
)
SELECT cte.*, t2.name
FROM cte
LEFT JOIN db2.table2 AS t2
    ON cte.id = t2.id""",
            """WITH cte AS (
    SELECT *
    FROM new_db1.table1
)
SELECT cte.*, t2.name
FROM cte
LEFT JOIN new_db2.table2 AS t2
    ON cte.id = t2.id""",
        ),
    ],
    ids=[
        "simple multiline query",
        "complex multiline query with CTE",
    ],
)
def test_multiline_queries(sql, expected):
    replacer = TableIdentifierReplacer(get_handler())
    result = replacer.replace(sql)
    # Compare normalized versions
    assert normalize_sql(result) == normalize_sql(expected)


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT * FROM catalog.db.table",
            "SELECT * FROM new_catalog.new_db.table",
        ),
        (
            "SELECT * FROM db.table",
            "SELECT * FROM new_catalog.new_db.table",
        ),
        (
            "SELECT * FROM `catalog`.`db`.`table`",
            "SELECT * FROM `new_catalog`.`new_db`.`table`",
        ),
        (
            "SELECT * FROM `db`.`table`",
            "SELECT * FROM new_catalog.`new_db`.`table`",
        ),
    ],
    ids=[
        "catalog modification",
        "catalog addition",
        "quoted catalog modification",
        "quoted catalog addition",
    ],
)
def test_catalog_handling(sql, expected):
    replacer = TableIdentifierReplacer(get_catalog_handler())
    result = replacer.replace(sql)
    assert normalize_sql(result) == normalize_sql(expected)
