import pytest

from table_identifier_replacer import TableIdentifierReplacer


def test_simple_table_replacement():
    def handler(catalog, db, table):
        return (catalog, "new_db", table)

    replacer = TableIdentifierReplacer(handler)
    sql = "SELECT * FROM db.table"
    expected = "SELECT * FROM new_db.table"
    assert replacer.replace(sql) == expected


def test_quoted_identifiers():
    def handler(catalog, db, table):
        return (catalog, "new_db", table)

    replacer = TableIdentifierReplacer(handler)
    sql = "SELECT * FROM `db`.`table`"
    expected = "SELECT * FROM `new_db`.`table`"
    assert replacer.replace(sql) == expected


def test_catalog_db_table():
    def handler(catalog, db, table):
        return ("new_catalog", "new_db", table)

    replacer = TableIdentifierReplacer(handler)
    sql = "SELECT * FROM catalog.db.table"
    expected = "SELECT * FROM new_catalog.new_db.table"
    assert replacer.replace(sql) == expected


def test_multiple_tables():
    def handler(catalog, db, table):
        return (catalog, "new_db", table)

    replacer = TableIdentifierReplacer(handler)
    sql = "SELECT * FROM db1.table1 JOIN db2.table2 ON db1.table1.id = db2.table2.id"
    expected = "SELECT * FROM new_db.table1 JOIN new_db.table2 ON new_db.table1.id = new_db.table2.id"
    assert replacer.replace(sql) == expected
