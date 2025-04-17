import logging
from typing import Callable, List, Optional, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.dialects import Spark
from sqlglot.expressions import Expression, Table

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TableIdentifierReplacer:
    """
    A class to replace table identifiers in Spark SQL queries.
    Handles table references after FROM with format db.table or catalog.db.table.
    Supports backtick-quoted identifiers.

    Args:
        handler: A function that takes (catalog, db, table_name) and returns
                (new_catalog, new_db, new_table_name). If any component should
                not be changed, return the original value for that component.
    """

    def __init__(
        self,
        handler: Callable[
            [Optional[str], Optional[str], Optional[str]],
            Tuple[Optional[str], Optional[str], Optional[str]],
        ],
    ):
        """
        Initialize the replacer with a handler function.

        Args:
            handler: Function that processes table identifiers and returns new values
        """
        self.handler = handler

    def _process_table(self, table: Table) -> Table:
        """
        Process a table reference and return a new table with modifications.

        Args:
            table: SQLGlot Table expression to process

        Returns:
            A new Table expression with modifications
        """
        # Extract components from the table expression
        catalog = table.args.get("catalog")
        db = table.args.get("db")
        name = table.args.get("this")

        logger.debug(f"Processing table: catalog={catalog}, db={db}, name={name}")
        logger.debug(f"Original SQL: {table.sql()}")

        # Skip if no database specified
        if db is None or name is None:
            logger.debug("Skipping table - no database or name specified")
            return table

        # Get original values
        orig_catalog = catalog.name if catalog else None
        orig_db = db.name if db else None
        orig_name = name.name if name else None

        logger.debug(
            f"Original values: catalog={orig_catalog}, db={orig_db}, name={orig_name}"
        )

        # Get new values from handler
        new_catalog, new_db, new_name = self.handler(orig_catalog, orig_db, orig_name)
        logger.debug(
            f"New values from handler: catalog={new_catalog}, db={new_db}, name={new_name}"
        )

        # Create a new table with modifications
        new_args = table.args.copy()
        if new_catalog is not None:
            logger.debug(f"Updating catalog from {orig_catalog} to {new_catalog}")
            new_args["catalog"] = exp.Identifier(
                this=new_catalog, quoted=catalog.quoted if catalog else False
            )
        if new_db is not None:
            logger.debug(f"Updating db from {orig_db} to {new_db}")
            new_args["db"] = exp.Identifier(this=new_db, quoted=db.quoted)
        if new_name is not None:
            logger.debug(f"Updating name from {orig_name} to {new_name}")
            new_args["this"] = exp.Identifier(this=new_name, quoted=name.quoted)

        new_table = Table(**new_args)
        logger.debug(f"Modified table SQL: {new_table.sql()}")
        return new_table

    def replace(self, sql: str) -> str:
        """
        Replace table identifiers in the SQL string.

        Args:
            sql: Input SQL string to process

        Returns:
            SQL string with table identifiers replaced
        """
        logger.debug(f"Input SQL: {sql}")
        try:
            # Parse the SQL into an AST
            expression = sqlglot.parse_one(sql, dialect=Spark)
            logger.debug(f"Parsed AST: {expression}")

            # Process all table references in the AST
            def visit(
                node: Expression, parent: Optional[Expression] = None
            ) -> Expression:
                if isinstance(node, Table):
                    logger.debug(f"Visiting table node: {node}")
                    return self._process_table(node)
                return node

            expression = expression.transform(visit)
            logger.debug(f"Modified AST: {expression}")

            # Get the transformed SQL
            return expression.sql(dialect=Spark, normalize=True, pretty=True)

        except sqlglot.errors.ParseError as e:
            logger.error(f"Failed to parse SQL: {str(e)}")
            raise ValueError(f"Failed to parse SQL: {str(e)}") from e
