import os
from http.client import HTTPException
from typing import *

from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout
from dotenv import load_dotenv
from fastmcp import FastMCP
from databricks.sdk import WorkspaceClient

load_dotenv()
mcp = FastMCP(name="Decision Tools")
client = WorkspaceClient(host=os.getenv("DB_SERVER_HOSTNAME"), token=os.getenv("DB_ACCESS_TOKEN"))
@mcp.tool()
async def list_tables() -> Dict[str, str]:
    """
    List tables available in the datalake.
    This must be called to fetch relevant table to match user query
    :return: List of table names and descriptions
    """
    result = client.tables.list(catalog_name=os.getenv("DB_CATALOG"), schema_name=os.getenv("DB_SCHEMA"))
    table_map = {}
    for table in result:
        table_map[table.name] = (table.comment if table.comment is not None else "")
    return table_map

@mcp.tool()
async def get_schema(table) -> Dict[str, str]:
    """
    Fetch the full table schema for the specified table.
    This must be called to get a better understanding of the
    table structure to make decisions regarding data processing
    and filtering
    :param table: Table name as string
    :return: Dict of table headers and corresponding types
    """
    result = client.tables.get(full_name=f"{os.getenv('DB_CATALOG')}.{os.getenv('DB_SCHEMA')}.{table}")
    column_map = {}
    for column in result.columns:
        column_map[column.name] = column.type_name.value
    return column_map


@mcp.tool()
async def parse_sql(commands) -> List[Any]:
    """
    This must be called to fetch relevant data from the
    selected table before sending it to visualization algorithms.
    The requested table may not have all user requested columns,
    in which case you must intuitively perform column extrapolations
    using SQL.
    Example:
    SELECT
        A,
        B,
        A / B AS C
    FROM
        SelectedTable;
    :param table: name of selected table
    :param commands: SQL formatted commands
    :return: json arrays of requested SQL data
    """
    result = client.statement_execution.execute_statement(
        statement=commands,
        warehouse_id=os.getenv("DB_WAREHOUS_ID"),
        catalog=os.getenv("DB_CATALOG"),
        schema=os.getenv("DB_SCHEMA"),
        wait_timeout='20s',
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
    )
    if result.status.state.value == "SUCCEEDED":
        return result.result.data_array
    else:
        raise HTTPException("Statement Timeout: try a shorter query")

if __name__ == "__main__":
    mcp.run(transport="http", port=8000)