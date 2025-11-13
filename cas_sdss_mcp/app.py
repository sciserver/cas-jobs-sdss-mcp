# This is derived from:
# https://github.com/nicknochnack/BuildMCPServer/blob/main/server.py
import pandas as pd
from mcp.server.fastmcp import FastMCP

data = pd.read_json("sqlxmatch_data.json")

# Server created
mcp = FastMCP("cas-sdss-mcp")


# Create the tool
@mcp.tool()
def get_table_names() -> list[str]:
    """This tool returns the list of table names with descriptions for querying SDSS data.

    It returns the table names in the form of [Database].[Table].

    Returns:
        list[str]: A list of table names.
    """
    cat_tbl_name = data.loc[:, ["table_name", "catalog_name"]].drop_duplicates().reset_index(drop=True)
    return cat_tbl_name.apply(lambda row: f"{row['catalog_name']}.{row['table_name']}", axis=1).tolist()


@mcp.tool()
def get_columns(table_name: str) -> list[tuple[str, str]]:
    """This tool returns the list of columns for a given table name.

    Args:
        table_name (str): The name of the table in the form of [Database].[Table].

    Returns:
        list[tuple[str, str]]: A list of column names and their descriptions.
    """
    try:
        catalog_name, tbl_name = table_name.split(".")
    except ValueError:
        return [f"Error: Table name '{table_name}' is not in the correct format. Use [Database].[Table]."]

    filtered = data[(data["table_name"] == tbl_name) & (data["catalog_name"] == catalog_name)]
    if filtered.empty:
        return [f"Error: Table '{table_name}' not found."]

    return filtered.loc[:, ["column_name", "description"]].apply(tuple, axis=1).tolist()


if __name__ == "__main__":
    mcp.run(transport="stdio")
