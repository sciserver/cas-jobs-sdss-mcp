# This is derived from:
# https://github.com/nicknochnack/BuildMCPServer/blob/main/server.py
import pandas as pd
from mcp.server.fastmcp import FastMCP

from cas_sdss_mcp.util import find_package_location

db_data = pd.read_parquet(find_package_location() / "data/database_names.parquet")
data = pd.read_parquet(find_package_location() / "data/tbls_cols.parquet")
fn_data = pd.read_parquet(find_package_location() / "data/x_match_fns.parquet")

# Server created
mcp = FastMCP("cas-sdss-mcp")


@mcp.tool()
def get_database_names() -> list[tuple[str, str]]:
    """This function returns the list of database names available for querying SDSS data.

    This returns a list of strings containing the following fields:
    - catalog_name the name of the catalog, can be used on the
    - summary a brief summary of the catalog

    Returns:
        list[str]: A list of database names.
    """
    db_data.fillna("", inplace=True)

    return db_data.loc[:, ["catalog_name", "summary"]].apply(tuple, axis=1).tolist()


@mcp.tool()
def get_database_description(database_name: str) -> tuple[str, str]:
    """This function returns the description of a given database.

    Args:
        database_name (str): The name of the database.

    Returns:
        tuple[str,str]: The summary and further remarks for the database.
    """
    descriptions = db_data.loc[db_data["catalog_name"] == database_name, ["summary", "remarks"]].drop_duplicates()
    descriptions.fillna("", inplace=True)
    if not descriptions.empty:
        return tuple(descriptions.iloc[0].values.tolist())
    else:
        return f"Error: Database '{database_name}' not found."


@mcp.tool()
def get_table_names(database_name: str) -> list[str]:
    """This tool returns the list of table names with descriptions for querying SDSS data.

    It returns the table names in the form of [Database].[Table].

    Returns:
        list[str]: A list of table names.
    """
    cat_tbl_name = (
        data.loc[data["catalog_name"] == database_name, ["table_name", "catalog_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
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


@mcp.tool()
def get_functions() -> list[tuple[str, str]]:
    """This tool returns the list of cross-match functions available for querying SDSS data.

    Returns:
        list[tuple[str, str]]: A list of function names and their descriptions.
    """
    return fn_data.loc[:, ["function_name", "description"]].apply(tuple, axis=1).tolist()


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
