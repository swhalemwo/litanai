import ibis
import sys
import tempfile
import subprocess
import pandas as pd

def debugger_is_active() -> bool:
    """Return if the debugger is currently active"""
    return hasattr(sys, 'gettrace') and sys.gettrace() is not None


def lmap (fun, iterable):
    return (list(map(fun, iterable)))

def flatten_list (list_of_lists):
    return([x for xs in list_of_lists for x in xs])

def split_list (list_ts, max_sublist_len):
    return[list_ts[i:i + max_sublist_len] for i in range(0, len(list_ts), max_sublist_len)]


@ibis.udf.scalar.builtin(name="ngramDistanceCaseInsensitive")
def ngdci(a: str, b:str) -> float:
    ...

@ibis.udf.scalar.builtin(name="countSubstringsCaseInsensitive")
def count_substrings_ci(a :str, b:str) -> int:
    ...


def print_first_elements(obj, n=5):
    """Print the first n elements of a given object."""
    if isinstance(obj, list) or isinstance(obj, tuple):
        # For list or tuple, slice the first n elements
        print(obj[:n])
    elif isinstance(obj, dict):
        # For dictionary, print the first n key-value pairs
        for i, (key, value) in enumerate(obj.items()):
            if i < n:
                print(f"{key}: {value}")
            else:
                break
    elif isinstance(obj, set):
        # For set, convert to list and slice
        print(list(obj)[:n])
    elif isinstance(obj, str):
        # For string, print the first n characters
        print(obj[:n])

    elif isinstance(obj, ibis.expr.types.relations.Table):
        
        print(f"{obj.count().execute()}, {len(obj.schema())}")
        print(obj)
        
    
                    

    else:
        # Handle other types as you see fit (e.g., custom objects or non-iterables)
        print("Unsupported type or empty object")


def print_names(obj):
    if isinstance(obj, dict):
        print(obj.keys())
    elif isinstance(obj, ibis.expr.types.relations.Table):
        print(obj.info())
    elif isinstance(obj, pd.core.frame.DataFrame):
        print(obj.columns)
    else:
        print("object not yet supported")


def view_xl(data, browser_xl="libreoffice"):
    """
    Exports a DataFrame to a spreadsheet for inspection.

    Sometimes it is helpful or necessary to have a look at data in a spreadsheet format.
    
    Parameters:
        data (pd.DataFrame): DataFrame to be displayed.
        browser_xl (str): Program for opening DataFrame. Default is 'libreoffice', 
                          fast alternative is 'gnumeric'.
                          
    Returns:
        None
    """
    # Check if running in an interactive environment (e.g., Jupyter)
    # if hasattr(__builtins__, 'get_ipython'):
    #     # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        # Write the DataFrame to CSV
        data.to_csv(tmp.name, index=False)
        # Open the file with the specified program
        # if platform.system() == "Windows":
        subprocess.run([browser_xl, tmp.name])
        # else:
        #     subprocess.run([browser_xl, tmp.name])


def move_tbl_to_conn(tbl, name, conn):
    """
    Inserts ibis table (or rather, query; e.g. from sqlite) to another database backend,
    and returns the ibis table object

    Parameters:
        tbl (ibis.expr.types.relations.Table): Ibis table to be moved to Clickhouse.
        name (str): Name of the table in Clickhouse.
        conn (ibis.client.Client): Connection to a database backend.    
    Returns:
        tx (ibis.expr.types.relations.Table): Table in database backend.
    """

    if isinstance(tbl, ibis.expr.types.relations.Table):
        tbl_to_insert = tbl.execute()
        conn.create_table(name, schema = tbl.schema(), overwrite = True)
        conn.insert(name, tbl_to_insert)
    elif isinstance(tbl, pd.DataFrame):
        
        conn.create_table(name, tbl, overwrite = True)
    
    tx = conn.table(name)
    return(tx)

