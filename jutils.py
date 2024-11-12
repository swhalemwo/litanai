import ibis
import sys

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
        print(obj.schema())
    elif isinstance(obj, pd.core.frame.DataFrame):
        print(obj.columns)
    else:
        print("object not yet supported")
