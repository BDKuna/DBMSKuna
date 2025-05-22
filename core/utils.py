import struct, os, sys
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
from core.schema import DataType, Column


def calculate_record_format(columns: list[Column]):
    fmt = ""
    for col in columns:
        if col.data_type == DataType.INT:
            fmt += "i"
        elif col.data_type == DataType.FLOAT:
            fmt += "f"
        elif col.data_type == DataType.VARCHAR:
            fmt += f"{col.varchar_length}s"
        elif col.data_type == DataType.BOOL:
            fmt += "?"
        else:
            raise NotImplementedError(f"Unsupported type {col.data_type}")
    return fmt

def get_empty_value(column: Column):
    if column.data_type == DataType.INT:
        return -1
    elif column.data_type == DataType.FLOAT:
        return -1.0
    elif column.data_type == DataType.VARCHAR:
        return ""  # representamos vacío como string vacío
    elif column.data_type == DataType.BOOL:
        return False
    else:
        raise NotImplementedError(f"Unsupported type {column.data_type}")

def get_min_value(column: Column):
    if column.data_type == DataType.INT:
        return -10**18
    elif column.data_type == DataType.FLOAT:
        return -10**18
    elif column.data_type == DataType.VARCHAR:
        return ""  # representamos vacío como string vacío
    elif column.data_type == DataType.BOOL:
        return False
    else:
        raise NotImplementedError(f"Unsupported type {column.data_type}")

def get_max_value(column: Column):
    if column.data_type == DataType.INT:
        return 10**18
    elif column.data_type == DataType.FLOAT:
        return 10**18
    elif column.data_type == DataType.VARCHAR:
        return chr(0x10FFFF) * 10  # representamos vacío como string vacío
    elif column.data_type == DataType.BOOL:
        return True
    else:
        raise NotImplementedError(f"Unsupported type {column.data_type}")


def calculate_column_format(column: Column):
    if column.data_type == DataType.INT:
        return "i"
    elif column.data_type == DataType.FLOAT:
        return "f"
    elif column.data_type == DataType.VARCHAR:
        return f"{column.varchar_length}s"
    elif column.data_type == DataType.BOOL:
        return "?"
    else:
        raise NotImplementedError(f"Unsupported type {column.data_type}")

def pad_str(s:str, length:int):
    return s.encode().ljust(length, b'\x00')


DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'tables')

from enum import Enum, auto

class IndexType(Enum):
    AVL = auto()
    ISAM = auto()
    HASH = auto()
    BTREE = auto()
    RTREE = auto()
    SEQ = auto()
    NONE = auto()


def get_table_file_path(table_name: str, filename: str) -> str:
    table_dir = os.path.join(DATA_DIR, table_name)
    os.makedirs(table_dir, exist_ok=True)
    return os.path.join(table_dir, filename)

def get_record_file_path(table_name: str) -> str:
    return get_table_file_path(table_name, f"{table_name}.dat")

def get_index_file_path(table_name: str, column_name: str, index_type: IndexType) -> str:
    index_name = index_type.name.lower()  # e.g., BTREE → btree
    return get_table_file_path(table_name, f"{table_name}_{column_name}_{index_name}.dat")
