import struct
from schema import DataType, Column

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

def pad_str(s:str, length:int):
    return s.encode().ljust(length, b'\x00')
