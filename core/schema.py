from enum import Enum, auto
import os, sys
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
from core.conditionschema import ConditionSchema

class DataType(Enum):
    INT = auto()
    FLOAT = auto()
    VARCHAR = auto()
    DATE = auto()
    BOOL = auto()

class IndexType(Enum):
    AVL = auto()
    ISAM = auto()
    HASH = auto()
    BTREE = auto()
    RTREE = auto()
    SEQ = auto()
    NONE = auto()

class Column:
    def __init__(self, name, data_type : DataType, is_primary = False, index_type = IndexType.NONE, varchar_length = -1):
        self.name = name
        self.data_type = data_type
        self.is_primary = is_primary
        self.index_type = index_type
        self.varchar_length = varchar_length

class TableSchema:
    def __init__(self, table_name: str = None, columns: list[Column] = None):
        self.table_name = table_name.lower() if table_name else None
        self.columns = columns if columns else []

    def get_primary_key(self):
        return next((col for col in self.columns if col.is_primary), None)

    def get_index_columns(self):
        return [col for col in self.columns if col.index_type != IndexType.NONE]

    def get_column_by_name(self, name: str):
        return next((col for col in self.columns if col.name == name), None)

    def __repr__(self):
        # Para asegurarnos de que la serialización sea adecuada
        return f"TableSchema(table_name={self.table_name}, columns={self.columns})"

class SelectSchema:
    def __init__(self, table_name: str = None, condition_schema: ConditionSchema = None, all : bool = None, column_list: list[str] = None):
        self.table_name = table_name
        self.condition_schema = condition_schema
        self.all = all
        self.column_list = column_list if column_list else []

class DeleteSchema:
    def __init__(self, table_name : str = None, condition_schema : ConditionSchema = None):
        self.table_name = table_name
        self.condition_schema = condition_schema