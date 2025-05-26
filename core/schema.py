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
    POINT = auto()

    def __str__(self):
        return self.name

class IndexType(Enum):
    AVL = auto()
    ISAM = auto()
    HASH = auto()
    BTREE = auto()
    RTREE = auto()
    BRIN = auto()
    NONE = auto()

    def __str__(self):
        return self.name

class Column:
    def __init__(self, name, data_type : DataType, is_primary = False, index_type = IndexType.NONE, varchar_length = None, index_name = None):
        self.name = name
        self.data_type = data_type
        self.is_primary = is_primary
        self.index_type = index_type
        self.index_name = index_name
        self.varchar_length = varchar_length

class TableSchema:
    def __init__(self, table_name: str = None, columns: list[Column] = None):
        self.table_name = table_name.lower() if table_name else None
        self.columns = columns if columns else []

    def error(self, error : str):
        raise RuntimeError(error)

    def get_primary_key(self):
        return next((col for col in self.columns if col.is_primary), None)

    def get_index_columns(self):
        return [col for col in self.columns if col.index_type != IndexType.NONE]

    def get_column_by_name(self, name: str):
        return next((col for col in self.columns if col.name == name), None)
    
    def get_indexes(self):
        indexes = {}
        for column in self.columns:
            indexes[column.name] = self.get_column_index_type(column)
        return indexes

    def get_column_index_type(self, column: Column):
        index_type = column.index_type
        match index_type:
            case IndexType.AVL:
                from indexes.avltree import AVLTree
                return AVLTree(self, column)
            case IndexType.ISAM:
                from indexes.ISAMtree import ISAMIndex
                return ISAMIndex(self, column)
            case IndexType.HASH:
                from indexes.EHtree import ExtendibleHashTree
                return ExtendibleHashTree(self, column)
            case IndexType.BTREE:
                from indexes.bplustree import BPlusTree
                return BPlusTree(self, column)
            case IndexType.RTREE:
                from indexes.Rtree import RTreeIndex
                return RTreeIndex(self, column)
            case IndexType.BRIN:
                pass
                # BRIN(table_schema, column)
            case IndexType.NONE:
                return None
            case _:
                self.error("invalid index type")

    def get_primary_index(self):
        column = self.get_primary_key()
        return self.get_column_index_type(column)
        
    def get_primary_key(self) -> Column:
        for column in self.columns:
            if column.is_primary:
                return column
        self.error("No primary key")


    def __repr__(self):
        # Para asegurarnos de que la serialización sea adecuada
        return f"TableSchema(table_name={self.table_name}, columns={self.columns})"

class SelectSchema:
    def __init__(self, table_name: str = None, condition_schema: ConditionSchema = None, all : bool = None, column_list: list[str] = None, order_by : str = None, asc : bool = True, limit : int = None):
        self.table_name = table_name
        self.condition_schema = condition_schema
        self.all = all
        self.column_list = column_list if column_list else []
        self.order_by = order_by
        self.asc = asc
        self.limit = limit

class DeleteSchema:
    def __init__(self, table_name : str = None, condition_schema : ConditionSchema = None):
        self.table_name = table_name
        self.condition_schema = condition_schema