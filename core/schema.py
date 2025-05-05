from enum import Enum, auto

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

class Column:
    def __init__(self, name, data_type:DataType, is_primary=False, index_type=IndexType.SEQ, varchar_length=-1):
        self.name = name
        self.data_type = data_type
        self.is_primary = is_primary
        self.index_type = index_type
        self.varchar_length = varchar_length

class TableSchema:
    def __init__(self, table_name: str, columns: list[Column]):
        self.table_name = table_name.lower()
        self.columns = columns

    def get_primary_key(self):
        return next((col for col in self.columns if col.is_primary), None)

    def get_index_columns(self):
        return [col for col in self.columns if col.index_type != IndexType.SEQ]

    def get_column_by_name(self, name: str):
        return next((col for col in self.columns if col.name == name), None)

    def __repr__(self):
        # Para asegurarnos de que la serialización sea adecuada
        return f"TableSchema(table_name={self.table_name}, columns={self.columns})"
