from enum import Enum, auto
from schema import DataType, IndexType, TableSchema, Column      

class TableSchemaBuilder:
    def __init__(self):
        self.reset()

    def reset(self):
        self.schema = TableSchema()

    def set_name(self, name: str) -> "TableSchemaBuilder":
        self.schema.table_name = name
        return self

    def add_column(self, name: str, data_type: DataType, is_primary_key: bool, index_type: IndexType = IndexType.SEQ, varchar_length : int = -1) -> "TableSchemaBuilder":
        self.schema.columns.append(Column(name, data_type, is_primary_key, index_type, varchar_length))
        return self

    def get(self) -> "TableSchemaBuilder":
        return self.schema
    
    def getclear(self) -> "TableSchemaBuilder":
        temp = self.schema
        self.schema = TableSchemaBuilder()
        return temp