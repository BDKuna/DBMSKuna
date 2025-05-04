from enum import Enum, auto

class IndexType(Enum):
    AVL = auto()
    ISAM = auto()
    HASH = auto()
    BTREE = auto()
    RTREE = auto()
    SEQ = auto()

class DataType(Enum):
    INT = auto()
    FLOAT = auto()
    VARCHAR = auto()
    DATE = auto()
    BOOL = auto()

class Column:
    def __init__(self, name: str, data_type: DataType, is_primary_key: bool, index_type: IndexType = IndexType.SEQ, varchar_length: int = -1):
        self.name = name
        self.data_type = data_type
        self.index_type = index_type
        self.is_primary_key = is_primary_key
        self.varchar_length = varchar_length

class CreateTableInstruction:
    def __init__(self):
        self.table_name: str = ""
        self.columns: list[Column] = []

    def create(self):
        print(f"Table name: {self.table_name}\nColumns:")
        for column in self.columns:
            print(f"Name: {column.name}, Data type: {column.data_type}, Is primary key?: {column.is_primary_key}, Index type: {column.index_type}")

class CreateTableBuilder:
    def __init__(self):
        self.reset()

    def reset(self):
        self.instruction: CreateTableInstruction = CreateTableInstruction()

    def set_name(self, name: str) -> "CreateTableBuilder":
        self.instruction.table_name = name
        return self

    def add_column(self, name: str, data_type: DataType, is_primary_key: bool, index_type: IndexType = IndexType.SEQ, varchar_length : int = -1) -> "CreateTableBuilder":
        self.instruction.columns.append(Column(name, data_type, is_primary_key, index_type, varchar_length))
        return self

    def get(self) -> CreateTableInstruction:
        return self.instruction
    
    def getclear(self) -> CreateTableInstruction:
        temp = self.instruction
        self.instruction = CreateTableInstruction()
        return temp

if __name__ == "__main__":
    builder = CreateTableBuilder()
    instruction = (builder
        .set_name("Productos")
        .add_column("ID", DataType.INT, True, IndexType.BTREE)
        .add_column("nombre", DataType.VARCHAR, False, IndexType.HASH, varchar_length=20)
        .add_column("precio", DataType.FLOAT, False, IndexType.ISAM)
        .add_column("cantidad", DataType.INT, False, IndexType.AVL)
        .getclear())

    instruction.create()
