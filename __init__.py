from core.schema import Column, DataType, TableSchema, IndexType
from core.record_file import Record
from indices.bplustree import BPlusTree

columns = [
    Column("id", DataType.INT, is_primary=True, index_type=IndexType.BTREE),
    Column("nombre", DataType.VARCHAR, varchar_length=30),
    Column("cantidad", DataType.INT)
]
schema = TableSchema("Productos", columns)

r = [ Record(schema, [i, "Pan", 3]) for i in range(50) ]

btree = BPlusTree(schema, columns[0])

btree.insert(r[1])
btree.insert(r[3])
btree.insert(r[2])