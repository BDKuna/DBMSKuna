import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
import random

from core.conditionschema import ConditionSchema
from core.schema import Column, TableSchema, SelectSchema
from core.schema import DataType, IndexType

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
from core.dbmanager import DBManager

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.Manager = DBManager()
        random.seed(42)
        self.pk = 0
        self.indices = [IndexType.BTREE, IndexType.AVL, IndexType.HASH, IndexType.ISAM]
        self.tipos = [DataType.INT, DataType.FLOAT, DataType.VARCHAR]
        self.columnas = []
        self.data = []

    def tearDown(self):
        self.Manager.drop_table("test")
        self.columnas.clear()
        self.data.clear()
        self.pk = 0

    def create_table(self):
        i = 1
        for idx in self.indices:
            for tipo in self.tipos:
                if tipo == DataType.VARCHAR:
                    if idx == IndexType.ISAM:
                        continue
                    self.columnas.append(Column(f'col{i}', data_type=tipo, is_primary=False, index_type=idx, varchar_length=10))
                else:
                    self.columnas.append(Column(f'col{i}', data_type=tipo, is_primary=False, index_type=idx))
                i += 1

        self.columnas.append(Column('col10', data_type=DataType.VARCHAR, is_primary=False, index_type=IndexType.RTREE, varchar_length=20))
        # Define primary key as first column with BTREE
        self.columnas[0] = Column('col1', data_type=DataType.INT, is_primary=True, index_type=IndexType.BTREE)

        tabla = TableSchema("test", self.columnas)
        self.Manager.create_table(tabla)

        # Assertions to verify schema
        schema = self.Manager.get_table_schema("test")
        self.assertEqual(schema.table_name, tabla.table_name)
        for num, col in enumerate(schema.columns):
            self.assertEqual(col.name, tabla.columns[num].name)
            self.assertEqual(col.data_type, tabla.columns[num].data_type)
            self.assertEqual(col.is_primary, tabla.columns[num].is_primary)
            self.assertEqual(col.index_type, tabla.columns[num].index_type)
            self.assertEqual(col.varchar_length, tabla.columns[num].varchar_length)

    def create_values(self, varchar_length=10, int_range=(0,1000), float_precision=6):
        row = []
        for col in self.columnas:
            if col.is_primary:
                row.append(self.pk)
                self.pk += 1
            elif col.index_type == IndexType.RTREE:
                row.append(f'({round(random.random(),3)},{round(random.random(),3)})')
            elif col.data_type == DataType.INT:
                row.append(random.randint(*int_range))
            elif col.data_type == DataType.FLOAT:
                row.append(round(random.random(), float_precision))
            elif col.data_type == DataType.VARCHAR:
                row.append(''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=varchar_length)))
        return row

    def insert_row(self):
        col_names = [col.name for col in self.columnas]
        values = self.create_values()
        self.data.append(values)
        self.Manager.insert("test", values, col_names)

    def verify_all_inserted(self):
        schema = SelectSchema("test", condition_schema=ConditionSchema(), all=True)
        result = self.Manager.select(schema)
        self.assertEqual(result['columns'], [col.name for col in self.columnas])
        self.assertEqual(len(result['records']), len(self.data))
        for i, record in enumerate(result['records']):
            self.assertEqual(record, self.data[i], f"Mismatch at row {i}: {record} != {self.data[i]}")

    def test_insert(self):
        self.create_table()
        for _ in range(100):
            self.insert_row()
        self.verify_all_inserted()

    def test_insert_repeated(self):
        # Adjust parameters for this test
        self.create_table()
        self.pk = 0
        self.data.clear()

        # Override create_values temporarily for varchar length and value ranges
        original_create_values = self.create_values
        def create_values_override():
            return original_create_values(varchar_length=3, int_range=(0,40), float_precision=3)
        self.create_values = create_values_override

        for _ in range(100):
            self.insert_row()
        self.verify_all_inserted()

if __name__ == '__main__':
    unittest.main()