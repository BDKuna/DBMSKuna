import os, sys
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
    Manager = DBManager()
    pk = 0 # para insertar valores  de primary key seguidos
    indices = [IndexType.BTREE, IndexType.AVL, IndexType.HASH]

    tipos = [DataType.INT, DataType.FLOAT, DataType.VARCHAR]

    tabla = []
    columnas = []
    data = []

    def tearDown(self):
        self.Manager.drop_table("test")
        self.tabla = []
        self.columnas = []
        self.data = []
        self.pk = 0

    def test_insert(self):
        self.columnas = []
        self.data = []
        self.pk = 0
        self.tabla = []
        def insert():
            # INSERT INTO test (col1, col2, col3, col4, col5, col6, col7, col8) VALUES (1, 'a', 1.0, 2, 'b', 2.0, 3, 'c')
            col_name = [f'col{i}' for i in range(1, len(self.columnas) + 1)]
            col_values = create_values()
            self.data.append(col_values)
            self.Manager.insert("test", col_values, col_name)

        def test_all_inserted():
            # SELECT * FROM test
            schema = SelectSchema("test", condition_schema=ConditionSchema(), all = True)
            result = self.Manager.select(schema)
            self.assertEqual(result['columns'], [i.name for i in self.columnas])
            self.assertEqual(len(result['records']), len(self.data))
            for i, record in enumerate(result['records']):
                 self.assertEqual(record, self.data[i], f"Error en la fila {i}: {record} != {self.data[i]}")

        def create_values():
            r = []
            for i in self.columnas:
                if i.is_primary:
                    # primary key
                    r.append(self.pk)
                    self.pk += 1
                elif i.data_type == DataType.INT:
                    r.append(random.randint(0, 1000))
                elif i.data_type == DataType.FLOAT:
                    r.append(round(random.random(),6))
                elif i.data_type == DataType.VARCHAR:
                    #string de longitud 10
                    r.append(''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=10)))
            return r

        def create_table():
            # CREATE TABLE(
            #     col1 INT PRIMARY KEY INDEX BTREE,
            #     col2 VARCHAR(10) INDEX BTREE,
            #     col3 FLOAT INDEX BTREE,
            #     col3 INT INDEX AVL,
            #     col4 VARCHAR(10) INDEX AVL,
            #     col5 FLOAT INDEX AVL,
            #     col6 INT INDEX HASH,
            #     col7 VARCHAR(10) INDEX HASH,
            #     col8 FLOAT INDEX HASH,
            #)
            i = 1
            for idx in self.indices:
                for tipo in self.tipos:
                    if tipo == DataType.VARCHAR:
                        # VARCHAR tiene longitud
                        self.columnas.append(Column(f'col{i}', data_type=tipo, is_primary=False, index_type=idx, varchar_length=10))
                    else:
                        # otros tipos no tienen longitud
                        self.columnas.append(Column(f'col{i}', data_type=tipo, is_primary=False, index_type=idx))
                    i += 1

            # primary key
            self.columnas[0] = Column('col1', data_type=DataType.INT, is_primary=True, index_type=IndexType.BTREE)
            print([i.name for i in self.columnas])

            # tabla de prueba
            tabla = TableSchema("test", self.columnas)
            self.Manager.create_table(tabla)

            self.assertEqual(self.Manager.get_table_schema("test").table_name, tabla.table_name)  # add assertion here
            for num, i in enumerate(self.Manager.get_table_schema("test").columns):
                self.assertEqual(i.name, tabla.columns[num].name)
                self.assertEqual(i.data_type, tabla.columns[num].data_type)
                self.assertEqual(i.is_primary, tabla.columns[num].is_primary)
                self.assertEqual(i.index_type, tabla.columns[num].index_type)
                self.assertEqual(i.varchar_length, tabla.columns[num].varchar_length)

        create_table()
        for i in range(100):
            insert()
        test_all_inserted()

    def test_insert_repeated(self):
        self.columnas = []
        self.data = []
        self.pk = 0
        self.tabla = []
        def insert():
            # INSERT INTO test (col1, col2, col3, col4, col5, col6, col7, col8) VALUES (1, 'a', 1.0, 2, 'b', 2.0, 3, 'c')
            col_name = [f'col{i}' for i in range(1, len(self.columnas) + 1)]
            col_values = create_values()
            self.data.append(col_values)
            self.Manager.insert("test", col_values, col_name)

        def test_all_inserted():
            # SELECT * FROM test
            schema = SelectSchema("test", condition_schema=ConditionSchema(), all = True)
            result = self.Manager.select(schema)
            self.assertEqual(result['columns'], [i.name for i in self.columnas])
            self.assertEqual(len(result['records']), len(self.data))
            for i, record in enumerate(result['records']):
                 self.assertEqual(record, self.data[i], f"Error en la fila {i}: {record} != {self.data[i]}")

        def create_values():
            r = []
            for i in self.columnas:
                if i.is_primary:
                    # primary key
                    r.append(self.pk)
                    self.pk += 1
                elif i.data_type == DataType.INT:
                    r.append(random.randint(0, 40))
                elif i.data_type == DataType.FLOAT:
                    r.append(round(random.random(),3))
                elif i.data_type == DataType.VARCHAR:
                    #string de longitud 10
                    r.append(''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=3)))
            return r

        def create_table():
            # CREATE TABLE(
            #     col1 INT PRIMARY KEY INDEX BTREE,
            #     col2 VARCHAR(10) INDEX BTREE,
            #     col3 FLOAT INDEX BTREE,
            #     col3 INT INDEX AVL,
            #     col4 VARCHAR(10) INDEX AVL,
            #     col5 FLOAT INDEX AVL,
            #     col6 INT INDEX HASH,
            #     col7 VARCHAR(10) INDEX HASH,
            #     col8 FLOAT INDEX HASH,
            #)
            i = 1
            for idx in self.indices:
                for tipo in self.tipos:
                    if tipo == DataType.VARCHAR:
                        # VARCHAR tiene longitud
                        self.columnas.append(Column(f'col{i}', data_type=tipo, is_primary=False, index_type=idx, varchar_length=10))
                    else:
                        # otros tipos no tienen longitud
                        self.columnas.append(Column(f'col{i}', data_type=tipo, is_primary=False, index_type=idx))
                    i += 1

            # primary key
            self.columnas[0] = Column('col1', data_type=DataType.INT, is_primary=True, index_type=IndexType.BTREE)
            print([i.name for i in self.columnas])

            # tabla de prueba
            tabla = TableSchema("test", self.columnas)
            self.Manager.create_table(tabla)

            self.assertEqual(self.Manager.get_table_schema("test").table_name, tabla.table_name)  # add assertion here
            for num, i in enumerate(self.Manager.get_table_schema("test").columns):
                self.assertEqual(i.name, tabla.columns[num].name)
                self.assertEqual(i.data_type, tabla.columns[num].data_type)
                self.assertEqual(i.is_primary, tabla.columns[num].is_primary)
                self.assertEqual(i.index_type, tabla.columns[num].index_type)
                self.assertEqual(i.varchar_length, tabla.columns[num].varchar_length)

        create_table()
        for i in range(100):
            insert()
        test_all_inserted()

if __name__ == '__main__':
    unittest.main()