import unittest
from core.schema import Column, TableSchema, DataType, IndexType
from core.dbmanager import DBManager

class TestISAMSimple(unittest.TestCase):
    def setUp(self):
        self.db = DBManager()
        schema = TableSchema("simple_isam", [
            Column("id", data_type=DataType.INT, is_primary=True, index_type=IndexType.ISAM),
            Column("name", data_type=DataType.VARCHAR, varchar_length=10)
        ])
        self.schema = schema
        try:
            self.db.drop_table(schema.table_name)
        except RuntimeError:
            pass
        self.db.create_table(schema)

    def test_insert_and_search(self):
        # Insertar registros simples
        rows = [
            [1, "alpha"],
            [2, "beta"],
            [3, "gamma"]
        ]
        col_names = [col.name for col in self.schema.columns]
        for row in rows:
            print(f"Inserting {row}")
            self.db.insert(self.schema.table_name, row, col_names)


        # Probar búsqueda en ISAMIndex
        index = self.db.get_index(self.schema, "id")
        print("Search for id=2:", index.search(2))
        print("Range search id=1..3:", index.rangeSearch(1,3))

if __name__ == "__main__":
    unittest.main()
