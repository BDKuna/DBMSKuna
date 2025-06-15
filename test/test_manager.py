import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from core.schema import Column, TableSchema, DataType, IndexType
from core.dbmanager import DBManager

class TestISAMSimpleString(unittest.TestCase):
    def setUp(self):
        self.db = DBManager()
        schema = TableSchema("simple_isam_str", [
            Column("id_str", data_type=DataType.VARCHAR, is_primary=True, index_type=IndexType.ISAM, varchar_length=10)
        ])
        self.schema = schema
        try:
            self.db.drop_table(schema.table_name)
        except RuntimeError:
            pass
        self.db.create_table(schema)

    def test_insert_and_search(self):
        rows = [
            ["alpha"],
            ["beta"],
            ["gamma"]
        ]
        col_names = [col.name for col in self.schema.columns]
        for row in rows:
            print(f"Inserting {row}")
            self.db.insert(self.schema.table_name, row, col_names)

        index = self.db.get_index(self.schema, "id_str")
        print("Search for id_str='beta':", index.search("beta"))
        print("Range search id_str='alpha'..'gamma':", index.rangeSearch("alpha", "gamma"))

if __name__ == "__main__":
    unittest.main()
