from schema import TableSchema
import pickle, os
"""
class Catalog:
    def __init__(self, file_path="catalog.dat"):
        self.tables = {}
        self.file_path = file_path
        self.load_catalog()

    def create_table(self, schema: TableSchema):
        self.tables[schema.table_name] = schema
        self.save_catalog()

    def get_schema(self, table_name: str) -> TableSchema:
        return self.tables.get(table_name)

    def get_table_names(self):
        return list(self.tables.keys())

    def save_catalog(self):
        #Serializa el catálogo de tablas a un archivo.
        with open(self.file_path, "wb") as file:
            pickle.dump(self.tables, file)

    def load_catalog(self):
        #Carga el catálogo desde un archivo serializado.
        if os.path.exists(self.file_path):
            with open(self.file_path, "rb") as file:
                self.tables = pickle.load(file)
        else:
            self.tables = {}
"""
