from core.schema import TableSchema, IndexType
from core.record_file import Record, RecordFile
from indices.avltree import AVLTree


class miniManager:
    def __init__(self, schema: TableSchema):
        self.schema = schema
        self.columns = schema.columns
        self.record_file = RecordFile(schema)
        self.indexes = {}
        for i in schema.columns:
            if i.index_type == IndexType.AVL:
                self.indexes[i.name] = AVLTree(schema, i)
            else:
                self.indexes[i.name] = None
        self.primary_key = [i for i in schema.columns if i.is_primary][0]
        self.primary_key_index = self.indexes[self.primary_key.name]

    def _search_equality(self, key: str, value) -> list[int]:
        # Buscar en el índice
        if key not in self.indexes.keys():
            raise Exception(f"Índice {key} no encontrado")
        if self.indexes[key] is None:
            raise Exception(f"Índice {key} no es un índice AVL")
        return [self.indexes[key].search(value)]

    def _search_range(self, key: str, min_value, max_value) -> list[int]:
        # Buscar en el índice
        if key not in self.indexes.keys():
            raise Exception(f"Índice {key} no encontrado")
        if self.indexes[key] is None:
            raise Exception(f"Índice {key} no es un índice AVL")
        pos = self.indexes[key].rangeSearch(min_value, max_value)
        return pos

    def insert(self, values: list):
        if len(values) != len(self.columns):
            raise Exception("El número de valores no coincide con el número de columnas")
        record = Record(self.schema, values)

        pos = self.record_file.append(record)

        #insertar los indices
        for index in self.indexes.keys():
            if self.indexes[index] is not None:
                self.indexes[index].insert(record, pos)

    def getAll(self):
        all_pos =  self.primary_key_index.getAll()
        return [self.record_file.read(pos) for pos in all_pos]

    def delete(self, records_pos: list[int]):
        # Eliminar los registros del archivo de registros
        to_delete = []
        for record_pos in records_pos:
            to_delete.append(self.record_file.read(record_pos))

        # Eliminar de los índices
        for r in to_delete:
            for j, key in enumerate(self.indexes.keys()):
                if self.indexes[key] is not None:
                    self.indexes[key].delete(r.values[j])
        # Eliminar del archivo de registros
        for record_pos in records_pos:
             self.record_file.delete(record_pos)

    def search(self, key: str, value) -> Record:
        # Buscar en el índice
        if key not in self.indexes.keys():
            raise Exception(f"Índice {key} no encontrado")
        if self.indexes[key] is None:
            raise Exception(f"Índice {key} no es un índice AVL")
        pos = self._search_equality(key, value)[0]
        return self.record_file.read(pos)

    def searchRange(self, key: str, min_value, max_value) -> list[Record]:
        # Buscar en el índice
        if key not in self.indexes.keys():
            raise Exception(f"Índice {key} no encontrado")
        if self.indexes[key] is None:
            raise Exception(f"Índice {key} no es un índice AVL")
        pos = self._search_range(key, min_value, max_value)
        return [self.record_file.read(p) for p in pos]

    def clear(self):
        # Limpiar el archivo de registros
        self.record_file.clear()
        # Limpiar los índices
        for i in self.indexes.keys():
            if self.indexes[i] is not None:
                self.indexes[i].clear()

    def __str__(self):
        r = "MANAGER\n"
        r += "Record File:\n"
        r += str(self.record_file) + "\n"
        r += "Indexes:\n"
        for i in self.indexes.keys():
            r += str(i) + "\n"
            r += str(self.indexes[i]) + "\n"

        r += "Schema:\n"
        r += str(self.schema) + "\n"
        r += "-------------------------------------------\n"
        return r
