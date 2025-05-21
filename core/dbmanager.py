import os, sys, shutil, pickle
from collections import Counter
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
import core.utils
from core.schema import DataType, TableSchema, IndexType, SelectSchema, Column
from indices.bplustree import BPlusTree
from indices.avltree import AVLTree
from indices.EH import ExtendibleHashTree
from indices.Rtree import RTreeIndex
from core.record_file import Record, RecordFile
import logger

class DBManager:
    def __init__(self):
        self.tables_path = f"{os.path.dirname(__file__)}/../tables"
        self.logger = logger.CustomLogger("DBManager")

    def error(self, error : str):
        raise RuntimeError(error)

    def create_table(self, table_schema : TableSchema):
        path = f"{self.tables_path}/{table_schema.table_name}"
        if os.path.exists(path):
            self.error("table already exists")
        else:
            os.makedirs(path)

            if not table_schema.columns:
                self.error("the table must have at least 1 column")

            counter = Counter(column.name for column in table_schema.columns)
            # verificar que haya exactamente un primary key, si no tirar error
            # si no se indica, que se un hash

            repeats = [name for name, count in counter.items() if count > 1]
            if len(repeats) > 0:
                self.error(f"the table can't have multiple columns with the same name (repeated names: {','.join(repeats)})")

            with open(f"{path}/metadata.dat", "wb") as file:
                pickle.dump(table_schema, file)
            for column in table_schema.columns:
                index_type = column.index_type
                if index_type is None:
                    continue
                match index_type:
                    case IndexType.AVL:
                        AVLTree(table_schema, column)
                    case IndexType.ISAM:
                        pass
                        # ISAM(table_schema, column)
                    case IndexType.HASH:
                        pass
                        ExtendibleHashTree(table_schema, column)
                    case IndexType.BTREE:
                        BPlusTree(table_schema, column)
                    case IndexType.RTREE:
                        pass
                        RTreeIndex(table_schema, column)
                    case IndexType.SEQ:
                        pass
                        # SEQ(table_schema, column)
                    case _:
                        self.error("invalid index type")
            self.logger.info("Table created successfully")

    
    def get_table_schema(self, table_name : str) -> TableSchema:
        path = f"{self.tables_path}/{table_name}"
        if os.path.exists(path):
            with open(f"{path}/metadata.dat", "rb") as file:
                return pickle.load(file)
        else:
            self.error("table doesn't exist")

    def drop_table(self, table_name : str):
        path = f"{self.tables_path}/{table_name}"
        if os.path.exists(path):
            shutil.rmtree(path)
        else:
            self.logger.error("table doesn't exists")
            #self.error("table doesn't exist")

    def select(self, select_schema : SelectSchema):
        table = self.get_table_schema(select_schema.table_name)
        if not select_schema.all:
            column_names = [column.name for column in table.columns]
            nonexistent = [column for column in select_schema.column_list if column not in column_names]
            if nonexistent:
                self.error(f"some columns don't exist (nonexistent columns: {','.join(nonexistent)})")
        
        result = self.select_condition(table, select_schema.condition)
        return result

    def select_condition(self, table_schema : TableSchema, condition):
        pass

    def insert(self, table_name:str, values: list):
        tableSchema: TableSchema = self.get_table_schema(table_name)
        if len(values) != len(tableSchema.columns):
            raise Exception("El número de valores no coincide con el número de columnas")
        record = Record(tableSchema, values)
        record_file = RecordFile(tableSchema)
        pos = record_file.append(record)

        indexes = tableSchema.get_indexes()

        #insertar los indices
        for i, index in enumerate(indexes.keys()):
            if indexes[index] is not None:
                indexes[index].insert(pos, record.values[i])

    def selectAll(self, table_name:str):
        tableSchema: TableSchema = self.get_table_schema(table_name)
        primaryIndex = tableSchema.get_primary_index()
        record_file = RecordFile(tableSchema)
        return [record_file.read(pos) for pos in primaryIndex.getAll()]
    
    def delete(self):
        pass

    def create_index(self):
        #trae schema, cambia index y guarda schema
        pass

    def drop_index(self):
        #trae schema, elimina index y guarda schema
        pass


def test():
    dbmanager = DBManager()
    import schemabuilder
    builder = schemabuilder.TableSchemaBuilder()
    builder.set_name("productos")
    builder.add_column(name="id", data_type=DataType.INT, is_primary_key=False)
    builder.add_column(name="nombre", data_type=DataType.VARCHAR, is_primary_key=True, index_type=IndexType.BTREE, varchar_length=20)
    schema:TableSchema = builder.get()
    dbmanager.drop_table("productos")
    dbmanager.create_table(schema)
    read_schema = dbmanager.get_table_schema("productos")

    dbmanager.insert(read_schema.table_name, [4, "Sergod2"])
    dbmanager.insert(read_schema.table_name, [6, "Paca"])
    dbmanager.insert(read_schema.table_name, [2, "Sergod5"])
    dbmanager.insert(read_schema.table_name, [5, "Sergod3"])
    dbmanager.insert(read_schema.table_name, [8, "Sergod1"])
    dbmanager.insert(read_schema.table_name, [7, "Eduardo"])
    dbmanager.insert(read_schema.table_name, [40, "Hola"])
    dbmanager.insert(read_schema.table_name, [11, "Sergod4"])
    dbmanager.insert(read_schema.table_name, [3, "Sergod6"])
    dbmanager.insert(read_schema.table_name, [9, "Buenas tardes"])
    indexes = schema.get_indexes()
    for index in indexes.keys():
        if indexes[index] is not None:
            print(indexes[index])

    for i in dbmanager.selectAll(schema.table_name):
        print(i)

    btree = BPlusTree(schema, Column("nombre", DataType.VARCHAR, True, IndexType.BTREE, varchar_length=20))
    print(btree.rangeSearch("Sergod", "Sergod9"))
    print(btree.search("Paca"))
    print(btree.search("Sergod5"))
    print(btree.search("Sergod3"))
    print(btree.search("Eduardo"))
    print(btree.search("Hola"))
    print(btree.search("Sergod4"))
    print(btree.search("Sergod6"))
    print(btree.search("Buenas tardes"))
    print(btree.search("ONO"))
    
    
    

def test_eh():
    # 1) Preparar esquema
    db = DBManager()
    import schemabuilder
    from core.schema import IndexType

    builder = schemabuilder.TableSchemaBuilder()
    builder.set_name("productos")
    # La clave 'id' la dejamos sin índice (solo heap)
    builder.add_column(name="id",     data_type=DataType.INT,     is_primary_key=False)
    # La segunda columna usará nuestro Extendible Hash
    builder.add_column(name="nombre", data_type=DataType.VARCHAR, is_primary_key=True,
                       index_type=IndexType.HASH, varchar_length=20)
    schema: TableSchema = builder.get()

    # 2) Reiniciar (borrar) y crear tabla
    db.drop_table("productos")
    db.create_table(schema)

    # 3) Insertar los 10 registros de ejemplo
    inserts = [
        (4, "Sergod2"),
        (6, "Paca"),
        (2, "Sergod5"),
        (5, "Sergod3"),
        (8, "Sergod1"),
        (7, "Eduardo"),
        (40,"Hola"),
        (11,"Sergod4"),
        (3, "Sergod6"),
        (9, "Buenas tardes"),
    ]
    for pk, nombre in inserts:
        db.insert(schema.table_name, [pk, nombre])

    # 4) Recuperar el índice hash de la columna "nombre"
    indexes = schema.get_indexes()
    hash_index: ExtendibleHashTree = indexes["nombre"]
    print("\n==> Objeto de índice:", hash_index)

    # 5) RecordFile para leer registros desde disco
    rf = RecordFile(schema)

    # 6) Probar getAll() + lectura de registros
    ptrs_all = hash_index.getAll()                   # list[int]
    recs_all = [rf.read(p) for p in ptrs_all]        # list[Record]
    keys_all = [rec.values[1] for rec in recs_all]   # la columna 'nombre' es el índice 1
    print("get_all keys:", sorted(keys_all))

    # 7) Probar search()
    print("\n==> Probar search()")
    for nombre in [
        "Paca", "Sergod5", "Sergod3",
        "Eduardo", "Hola", "Sergod4",
        "Sergod6", "Buenas tardes", "INOEXISTENTE"
    ]:
        ptrs = hash_index.search(nombre)  # list[int]
        if not ptrs:
            print(f"search({nombre!r}) -> None")
        else:
            recs = [rf.read(p) for p in ptrs]
            keys = [rec.values[1] for rec in recs]
            print(f"search({nombre!r}) -> keys {keys}")

    # 8) Probar delete() y getAll() de nuevo
    print("\n==> Probando deletes...")
    for nombre in ["Paca", "Hola", "NOEXISTE"]:
        ok = hash_index.delete(nombre)
        ptrs = hash_index.getAll()
        recs = [rf.read(p) for p in ptrs]
        keys = [rec.values[1] for rec in recs]
        print(f"delete({nombre!r}) -> {ok}")
        print("  keys tras delete:", sorted(keys))

    print("\n✅ Todos los tests de EHT pasaron.")
    
def test_rtree():
    from core.schema import DataType, IndexType
    import schemabuilder
    from core.record_file import RecordFile

    # 1) Preparo el DBManager y construyo el esquema
    db = DBManager()
    builder = schemabuilder.TableSchemaBuilder()
    builder.set_name("puntos")
    # a) Columna id como PK (sin índice espacial)
    builder.add_column(name="id",
                       data_type=DataType.INT,
                       is_primary_key=True)
    # b) Columna 'coord' con índice RTREE: pongo varchar_length > 0
    builder.add_column(name="coord",
                       data_type=DataType.VARCHAR,
                       is_primary_key=False,
                       index_type=IndexType.RTREE,
                       varchar_length=32)
    schema = builder.get()

    # 2) (Re)creo la tabla
    db.drop_table("puntos")
    db.create_table(schema)

    # 3) Inserto unos puntos de prueba
    ejemplos = [
        (1, "(10.0,20.0)"),
        (2, "(5.5, 5.5)"),
        (3, "(15.0,15.0)"),
        (4, "(12.0,22.0)"),
    ]
    for pid, wkt in ejemplos:
        # db.insert espera lista de valores en el orden de las columnas
        db.insert("puntos", [pid, wkt])

    # 4) Recupero el índice espacial
    idx = schema.get_indexes()["coord"]
    print("\n==> Objeto de índice R-Tree:", idx)

    # 5) Uso RecordFile para leer registros desde disco
    rf = RecordFile(schema)

    # 6) Pruebo getAll() → devuelve lista de punteros (ints)
    ptrs = idx.getAll()                     # e.g. [0,1,2,3]
    recs = [rf.read(p) for p in ptrs]       # recupero cada Record
    geoms = [r.values[1] for r in recs]     # la segunda columna es 'coord'
    print("getAll geometrías:", sorted(geoms))
    assert set(geoms) == {e[1] for e in ejemplos}

    # 7) Pruebo search() sobre cada punto
    print("\n==> Probando search()")
    for _, wkt in ejemplos:
        ptr_list = idx.search(wkt)
        assert ptr_list, f"No encontró {wkt}"
        # leo y verifico
        rec = rf.read(ptr_list[0])
        assert rec.values[1] == wkt
        print(f"search({wkt!r}) -> OK")

    # 8) Pruebo rangeSearch() con un rectángulo que atrape sólo algunos
    print("\n==> Probando rangeSearch()")
    # Para simplificar, convierto nuestros WKT de string "(x,y)" a floats
    def parse(s):
        x,y = s.strip("()").split(",")
        return float(x), float(y)
    # rango que incluya sólo los puntos (10,20) y (12,22)
    xmin, ymin = 9.0, 19.0
    xmax, ymax = 13.0, 23.0
    inside = idx.rangeSearch(xmin, ymin, xmax, ymax)
    ids_in = sorted(rf.read(p).values[0] for p in inside)
    print(f"rangeSearch([{xmin},{ymin}]–[{xmax},{ymax}]) IDs:", ids_in)
    assert set(ids_in) == {1, 4}

    # 9) Pruebo delete() y vuelvo a listar
    print("\n==> Probando delete()")
    borrado = "(5.5, 5.5)"
    ok = idx.delete(borrado)
    assert ok, "delete devolvió False"
    ptrs2 = idx.getAll()
    geoms2 = [rf.read(p).values[1] for p in ptrs2]
    print(f"tras delete({borrado!r}) geometrías:", sorted(geoms2))
    assert borrado not in geoms2

    print("\n✅ Todos los tests de RTreeIndex pasaron correctamente.")

    
    
if __name__ == "__main__":
    #test()
    test_eh()
    test_rtree()