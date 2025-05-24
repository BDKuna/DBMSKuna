import os, sys, shutil, pickle
from collections import Counter
from bitarray import bitarray
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

from core.conditionschema import Condition, BinaryCondition, BetweenCondition, NotCondition, BooleanColumn, ConditionColumn, ConditionValue, ConditionSchema, BinaryOp
from core.schema import Column, DataType, TableSchema, IndexType, SelectSchema, DeleteSchema
from core import utils
from indices.bplustree import BPlusTree
from indices.avltree import AVLTree
from indices.EHtree import ExtendibleHashTree
from indices.Rtree import RTreeIndex
from indices.noindex import NoIndex
from core.record_file import Record, RecordFile
import logger

class DBManager:
    def __init__(self):
        self.tables_path = f"{os.path.dirname(__file__)}/../tables"
        self.logger = logger.CustomLogger("DBManager")
        self.indexes = {}

    def error(self, error : str):
        raise RuntimeError(error)

    def get_table_schema(self, table_name : str) -> TableSchema:
        path = f"{self.tables_path}/{table_name}"
        if os.path.exists(path):
            with open(f"{path}/metadata.dat", "rb") as file:
                return pickle.load(file)
        else:
            self.error("table doesn't exist")

    def save_table_schema(self, table_schema : TableSchema, path : str) -> None:
        with open(f"{path}/metadata.dat", "wb") as file:
            pickle.dump(table_schema, file)

    def get_index(self, table_schema : TableSchema, column_name : str): # TODO falta crear un nuevo indice NoIndex
        index_name = f"{table_schema.table_name}.{column_name}"
        if index_name in self.indexes:
            return self.indexes[index_name]
        index = None
        for column in table_schema.columns:
            if column.name == column_name:
                index_type = column.index_type
                match index_type:
                    case IndexType.AVL:
                        index = AVLTree(table_schema, column)
                        pass
                    case IndexType.ISAM:
                        # index = ISAM(table_schema, column)
                        pass
                    case IndexType.HASH:
                        index = ExtendibleHashTree(table_schema, column)
                        pass
                    case IndexType.BTREE:
                        index = BPlusTree(table_schema, column)
                    case IndexType.RTREE:
                        index = RTreeIndex(table_schema, column)
                        pass
                    case IndexType.NONE:
                        index = NoIndex(table_schema, column)
                        pass
                    case _:
                        pass
        self.indexes[index_name] = index
        return index

    def list_to_bitmap(self, list : list[int]) -> bitarray:
        if len(list) == 0:
            bitmap = bitarray(1)
        else:
            max_id = max(list)
            bitmap = bitarray(max_id + 1 + 1)
        bitmap.setall(0)

        for id in list:
            bitmap[id + 1] = 1

        return bitmap
    
    def bitmap_to_list(self, bitmap : bitarray) -> list[int]:
        return [i for i, bit in enumerate(bitmap[1:]) if bit]
    
    def bitmap_or(self, a : bitarray, b : bitarray) -> bitarray:
        len_diff = len(a) - len(b)
        if len_diff > 0:
            b.extend([b[0]] * len_diff)
        else:
            a.extend([a[0]] * (-len_diff))

        return a | b

    def bitmap_and(self, a : bitarray, b : bitarray) -> bitarray:
        len_diff = len(a) - len(b)
        if len_diff > 0:
            b.extend([b[0]] * len_diff)
        else:
            a.extend([a[0]] * (-len_diff))

        return a & b

    def bitmap_not(self, a : bitarray) -> bitarray:
        print(a)
        print(~a)
        return ~a
    
    def bitmap_difference(self, a : bitarray, b : bitarray) -> bitarray:
        return self.bitmap_and(a, self.bitmap_not(b))
    
    def retrieve_data(self, table_schema : TableSchema, bitmap : bitarray) -> list[Record]:
        ids = self.bitmap_to_list(bitmap)
        records = []
        record_file = RecordFile(table_schema)
        for id in ids:
            records.append(record_file.read(id))
        if bitmap[0]:
            id = len(bitmap) -1
            while id < record_file.max_id():
                records.append(record_file.read(id))
                id += 1
        return records
    
    def retrieve_data_and_delete(self, table_schema : TableSchema, bitmap : bitarray) -> list[Record]:
        ids = self.bitmap_to_list(bitmap)
        records = []
        record_file = RecordFile(table_schema)
        for id in ids:
            records.append(record_file.read(id))
            record_file.delete(id)
        if bitmap[0]:
            id = ids[-1] + 1
            while id < record_file.max_id():
                records.append(record_file.read(id))
                record_file.delete(id)
                id += 1
        return records

    def create_table(self, table_schema : TableSchema) -> None:
        path = f"{self.tables_path}/{table_schema.table_name}"
        if os.path.exists(path):
            self.error("table already exists")
        else:
            os.makedirs(path)

            if not table_schema.columns:
                self.error("the table must have at least 1 column")

            counter = Counter(column.name for column in table_schema.columns)

            repeats = [name for name, count in counter.items() if count > 1]
            if len(repeats) > 0:
                self.error(f"the table can't have multiple columns with the same name (repeated names: {','.join(repeats)})")

            counter = Counter(column.is_primary for column in table_schema.columns)
            if counter[True] == 0:
                self.error(f"the table must have a primary key")
            if counter[True] > 1:
                self.error(f"the table can only have one primary key")
            
            for column in table_schema.columns:
                if column.is_primary:
                    if column.index_type == IndexType.NONE:
                        column.index_type = IndexType.HASH
                if column.data_type == DataType.VARCHAR:
                    if column.varchar_length == -1:
                        self.error("Varchar length was not specified")

            self.save_table_schema(table_schema, path)

            for column in table_schema.columns:
                self.get_index(table_schema, column.name)
            
            self.logger.info("Table created successfully")

    def drop_table(self, table_name : str) -> None:
        path = f"{self.tables_path}/{table_name}"
        if os.path.exists(path):
            shutil.rmtree(path)
        else:
            self.error("table doesn't exist")

    #------------------------ SELECT IMPLEMENTATION ----------------------------

    def select(self, select_schema : SelectSchema) -> dict[str, list]:
        table = self.get_table_schema(select_schema.table_name)
        column_names = [column.name for column in table.columns]
        if not select_schema.all:
            nonexistent = [column for column in select_schema.column_list if column not in column_names]
            if nonexistent:
                self.error(f"some columns don't exist (nonexistent columns: {','.join(nonexistent)})")
        
        if select_schema.condition_schema.condition:
            bitmap = self.select_condition(table, select_schema.condition_schema.condition)
        else:
            bitmap = bitarray(1)
            bitmap.setall(1)
        result = self.retrieve_data(table, bitmap)
        if not select_schema.all:
            for record in result:
                value_map = {col.name: val for col, val in zip(table.columns, record.values)}
                record.values = [value_map[name] for name in select_schema.column_list]
        return {
            'columns': column_names if select_schema.all else select_schema.column_list,
            'records': [record.values for record in result]
        }

    def select_condition(self, table_schema : TableSchema, condition : Condition) -> bitarray:
        condition_type = type(condition)
        if condition_type == BinaryCondition:
            op = condition.op
            if op in [BinaryOp.AND, BinaryOp.OR]:
                match op:
                    case BinaryOp.AND:
                        return self.bitmap_and(self.select_condition(table_schema, condition.left), self.select_condition(table_schema, condition.right))
                    case BinaryOp.OR:
                        return self.bitmap_or(self.select_condition(table_schema, condition.left), self.select_condition(table_schema, condition.right))
            else:
                column = None
                for i in table_schema.columns:
                    if i.name == condition.left.column_name:
                        column = i
                        break
                if not column:
                    self.error(f"column '{condition.left.column_name}' doesn't exist in table '{table_schema.table_name}'")
                if column.data_type != utils.get_data_type(condition.right.value):
                    self.error(f"value '{condition.right.value}' is not of data type {column.data_type}")
                match op:    
                    case BinaryOp.EQ: # Usa indices
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.list_to_bitmap(index.search(condition.right.value))
                    case BinaryOp.NEQ: # Usa indices
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.bitmap_not(self.list_to_bitmap(index.search(condition.right.value)))
                    case BinaryOp.LT: # Usa indices (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.bitmap_difference(self.list_to_bitmap(index.rangeSearch(None, condition.right.value)), self.list_to_bitmap(index.search(condition.right.value)))
                    case BinaryOp.GT: # Usa indices (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.bitmap_difference(self.list_to_bitmap(index.rangeSearch(condition.right.value, None)), self.list_to_bitmap(index.search(condition.right.value)))
                    case BinaryOp.LE: # Usa indices (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.list_to_bitmap(index.rangeSearch(None, condition.right.value))
                    case BinaryOp.GE: # Usa indices (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.list_to_bitmap(index.rangeSearch(condition.right.value, None))
        elif condition_type == BetweenCondition: # Usa indices (menos hash)
            column = None
            for i in table_schema.columns:
                if i.name == condition.left.column_name:
                    column = i
                    break
            if not column:
                self.error(f"column '{condition.left.column_name}' doesn't exist in table '{table_schema.table_name}'")
            if column.data_type != utils.get_data_type(condition.mid.value) or column.data_type != utils.get_data_type(condition.right.value):
                self.error(f"value '{condition.right.value}' is not of data type {column.data_type}")
            index = self.get_index(table_schema, condition.left.column_name)
            return self.list_to_bitmap(index.rangeSearch(condition.mid.value, condition.right.value))
        elif condition_type == NotCondition:
            return self.bitmap_not(self.select_condition(table_schema, condition.condition))
        elif condition_type == BooleanColumn: # Usa indices
            column = None
            for i in table_schema.columns:
                if i.name == condition.column_name:
                    column = i
                    break
            if not column:
                self.error(f"column '{condition.column_name}' doesn't exist in table '{table_schema.table_name}'")
            if column.data_type != DataType.BOOL:
                self.error(f"column '{condition.column_name}' is not of data type {DataType.BOOL}")
            if DataType.BOOL != utils.get_data_type(condition.right.value):
                self.error(f"value '{condition.right.value}' is not of data type {DataType.BOOL}")
            index = self.get_index(table_schema, condition.column_name)
            return self.list_to_bitmap(index.search(True))
        else:
            self.error("invalid condition")
        
    #------------------------ INSERT IMPLEMENTATION ----------------------------

    def insert(self, table_name:str, values: list, columns: list):
        tableSchema: TableSchema = self.get_table_schema(table_name)
        table_columns = [column.name for column in tableSchema.columns]

        if columns and sorted(columns) != sorted(table_columns):
            self.error("The specificed columns don't match the table's columns")

        if len(values) != len(tableSchema.columns):
            self.error("The number of values doesn't match the number of columns")

        if columns:
            data_dict = dict(zip(columns, values))
            reordered_values = [data_dict[col] for col in table_columns]
        else:
            reordered_values = values

        # print(reordered_values)

        for i, value in enumerate(reordered_values):
            if tableSchema.columns[i].data_type != utils.get_data_type(value):
                self.error(f"value '{value}' is not of data type {tableSchema.columns[i].data_type}")

        record = Record(tableSchema, reordered_values)
        record_file = RecordFile(tableSchema)
        pos = record_file.append(record)

        indexes = tableSchema.get_indexes()

        #insertar los indices
        for i, index in enumerate(indexes.keys()):
            if indexes[index] is not None:
                indexes[index].insert(pos, record.values[i])

    def delete(self, delete_schema : DeleteSchema) -> None:
        table = self.get_table_schema(delete_schema.table_name)
        bitmap = self.select_condition(table, delete_schema.condition_schema.condition)
        result = self.retrieve_data_and_delete(table, bitmap)
        for record in result:
            for pos, value in enumerate(record.values):
                index = self.get_index(table, table.columns[pos].name)
                index.delete(value)

    def create_index(self, table_name : str, index_name : str, columns : list[str], index_type : IndexType = IndexType.BTREE):
        if len(columns) > 1:
            self.error(f"Index on more than one column not supported")
        column_name = columns[0]
        table_schema = self.get_table_schema(table_name)
        column = None
        for i in table_schema.columns:
            if i.name == column_name:
                column = i
                break
        if not column:
            self.error(f"column with name '{column_name}' doesn't exist")
        if column.index_type != IndexType.NONE:
            self.error(f"column already has an index")

        column.index_type = index_type
        column.index_name = index_name

        self.get_index(table_schema, column_name)

        path = f"{self.tables_path}/{table_name}"
        self.save_table_schema(table_schema, path)

    def drop_index(self, table_name : str, index_name : str) -> None:
        table_schema = self.get_table_schema(table_name)
        for column in table_schema.columns:
            if column.index_name == index_name:
                index = self.get_index(table_schema, column.name)
                index.clear()
                column.index_type = IndexType.NONE
                path = f"{self.tables_path}/{table_schema.table_name}"
                self.save_table_schema(table_schema, path)
                return
        self.error(f"Index with name '{index_name}' on table '{table_name}' doesn't exist")
              
def test():
    dbmanager = DBManager()
    import schemabuilder
    builder = schemabuilder.TableSchemaBuilder()
    builder.set_name("productos")
    builder.add_column(name="id", data_type=DataType.INT, is_primary_key=True, index_type=IndexType.BTREE)
    builder.add_column(name="nombre", data_type=DataType.VARCHAR, is_primary_key=False, varchar_length=20)
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
    

    btree = BPlusTree(schema, Column("id", DataType.INT, True, IndexType.BTREE))
    print(btree.search(2))
    """
    select_schema = SelectSchema(schema.table_name,all=True)
    print(dbmanager.select(select_schema))

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
    """
    assert 1==1

# test()