import os, sys, shutil, pickle
from collections import Counter
from bitarray import bitarray
import heapq
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

from core.conditionschema import Condition, BinaryCondition, BetweenCondition, NotCondition, BooleanColumn, ConditionColumn, ConditionValue, ConditionSchema, BinaryOp
from core.schema import DataType, TableSchema, IndexType, SelectSchema, DeleteSchema
from core import utils
from indexes.bplustree import BPlusTree
from indexes.avltree import AVLTree
from indexes.EHtree import ExtendibleHashTree
from indexes.Rtree import RTreeIndex, MBR, Circle
from indexes.ISAMtree import ISAMIndex
from indexes.noindex import NoIndex

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
                        index = ISAMIndex(table_schema, column)
                        index.build_index()
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
    
    def retrieve_data(self, table_schema : TableSchema, bitmap : bitarray, limit = None) -> list[Record]:
        ids = self.bitmap_to_list(bitmap)
        records = []
        record_file = RecordFile(table_schema)
        count = 0
        for id in ids:
            if limit != None and count >= limit:
                break
            records.append(record_file.read(id))
            count += 1
        if bitmap[0]:
            id = len(bitmap) -1
            while id < record_file.max_id():
                if limit != None and count >= limit:
                    break
                record = record_file.read(id)
                records.append(record)
                id += 1
                count += 1
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
            if select_schema.order_by != None and select_schema.order_by not in column_names:
                self.error(f"ordered by column '{select_schema.order_by}' doesn't exist")
            nonexistent = [column for column in select_schema.column_list if column not in column_names]
            if nonexistent:
                self.error(f"some columns don't exist (nonexistent columns: {','.join(nonexistent)})")
        
        if select_schema.condition_schema.condition:
            bitmap = self.select_condition(table, select_schema.condition_schema.condition)
        else:
            bitmap = bitarray(1)
            bitmap.setall(1)
        if select_schema.order_by == None:
            result = self.retrieve_data(table, bitmap, select_schema.limit)
        else:
            result = self.retrieve_data(table, bitmap)
            for i, column in enumerate(column_names):
                if select_schema.order_by == column:
                    ordered_column_num = i
                    break
            if select_schema.limit != None:
                if select_schema.limit > len(result) / 2:
                    if select_schema.asc:
                        result = sorted(result, key=lambda x : x.values[ordered_column_num])[:select_schema.limit]
                    else:
                        result = sorted(result, reverse=True, key=lambda x : x.values[ordered_column_num])[:select_schema.limit]
                else:
                    if select_schema.asc:
                        result = heapq.nsmallest(select_schema.limit, result, key=lambda x : x.values[ordered_column_num])
                    else:
                        result = heapq.nlargest(select_schema.limit, result, key=lambda x : x.values[ordered_column_num])
            else:
                if select_schema.asc:
                    result = sorted(result, key=lambda x : x.values[ordered_column_num])
                else:
                    result = sorted(result, reverse=True, key=lambda x : x.values[ordered_column_num])
        if not select_schema.all:
            for record in result:
                value_map = {col.name: val for col, val in zip(table.columns, record.values)}
                record.values = [value_map[name] for name in select_schema.column_list]
        final_result = []
        for record in result:
            if record:
                for i, value in enumerate(record.values):
                    if isinstance(value, tuple):
                        record.values[i] = str(value)
                final_result.append(record.values)
        return {
            'columns': column_names if select_schema.all else select_schema.column_list,
            'records': final_result
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
                if column.data_type == DataType.POINT:
                    match op:
                        case BinaryOp.WR:
                            if utils.get_data_type(condition.right.value) != "rectangle":
                                self.error(f"value '{condition.right.value}' is not a valid rectangle definition")
                            index = self.get_index(table_schema, condition.left.column_name)
                            mbr = MBR(condition.right.value[0], condition.right.value[1], condition.right.value[2], condition.right.value[3])
                            return self.list_to_bitmap(index.rangeSearch(mbr))
                        case BinaryOp.WC:
                            if utils.get_data_type(condition.right.value) != "circle":
                                self.error(f"value '{condition.right.value}' is not a valid circle definition")
                            index = self.get_index(table_schema, condition.left.column_name)
                            circle = Circle(condition.right.value[0], condition.right.value[1], condition.right.value[2])
                            return self.list_to_bitmap(index.rangeSearch(circle))
                        case BinaryOp.KNN:
                            if utils.get_data_type(condition.right.value) != "knn":
                                self.error(f"value '{condition.right.value}' is not a valid knn definition")
                            index = self.get_index(table_schema, condition.left.column_name)
                            return self.list_to_bitmap(index.knnSearch(condition.right.value[0], condition.right.value[1], condition.right.value[2]))
                        case _:
                            self.error("operation not supported for POINT type")
                if column.data_type != utils.get_data_type(condition.right.value):
                    self.error(f"value '{condition.right.value}' is not of data type {column.data_type}")
                match op:
                    case BinaryOp.EQ: # Usa indexes
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.list_to_bitmap(index.search(condition.right.value))
                    case BinaryOp.NEQ: # Usa indexes
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.bitmap_not(self.list_to_bitmap(index.search(condition.right.value)))
                    case BinaryOp.LT: # Usa indexes (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.bitmap_difference(self.list_to_bitmap(index.rangeSearch(None, condition.right.value)), self.list_to_bitmap(index.search(condition.right.value)))
                    case BinaryOp.GT: # Usa indexes (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.bitmap_difference(self.list_to_bitmap(index.rangeSearch(condition.right.value, None)), self.list_to_bitmap(index.search(condition.right.value)))
                    case BinaryOp.LE: # Usa indexes (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.list_to_bitmap(index.rangeSearch(None, condition.right.value))
                    case BinaryOp.GE: # Usa indexes (menos hash)
                        index = self.get_index(table_schema, condition.left.column_name)
                        return self.list_to_bitmap(index.rangeSearch(condition.right.value, None))
        elif condition_type == BetweenCondition: # Usa indexes (menos hash)
            column = None
            for i in table_schema.columns:
                if i.name == condition.left.column_name:
                    column = i
                    break
            if not column:
                self.error(f"column '{condition.left.column_name}' doesn't exist in table '{table_schema.table_name}'")
            if column.data_type == DataType.POINT:
                self.error("operation not supported for POINT type")
            if column.data_type != utils.get_data_type(condition.mid.value) or column.data_type != utils.get_data_type(condition.right.value):
                self.error(f"value '{condition.right.value}' is not of data type {column.data_type}")
            index = self.get_index(table_schema, condition.left.column_name)
            return self.list_to_bitmap(index.rangeSearch(condition.mid.value, condition.right.value))
        elif condition_type == NotCondition:
            return self.bitmap_not(self.select_condition(table_schema, condition.condition))
        elif condition_type == BooleanColumn: # Usa indexes
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

        #insertar los indexes
        for i, column in enumerate(tableSchema.columns):
            index = self.get_index(tableSchema, column.name)
            if index:
                index.insert(pos, record.values[i])

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
