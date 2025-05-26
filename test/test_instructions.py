import os, sys

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

from core.schema import DataType, TableSchema, IndexType
from core.dbmanager import DBManager
from core.schemabuilder import TableSchemaBuilder
from core import utils
from core import stats

import matplotlib.pyplot as plt
import csv
import time


def insert_csv_index_only_with_timer(table_schema: TableSchema, index_type: IndexType, csv_path: str):
    col = next(c for c in table_schema.columns if c.name == "value")
    col.index_type = index_type

    dbmanager = DBManager()
    index = dbmanager.get_index(table_schema, col.name)

    times = []
    reads = []
    writes = []

    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            pos = int(row["pos"])
            value = utils.convert_value(row["value"].strip(), col.data_type)

            stats.reset_counters()
            start = time.perf_counter()
            index.insert(pos, value)
            end = time.perf_counter()
            counter = stats.get_counts()

            times.append(end - start)
            reads.append(counter["reads"])
            writes.append(counter["writes"])

    return times, reads, writes

def insert_csv_rtree_with_timer(dbmanager: DBManager, table_schema: TableSchema, csv_path: str):
    col = next(c for c in table_schema.columns if c.name == "location")

    index = dbmanager.get_index(table_schema, col.name)

    times = []

    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            pos = int(row["pos"])
            point_str = row["puntos"].strip()
            point = utils.convert_value(point_str, col.data_type)

            start = time.perf_counter()
            index.insert(pos, point)
            end = time.perf_counter()

            times.append(end - start)

    return times


def test_index_insertions():
    dbmanager = DBManager()

    index_types = [
        IndexType.BTREE,
        IndexType.AVL,
        IndexType.HASH,
    ]

    csv_file = "basic_index_test.csv"
    all_results = {}

    for index_type in index_types:
        dbmanager.drop_table(f"test-{index_type.name.lower()}", True)

        builder = TableSchemaBuilder()
        builder.set_name(f"test-{index_type.name.lower()}")
        builder.add_column(name="id", data_type=DataType.INT, is_primary_key=True)
        builder.add_column(name="value", data_type=DataType.INT, is_primary_key=False, index_type=index_type)
        builder.add_column(name="label", data_type=DataType.VARCHAR, is_primary_key=False, varchar_length=20)

        schema = builder.get()
        dbmanager.create_table(schema)

        times, reads, writes = insert_csv_index_only_with_timer(schema, index_type, csv_file)

        all_results[index_type.name] = {
            "times": times,
            "reads": reads,
            "writes": writes,
        }

        print(f"{index_type.name}: inserted {len(times)} rows")

    # Graficar tiempo de inserción
    plt.figure(figsize=(10, 6))
    for name, data in all_results.items():
        plt.plot(data["times"], label=f"{name}")
    plt.xlabel("Insert number")
    plt.ylabel("Time (seconds)")
    plt.title("Insertion Time per Index Type")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Graficar lecturas
    plt.figure(figsize=(10, 6))
    for name, data in all_results.items():
        plt.plot(data["reads"], label=f"{name}")
    plt.xlabel("Insert number")
    plt.ylabel("Memory Reads")
    plt.title("Memory Reads per Insertion")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Graficar escrituras
    plt.figure(figsize=(10, 6))
    for name, data in all_results.items():
        plt.plot(data["writes"], label=f"{name}")
    plt.xlabel("Insert number")
    plt.ylabel("Memory Writes")
    plt.title("Memory Writes per Insertion")
    plt.legend()
    plt.grid(True)
    plt.show()
    

def test_rtree_insertions():
    dbmanager = DBManager()
    table_name = "testrtree"
    csv_file = "basic_rtree.csv"

    dbmanager.drop_table(table_name, True)

    builder = TableSchemaBuilder()
    builder.set_name(table_name)
    builder.add_column(name="id", data_type=DataType.INT, is_primary_key=True)
    builder.add_column(name="location", data_type=DataType.POINT, is_primary_key=False, index_type=IndexType.RTREE)

    schema = builder.get()
    dbmanager.create_table(schema)

    times = insert_csv_rtree_with_timer(dbmanager, schema, csv_file)

    print(f"R-TREE: inserted {len(times)} rows")

    plt.figure(figsize=(10, 6))
    plt.plot(times, label="RTREE")
    plt.xlabel("Insert number")
    plt.ylabel("Time (seconds)")
    plt.title("Insertion Time for RTree Index")
    plt.legend()
    plt.grid(True)
    plt.show()


test_rtree_insertions()