import csv
import os
import random

from anyio import sleep
from faker import Faker
from pandas.io.clipboard import init_dev_clipboard_clipboard

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in os.sys.path:
    os.sys.path.append(root_path)
from parser import parser
from core.dbmanager import DBManager
from core import stats
import time

def generate_inventory_csv(path, n, seed=None, dims=(100, 100)):
    """
    Genera un CSV de inventarios con columnas:
    id,name,category,subcategory,brand,price,weight_kg,
    length_cm,width_cm,height_cm,geom,stock,fecha_ingreso,descripcion
    donde 'geom' unifica location_x y location_y.
    """
    fake = Faker()
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    CATEGORY_MAP = {
        'Electronics': {
            'subcategories': {'Phones': ['Apple','Samsung'],
                              'Laptops': ['Dell','HP']},
            'price':        {'Phones': (300,1500),
                             'Laptops': (500,2500)},
            'dims':         {'Phones': ((12,18),(6,8),(0.5,1)),
                             'Laptops': ((30,40),(20,30),(1,3))},
            'weight':       {'Phones': (0.1,0.5),
                             'Laptops': (1,3)},
            'stock':        (0,10000)
        },
        'Furniture': {
            'subcategories': {'Tables': ['IKEA'],
                              'Chairs': ['IKEA']},
            'price':        {'Tables': (50,500),
                             'Chairs': (30,300)},
            'dims':         {'Tables': ((100,200),(50,100),(70,80)),
                             'Chairs': ((40,60),(40,60),(80,100))},
            'weight':       {'Tables': (20,80),
                             'Chairs': (5,20)},
            'stock':        (0,10000)
        }
    }

    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'id','name','category','subcategory','brand','price','weight_kg',
            'length_cm','width_cm','height_cm','geom','stock','fecha_ingreso','descripcion'
        ])
        for i in range(1, n+1):
            cat = random.choice(list(CATEGORY_MAP))
            cfg = CATEGORY_MAP[cat]
            sub = random.choice(list(cfg['subcategories']))
            brand = random.choice(cfg['subcategories'][sub])
            price = round(random.uniform(*cfg['price'][sub]), 2)
            weight = round(random.uniform(*cfg['weight'][sub]), 3)
            dr = cfg['dims'][sub]
            length = round(random.uniform(*dr[0]), 2)
            width  = round(random.uniform(*dr[1]), 2)
            height = round(random.uniform(*dr[2]), 2)
            x = round(random.uniform(0, dims[0]), 2)
            y = round(random.uniform(0, dims[1]), 2)
            geom = f"({x},{y})"
            stock = random.randint(*cfg['stock'])
            fecha = fake.date_between(start_date='-365d', end_date='today').isoformat()
            name = f"{brand} {sub.rstrip('s')}"
            descripcion = fake.text(max_nb_chars=80).replace("\n", " ")
            writer.writerow([
                i, name, cat, sub, brand, price, weight,
                length, width, height, geom, stock, fecha, descripcion
            ])

def generate_stats(query, times, reads, writes):
    stats.reset_counters()
    start = time.time()
    print(parser.execute_sql(query))
    end = time.time()
    counter = stats.get_counts()
    print(counter)
    times.append(end - start)
    reads.append(counter["reads"])
    writes.append(counter["writes"])


import matplotlib.pyplot as plt
import numpy as np

def zero_to_small(val, small=1e-5):
    return val if val > 0 else small

def plot_times(times, indices, cant):
    # Índices con No Index al inicio
    categories = ["No Index"] + indices
    queries = ["Index Creation", "Range Query", "Greater Query", "Equal Query", "Index Drop"]

    # Organizar datos para gráfico (por consulta, por índice)
    # Queremos ordenar las consultas en el orden queries: times[3], times[0], times[1], times[2], times[4]
    # times list is: [Range, Greater, Equal, Creation, Drop]
    data = []
    data.append([zero_to_small(times[3][i]) for i in range(len(categories))])  # Index Creation
    data.append([zero_to_small(times[0][i]) for i in range(len(categories))])  # Range Query
    data.append([zero_to_small(times[1][i]) for i in range(len(categories))])  # Greater Query
    data.append([zero_to_small(times[2][i]) for i in range(len(categories))])  # Equal Query
    data.append([zero_to_small(times[4][i]) for i in range(len(categories))])  # Index Drop

    n_groups = len(categories)
    n_queries = len(queries)

    bar_width = 0.15
    index = np.arange(n_groups)

    plt.figure(figsize=(12,6))
    for i in range(n_queries):
        plt.bar(index + i*bar_width, data[i], bar_width, label=queries[i])

    plt.xlabel("Index Type")
    plt.ylabel("Time (seconds)")
    plt.title(f"Execution Times (seconds) - Inventory {cant} records (Log Scale)")
    plt.xticks(index + bar_width * (n_queries-1)/2, categories)
    plt.yscale('log')
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

def plot_reads_writes(reads_or_writes, indices, cant, title):
    # Índices con No Index al inicio
    categories = ["No Index"] + indices
    queries = ["Index Creation", "Range Query", "Greater Query", "Equal Query"]

    # reads_or_writes list is [Range, Greater, Equal, Creation]
    # Para graficar orden: Creation, Range, Greater, Equal
    data = []
    data.append([zero_to_small(reads_or_writes[3][i]) for i in range(len(categories))])  # Index Creation
    data.append([zero_to_small(reads_or_writes[0][i]) for i in range(len(categories))])  # Range Query
    data.append([zero_to_small(reads_or_writes[1][i]) for i in range(len(categories))])  # Greater Query
    data.append([zero_to_small(reads_or_writes[2][i]) for i in range(len(categories))])  # Equal Query

    n_groups = len(categories)
    n_queries = len(queries)

    bar_width = 0.15
    index = np.arange(n_groups)

    plt.figure(figsize=(12,6))
    for i in range(n_queries):
        plt.bar(index + i*bar_width, data[i], bar_width, label=queries[i])

    plt.xlabel("Index Type")
    plt.ylabel("Count")
    plt.title(f"{title} - Inventory {cant} records (Log Scale)")
    plt.xticks(index + bar_width * (n_queries-1)/2, categories)
    plt.yscale('log')
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

# Ejemplo de uso:

# cant = número de registros, ej: 1000
# indices = lista con índices usados, ej: ['HASH', 'BTREE', 'AVL', 'ISAM']
# times, reads, writes = listas de listas según tu estructura

# plot_times(times, indices, cant)
# plot_reads_writes(reads, indices, cant, "Disk Reads")
# plot_reads_writes(writes, indices, cant, "Disk Writes")



if __name__ == "__main__":
    # Generar inventarios
    path = "data/inventarios.csv"
    cantidades = [1000]


    manager = DBManager()
    for cant in cantidades:
        generate_inventory_csv(
            path=(path + str(cant)),
            n=cant,
            seed=42,
            dims=(200, 200)
        )

        create = "CREATE TABLE inventarios (id INT PRIMARY KEY, name VARCHAR(100), category VARCHAR(50), subcategory VARCHAR(50), brand VARCHAR(50), price FLOAT, weight_kg FLOAT, length_cm FLOAT, width_cm FLOAT, height_cm FLOAT, geom POINT INDEX RTREE, stock INT, fecha_ingreso VARCHAR(15), descripcion VARCHAR(250));"
        indices= ["HASH", "BTREE", "AVL", "ISAM"]
        create_index = [f"CREATE INDEX idx_stock_{idx} ON inventarios USING {idx}(stock);" for idx in indices]
        drop_index = [f"DROP INDEX idx_stock_{idx} ON inventarios;" for idx in indices]

        select_range = "SELECT * FROM inventarios WHERE stock BETWEEN 2500 AND 2600;"
        select_greater = "SELECT * FROM inventarios WHERE stock > 9900;"
        select_equal = "SELECT * FROM inventarios WHERE stock = 500;"

        times = [[] for _ in range(5)]
        reads = [[] for _ in range(5)]
        writes = [[] for _ in range(5)]

        #create table
        parser.execute_sql(create)
        #insert data
        manager.import_csv("inventarios", path+str(cant))
        #test with no index
        parser.execute_sql("INSERT INTO inventarios (id, name, category, subcategory, brand, price, weight_kg, length_cm, width_cm, height_cm, geom, stock, fecha_ingreso, descripcion) VALUES (0, 'Test', 'Test', 'Test', 'Test', 0.0, 0.0, 0.0, 0.0, 0.0, (1.0,1.0), 10000, '2023-10-01', 'Test record');")
        generate_stats(select_range,times[0], reads[0], writes[0])
        generate_stats(select_greater,times[1], reads[1], writes[1])
        generate_stats(select_equal,times[2], reads[2], writes[2])
        times[3].append(0)  # No index creation time
        reads[3].append(0)  # No index reads
        writes[3].append(0)  # No index writes

        times[4].append(0)  # No index drop time
        reads[4].append(0)  # No index reads
        writes[4].append(0)  # No index writes

        for i, idx in enumerate(indices):
            #create index
            print(f"Creating index {idx}")
            generate_stats(create_index[i],times[3], reads[3], writes[3])
            #test with index
            generate_stats(select_range,times[0], reads[0], writes[0])
            generate_stats(select_greater,times[1], reads[1], writes[1])
            generate_stats(select_equal,times[2], reads[2], writes[2])
            #drop index
            print(f"Dropping index {idx}")
            generate_stats(drop_index[i],times[4], reads[4], writes[4])

        print("Results for inventory with", cant, "records:")
        print("Execution times (seconds):")
        for i, idx in enumerate(["No Index"] + indices):
            print(f"{idx}:")
            print(f"  Index Creation: {times[3][i]:.4f}")
            print(f"  Range Query: {times[0][i]:.4f}")
            print(f"  Greater Query: {times[1][i]:.4f}")
            print(f"  Equal Query: {times[2][i]:.4f}")
            print(f"  Index Drop: {times[4][i]:.4f}")
        print("\nDisk Reads:")
        for i, idx in enumerate(["No Index"] + indices):
            print(f"{idx}:")
            print(f"  Index Creation: {reads[3][i]}")
            print(f"  Range Query: {reads[0][i]}")
            print(f"  Greater Query: {reads[1][i]}")
            print(f"  Equal Query: {reads[2][i]}")
        print("\nDisk Writes:")
        for i, idx in enumerate(["No Index"] + indices):
            print(f"{idx}:")
            print(f"  Index Creation: {writes[3][i]}")
            print(f"  Range Query: {writes[0][i]}")
            print(f"  Greater Query: {writes[1][i]}")
            print(f"  Equal Query: {writes[2][i]}")
        print("\n" + "="*50 + "\n")

        plot_times(times, indices, cant)
        plot_reads_writes(reads, indices, cant, "Disk Reads")
        plot_reads_writes(writes, indices, cant, "Disk Writes")



