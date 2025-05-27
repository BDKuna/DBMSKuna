import csv
import os
import random
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
            'stock':        (0,100)
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
            'stock':        (0,50)
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
    result = parser.execute_sql(query)
    end = time.time()
    counter = stats.get_counts()

    times.append(end - start)
    reads.append(counter["reads"])
    writes.append(counter["writes"])


if __name__ == "__main__":
    # Generar inventarios
    path = "data/inventarios.csv"
    generate_inventory_csv(
        path=path,
        n=1000,
        seed=42,
        dims=(200, 200)
    )

    manager = DBManager()
    create = "CREATE TABLE inventarios (id INT PRIMARY KEY, name VARCHAR(100), category VARCHAR(50), subcategory VARCHAR(50), brand VARCHAR(50), price FLOAT, weight_kg FLOAT, length_cm FLOAT, width_cm FLOAT, height_cm FLOAT, geom VARCHAR(20), stock INT, fecha_ingreso VARCHAR(15), descripcion VARCHAR(250));"
    indices= ["BTREE"]
    create_index = [f"CREATE INDEX idx_stock_{idx} ON inventarios USING {idx}(stock);" for idx in indices]
    drop_index = [f"DROP INDEX idx_stock_{idx} ON inventarios;" for idx in indices]

    select_range = "SELECT * FROM inventarios WHERE stock BETWEEN 25 AND 75;"
    select_greater = "SELECT * FROM inventarios WHERE stock > 50;"
    select_equal = "SELECT * FROM inventarios WHERE stock = 50;"

    times = [[] for _ in range(5)]
    reads = [[] for _ in range(5)]
    writes = [[] for _ in range(5)]

    #create table
    parser.execute_sql(create)
    #insert data
    manager.import_csv("inventarios", path)
    #test with no index
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

    # Print results
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

    import matplotlib.pyplot as plt
    import numpy as np

    # Your indices list
    indices = ["HASH", "BTREE", "AVL", "ISAM"]
    queries = ["stock BETWEEN 1 AND 100", "stock > 50", "stock = 50"]


    def extract_phase_data(stats_list, query_idx, num_indices=4):
        """
        Extract data grouped by phase:
        - Phase 0: No index (first measurement for query)
        - Phases 1..num_indices: after each index creation (one per index)
        Returns a list of length num_indices+1 with values
        """
        # First value is no index (initial)
        no_index_val = stats_list[query_idx][0]

        # Next values correspond to index phases, one for each index type
        # Since for each index, after create, you appended one measurement for each query,
        # those measurements come in order starting from index 1
        # so the 1..num_indices values for this query start at 1 and go up by 1 each index
        index_vals = []
        start = 1  # first appended index measurement for this query
        for i in range(num_indices):
            # for each index, pick the (i+1)-th measurement of this query
            # i.e. times[query_idx][i+1]
            if len(stats_list[query_idx]) > i + 1:
                index_vals.append(stats_list[query_idx][i + 1])
            else:
                index_vals.append(np.nan)
        return [no_index_val] + index_vals


    # Extract times, reads, writes for each query, grouped by phase
    times_by_query = [extract_phase_data(times, i) for i in range(3)]
    reads_by_query = [extract_phase_data(reads, i) for i in range(3)]
    writes_by_query = [extract_phase_data(writes, i) for i in range(3)]

    phases = ["No Index"] + indices


    def plot_metric(metric_data, ylabel, title):
        x = np.arange(len(phases))
        width = 0.2

        fig, ax = plt.subplots(figsize=(10, 6))

        for i, query in enumerate(queries):
            values = metric_data[i]
            ax.bar(x + (i - 1) * width, values, width, label=query)

        ax.set_xticks(x)
        ax.set_xticklabels(phases)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend()
        plt.tight_layout()
        plt.show()


    plot_metric(times_by_query, "Time (seconds)", "Query Execution Time by Index Type")
    plot_metric(reads_by_query, "Disk Reads", "Disk Reads by Index Type")
    plot_metric(writes_by_query, "Disk Writes", "Disk Writes by Index Type")
