import csv
import os
import random
from faker import Faker

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

if __name__ == "__main__":
    generate_inventory_csv(
        path="inventarios.csv",
        n=1000,
        seed=42,
        dims=(200, 200)
    )
    print("inventarios.csv creado")

  
    """
    import csv
import random
import string

def generate_basic_csv(path: str, n: int = 10_000, seed: int = 42):

    random.seed(seed)
    letters = string.ascii_letters

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'value', 'label'])
        for i in range(1, n+1):
            # id: entero autoincremental
            _id = i
            # value: float entre 0 y 1 con 4 decimales
            value = round(random.random(), 4)
            # label: cadena aleatoria de 8 caracteres
            label = ''.join(random.choices(letters, k=8))
            writer.writerow([_id, value, label])

    print(f"{n} registros generados en {path}")

if __name__ == "__main__":
    generate_basic_csv("basic1.csv")
    """
