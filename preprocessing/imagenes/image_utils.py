import os
import cv2
import pandas as pd
from collections import Counter
import pickle

def verificar_dimensiones(images_path: str, sample_size=1000):
    dims_counter = Counter()
    corrupt = []

    for idx, filename in enumerate(os.listdir(images_path)):
        if idx >= sample_size:
            break
        if not filename.lower().endswith(".jpg"):
            continue
        img = cv2.imread(os.path.join(images_path, filename))
        if img is None:
            corrupt.append(filename)
            continue
        dims_counter[img.shape[:2]] += 1  # alto x ancho

    print("Dimensiones comunes:")
    for k, v in dims_counter.most_common(5):
        print(f"{k}: {v}")

    if corrupt:
        print("Archivos corruptos:", corrupt[:5])
    else:
        print("✅ Todo correcto")

def asociar_con_metadatos(csv_path: str, images_path: str, guardar_en: str = None):
    df = pd.read_csv(csv_path, on_bad_lines='skip', encoding='utf-8')
    files = set(os.listdir(images_path))
    print(f"📉 Cargado {len(df)} filas del CSV (se omitieron algunas con errores).")
    df["image_exists"] = df["id"].apply(lambda x: f"{x}.jpg" in files)
    df_valid = df[df["image_exists"] == True].copy()
    print(f"✔ {len(df_valid)} imágenes tienen metadatos válidos")

    if guardar_en:
        with open(guardar_en, "wb") as f:
            pickle.dump(df_valid, f)
            print(f"📁 Guardado en {guardar_en}")

    return df_valid
