import os
import numpy as np
from sklearn.cluster import KMeans
import argparse
import pickle

# Ruta del dataset y salida
ROOT_FOLDER = "F:/fashion_dataset/archive/fashion-dataset"
DESCRIPTOR_FILE = os.path.join(ROOT_FOLDER, "all_descriptors.npy")
OUTPUT_DIR = os.path.join(ROOT_FOLDER, "visual_words")

# Asegurar carpeta de salida
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Argumentos por terminal
parser = argparse.ArgumentParser()
parser.add_argument('--k', type=int, default=500, help='Número de clusters para KMeans (ej. 100, 500, 1000)')
args = parser.parse_args()

print(f"📥 Cargando descriptores desde: {DESCRIPTOR_FILE}")
X = np.load(DESCRIPTOR_FILE)

print(f"🧠 Entrenando KMeans con K={args.k}...")
kmeans = KMeans(n_clusters=args.k, random_state=42, verbose=1, n_init='auto')
kmeans.fit(X)

# Guardar modelo (solo centroides)
output_path = os.path.join(OUTPUT_DIR, f"visual_words_k{args.k}.pkl")
with open(output_path, "wb") as f:
    pickle.dump(kmeans.cluster_centers_, f)

print(f"✅ Diccionario visual guardado en: {output_path}")
