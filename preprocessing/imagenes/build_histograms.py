import os
import numpy as np
import pickle
from tqdm import tqdm
import argparse
from sklearn.cluster import KMeans

# Argumento: cantidad de clusters K (debe coincidir con el usado en KMeans)
parser = argparse.ArgumentParser()
parser.add_argument('--k', type=int, default=500, help='Cantidad de clusters (visual words)')
args = parser.parse_args()
K = args.k

# Rutas base
ROOT = "F:/fashion_dataset/archive/fashion-dataset"
DESC_FOLDER = os.path.join(ROOT, "descriptors")
DICT_PATH = os.path.join(ROOT, "visual_words", f"visual_words_k{K}.pkl")
OUT_FOLDER = os.path.join(ROOT, f"histograms_k{K}")
os.makedirs(OUT_FOLDER, exist_ok=True)

# Cargar visual words (centroides del KMeans)
print(f"📦 Cargando diccionario visual desde: {DICT_PATH}")
with open(DICT_PATH, "rb") as f:
    visual_words = pickle.load(f)  # ndarray (K, 128)

# Crear modelo KMeans con los centroides ya calculados (sin reentrenar)
kmeans = KMeans(n_clusters=K, init=visual_words, n_init=1)
kmeans.fit(visual_words)  # ⚠️ Solo para marcar el modelo como entrenado

# Listar archivos de descriptores
files = [f for f in os.listdir(DESC_FOLDER) if f.endswith(".npy")]
print(f"🖼️ Procesando {len(files)} imágenes...")

for fname in tqdm(files):
    path = os.path.join(DESC_FOLDER, fname)
    desc = np.load(path)

    if desc is None or len(desc) == 0:
        continue  # Imagen vacía

    # Asignar cada descriptor a su visual word más cercano
    words = kmeans.predict(desc)

    # Construir histograma
    hist = np.bincount(words, minlength=K)  # array de tamaño K con frecuencias

    # Guardar histograma
    outname = os.path.splitext(fname)[0] + ".npy"
    outpath = os.path.join(OUT_FOLDER, outname)
    np.save(outpath, hist)

print(f"✅ Histograms guardados en: {OUT_FOLDER}")
