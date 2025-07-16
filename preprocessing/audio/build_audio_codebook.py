import os
import pickle
import numpy as np
from tqdm import tqdm
from sklearn.cluster import KMeans

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
INDEX_PATH = os.path.join(ROOT, "mfcc_index.pkl")
OUTPUT_PATH = os.path.join(ROOT, "codebook_audio_k100.pkl")
MAX_DESCRIPTORS = 1_000_000
K = 100

# --- Cargar índice ---
print(f"📂 Cargando MFCC index desde: {INDEX_PATH}")
with open(INDEX_PATH, "rb") as f:
    mfcc_index = pickle.load(f)

all_desc = []
total = 0

for mfcc in tqdm(mfcc_index.values()):
    all_desc.append(mfcc)
    total += len(mfcc)
    if total >= MAX_DESCRIPTORS:
        break

matrix = np.vstack(all_desc)
print(f"📊 Matriz final: {matrix.shape}")

print(f"🧠 Entrenando KMeans con K={K}...")
kmeans = KMeans(n_clusters=K, random_state=42, n_init='auto', verbose=1)
kmeans.fit(matrix)

# --- Guardar codebook ---
with open(OUTPUT_PATH, "wb") as f:
    pickle.dump(kmeans.cluster_centers_, f)

print(f"✅ Codebook guardado en: {OUTPUT_PATH}")
