import os
import numpy as np
import pickle
from tqdm import tqdm
from sklearn.cluster import KMeans

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
DESC_FOLDER = os.path.join(ROOT, "mfcc_descriptors")
CODEBOOK_PATH = os.path.join(ROOT, "codebook_audio_k100.pkl")
OUT_PATH = os.path.join(ROOT, "histograms_audio_k100.pkl")
K = 100

# --- Cargar codebook ---
print(f"📥 Cargando codebook desde: {CODEBOOK_PATH}")
with open(CODEBOOK_PATH, "rb") as f:
    centroids = pickle.load(f)

# Crear modelo KMeans con los centroides
kmeans = KMeans(n_clusters=K, init=centroids, n_init=1)
kmeans.fit(centroids)  # marcar como entrenado

# --- Construir histogramas ---
histograms = {}

print(f"🔄 Procesando MFCCs desde: {DESC_FOLDER}")
for fname in tqdm(os.listdir(DESC_FOLDER)):
    if not fname.endswith(".npy"):
        continue

    audio_id = os.path.splitext(fname)[0]
    path = os.path.join(DESC_FOLDER, fname)
    mfcc = np.load(path)

    if mfcc is None or len(mfcc) == 0:
        continue

    words = kmeans.predict(mfcc)
    hist = np.bincount(words, minlength=K)
    histograms[audio_id] = hist

# --- Guardar histograms ---
with open(OUT_PATH, "wb") as f:
    pickle.dump(histograms, f)

print(f"✅ Histograms guardados en: {OUT_PATH} | Total: {len(histograms)} audios")
