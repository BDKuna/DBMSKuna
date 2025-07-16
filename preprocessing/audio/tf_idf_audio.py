import os
import pickle
import numpy as np
from tqdm import tqdm

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
K = 100
HISTOGRAMS_PATH = os.path.join(ROOT, f"histograms_audio_k{K}.pkl")
TFIDF_PATH = os.path.join(ROOT, f"tfidf_audio_k{K}.pkl")

# --- Cargar histogramas ---
print(f"📂 Cargando histogramas desde: {HISTOGRAMS_PATH}")
with open(HISTOGRAMS_PATH, "rb") as f:
    histograms = pickle.load(f)

N = len(histograms)
df = np.zeros(K)

# --- Calcular document frequency ---
for vec in histograms.values():
    df += (vec > 0).astype(int)

idf = np.log((1 + N) / (1 + df)) + 1  # Smoothing para evitar div/0

# --- Calcular TF-IDF por audio ---
tfidf_vectors = {}

for audio_id, vec in tqdm(histograms.items(), desc="🧠 Calculando TF-IDF"):
    tf = vec / (np.sum(vec) + 1e-9)
    tfidf = tf * idf
    tfidf_vectors[audio_id] = tfidf

# --- Guardar vectores TF-IDF ---
with open(TFIDF_PATH, "wb") as f:
    pickle.dump(tfidf_vectors, f)

print(f"✅ TF-IDF guardado en: {TFIDF_PATH} | Total audios: {len(tfidf_vectors)}")
