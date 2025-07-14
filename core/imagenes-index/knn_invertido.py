import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import cv2
import numpy as np
import pickle
from tqdm import tqdm
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from indexes.visualindex import VisualInvertedIndex
import time
start = time.time()

# --- 1. Cargar índice invertido y modelo ---
K = 1000
ROOT = "F:/fashion_dataset/archive/fashion-dataset"
INDEX_PATH = os.path.join(ROOT, f"visual_index_k{K}.pkl")
DICT_PATH = os.path.join(ROOT, "visual_words", f"visual_words_k{K}.pkl")
QUERY_IMAGE = os.path.join(ROOT, "images", "10001.jpg")  # ⚠️ cambia si es necesario

print("📂 Cargando índice invertido...")
index = VisualInvertedIndex.load(INDEX_PATH)

print("📂 Cargando modelo visual words...")
with open(DICT_PATH, "rb") as f:
    visual_words = pickle.load(f)

kmeans = KMeans(n_clusters=K, init=visual_words, n_init=1)
kmeans.fit(visual_words)

# --- 2. Extraer descriptores de la imagen query ---
def extract_sift_descriptors(img_path: str) -> np.ndarray:
    img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Imagen no encontrada: {img_path}")
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sift = cv2.SIFT_create()
    _, desc = sift.detectAndCompute(gray, None)
    return desc if desc is not None else np.empty((0, 128))

print("🧪 Procesando imagen query...")
desc = extract_sift_descriptors(QUERY_IMAGE)
if desc.shape[0] == 0:
    raise ValueError("Imagen sin descriptores")

words = kmeans.predict(desc)
query_hist = np.bincount(words, minlength=K)
nonzero_words = np.nonzero(query_hist)[0]

# --- 3. Obtener candidatos y comparar ---
candidates = index.get_candidates(nonzero_words)
print(f"🔍 Comparando contra {len(candidates)} candidatos")

results = []
query_hist_norm = query_hist / np.linalg.norm(query_hist) if np.linalg.norm(query_hist) > 0 else query_hist

for img_id in tqdm(candidates):
    cand_hist = index.get_histogram(nonzero_words, img_id, K)
    cand_hist = np.array(cand_hist)
    norm = np.linalg.norm(cand_hist)
    if norm == 0:
        continue
    cand_hist_norm = cand_hist / norm
    sim = cosine_similarity([query_hist_norm], [cand_hist_norm])[0][0]
    results.append((sim, img_id))

# --- 4. Mostrar resultados ---
results.sort(reverse=True)
print("\n📌 Resultados más similares:")
for sim, img_id in results[:5]:
    print(f"🔸 {img_id}  | Score: {sim:.4f}")

end = time.time()
print(f"⏱ Tiempo total: {end - start:.2f} segundos")