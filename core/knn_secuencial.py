import os
import cv2
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
from tqdm import tqdm
import time 
start = time.time()

# --- 1. Cargar histogramas existentes ---
def load_histograms(folder: str) -> dict:
    histograms = {}
    for fname in os.listdir(folder):
        if fname.endswith(".npy"):
            path = os.path.join(folder, fname)
            hist = np.load(path)
            image_id = os.path.splitext(fname)[0]
            histograms[image_id] = hist
    return histograms

# --- 2. Extraer descriptores de imagen query ---
def extract_sift_descriptors(img_path: str) -> np.ndarray:
    img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Imagen no encontrada: {img_path}")
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sift = cv2.SIFT_create()
    _, desc = sift.detectAndCompute(gray, None)
    return desc if desc is not None else np.empty((0, 128))

# --- 3. Convertir a histograma usando KMeans cargado ---
def extract_query_histogram(img_path: str, kmeans_model: KMeans, K: int) -> np.ndarray:
    desc = extract_sift_descriptors(img_path)
    if desc.shape[0] == 0:
        return np.zeros(K, dtype=int)
    words = kmeans_model.predict(desc)
    hist = np.bincount(words, minlength=K)
    return hist

# --- 4. Comparar contra todos los histogramas usando similitud coseno ---
def buscar_similares(query_hist: np.ndarray, all_hists: dict, top_k: int = 5):
    query_hist = query_hist.reshape(1, -1)
    sims = []
    for image_id, hist in all_hists.items():
        sim = cosine_similarity(query_hist, hist.reshape(1, -1))[0][0]
        sims.append((sim, image_id))
    sims.sort(reverse=True)
    return sims[:top_k]

# --- 5. Ejemplo de uso ---
if __name__ == "__main__":
    K = 1000  # puedes cambiar esto a 100 o 1000 según lo desees
    ROOT = "F:/fashion_dataset/archive/fashion-dataset"

    HIST_FOLDER = os.path.join(ROOT, f"histograms_k{K}")
    MODEL_PATH = os.path.join(ROOT, "visual_words", f"visual_words_k{K}.pkl")
    QUERY_IMAGE = "F:/fashion_dataset/archive/fashion-dataset/images/10001.jpg" # prueba con cualquier imagen
    assert os.path.exists(QUERY_IMAGE), f"Ruta no válida: {QUERY_IMAGE}"
    print("📂 Cargando modelo...")
    with open(MODEL_PATH, "rb") as f:
        visual_words = pickle.load(f)

    kmeans = KMeans(n_clusters=K, init=visual_words, n_init=1)
    kmeans.fit(visual_words)  # necesario para permitir predict

    print("📂 Cargando histogramas...")
    all_hists = load_histograms(HIST_FOLDER)

    print("🧪 Procesando imagen query...")
    query_hist = extract_query_histogram(QUERY_IMAGE, kmeans, K)

    print("🔍 Buscando similares...")
    resultados = buscar_similares(query_hist, all_hists, top_k=5)

    print("\n📌 Resultados más similares:")
    for score, img_id in resultados:
        print(f"🔸 {img_id}  | Score: {score:.4f}")

end = time.time()
print(f"⏱ Tiempo total: {end - start:.2f} segundos")