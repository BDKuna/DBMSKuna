import os
import pickle
import numpy as np
from tqdm import tqdm

def load_histograms(pkl_path):
    with open(pkl_path, 'rb') as f:
        return pickle.load(f)

def compute_tf_idf(histograms: dict[str, np.ndarray], K: int) -> dict[str, np.ndarray]:
    N = len(histograms)
    df = np.zeros(K)

    # Calcular document frequency (df)
    for vec in histograms.values():
        df += (vec > 0).astype(int)

    idf = np.log((1 + N) / (1 + df)) + 1

    tf_idf_vectors = {}
    for img_id, vec in tqdm(histograms.items(), desc="🧠 Calculando TF-IDF"):
        tf = vec / (np.sum(vec) + 1e-9)
        tf_idf = tf * idf
        tf_idf_vectors[img_id] = tf_idf

    return tf_idf_vectors

def main():
    ROOT = "F:/fashion_dataset/archive/fashion-dataset"
    for K in [100, 500, 1000]:
        hist_path = os.path.join(ROOT, f"histograms_k{K}", f"histograms_k{K}.pkl")
        out_path = os.path.join(ROOT, f"tfidf_k{K}.pkl")

        if not os.path.exists(hist_path):
            print(f"❌ No se encontró: {hist_path}")
            continue

        print(f"\n📂 Cargando histogramas desde {hist_path}...")
        histograms = load_histograms(hist_path)

        tf_idf = compute_tf_idf(histograms, K)

        with open(out_path, 'wb') as f:
            pickle.dump(tf_idf, f)

        print(f"✅ TF-IDF guardado en: {out_path} | Total vectores: {len(tf_idf)}")

if __name__ == "__main__":
    main()
