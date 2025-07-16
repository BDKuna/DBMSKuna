import os
import numpy as np
import pickle

def merge_histograms_npy_to_pkl(hist_folder: str, out_path: str):
    histograms = {}
    for fname in os.listdir(hist_folder):
        if fname.endswith(".npy"):
            image_id = os.path.splitext(fname)[0]
            fpath = os.path.join(hist_folder, fname)
            hist = np.load(fpath)
            histograms[image_id] = hist

    with open(out_path, 'wb') as f:
        pickle.dump(histograms, f)

    print(f"✅ Guardado en: {out_path} | Total: {len(histograms)} histogramas")

if __name__ == "__main__":
    ROOT = "F:/fashion_dataset/archive/fashion-dataset"
    for k in [100, 500, 1000]:
        folder = os.path.join(ROOT, f"histograms_k{k}")
        out_file = os.path.join(folder, f"histograms_k{k}.pkl")
        print(f"\n📦 Procesando histograms_k{k}...")
        merge_histograms_npy_to_pkl(folder, out_file)
