import os
import librosa
import numpy as np
from tqdm import tqdm

# ✅ Ruta correcta
ROOT_AUDIO = "F:/spotify_songs/dataset"
AUDIO_DIR = os.path.join(ROOT_AUDIO, "fma_small")
OUT_DIR = os.path.join(ROOT_AUDIO, "mfcc_descriptors")
os.makedirs(OUT_DIR, exist_ok=True)

SAMPLE_RATE = 22050
N_MFCC = 13
DURATION = 30

def extraer_mfcc(path):
    y, _ = librosa.load(path, sr=SAMPLE_RATE, mono=True, duration=DURATION)
    mfcc = librosa.feature.mfcc(y=y, sr=SAMPLE_RATE, n_mfcc=N_MFCC)
    return mfcc.T

print(f"🎧 Procesando audios en: {AUDIO_DIR}")
for root, _, files in os.walk(AUDIO_DIR):
    for fname in tqdm(files):
        if not fname.endswith(".mp3"):
            continue
        audio_id = os.path.splitext(fname)[0]
        path = os.path.join(root, fname)
        try:
            mfcc = extraer_mfcc(path)
            if mfcc.shape[0] > 0:
                np.save(os.path.join(OUT_DIR, f"{audio_id}.npy"), mfcc)
        except Exception as e:
            print(f"⚠️ Error con {fname}: {e}")

print(f"✅ MFCCs guardados en: {OUT_DIR}")