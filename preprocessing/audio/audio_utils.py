import librosa
import numpy as np
import pickle
from sklearn.cluster import KMeans

SAMPLE_RATE = 22050
N_MFCC = 13
DURATION = 30
K = 100

ROOT = "F:/spotify_songs/dataset"
CODEBOOK_PATH = f"{ROOT}/codebook_audio_k{K}.pkl"
TFIDF_PATH = f"{ROOT}/tfidf_audio_k{K}.pkl"

def extract_mfcc_from_wav(wav_path):
    y, _ = librosa.load(wav_path, sr=SAMPLE_RATE, mono=True, duration=DURATION)
    mfcc = librosa.feature.mfcc(y=y, sr=SAMPLE_RATE, n_mfcc=N_MFCC)
    return mfcc.T

def build_histogram_from_mfcc(mfcc, centroids):
    model = KMeans(n_clusters=K, init=centroids, n_init=1)
    model.fit(centroids)
    words = model.predict(mfcc)
    hist = np.bincount(words, minlength=K)
    return hist

def tfidf_transform(hist, idf):
    tf = hist / (np.sum(hist) + 1e-9)
    return tf * idf

def load_centroids():
    with open(CODEBOOK_PATH, "rb") as f:
        return pickle.load(f)

def load_tfidf_index():
    with open(TFIDF_PATH, "rb") as f:
        return pickle.load(f)
