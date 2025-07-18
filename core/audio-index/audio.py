import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pygame

import time
import heapq
import pickle
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from preprocessing.audio.audio_utils import (
    extract_mfcc_from_wav, build_histogram_from_mfcc, tfidf_transform, load_centroids
)
from indexes.audioindex import AudioInvertedIndex

# Configuración de ruta
ROOT = "../../datasets/spotify_songs"
K = 100
QUERY_PATH = os.path.join(ROOT, "test.mp3")
TFIDF_PATH = os.path.join(ROOT, f"tfidf_audio_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"audio_index_k{K}.pkl")
AUDIO_DIR = os.path.join(ROOT, "fma_small")
TOP_K = 5

# Función para obtener ruta correcta del archivo mp3
def get_audio_path_from_id(audio_id, base_path):
    subdir = str(audio_id)[:3]  # Ej: '026'
    return os.path.join(base_path, subdir, f"{audio_id}.mp3")

# Inicializar pygame
pygame.init()
pygame.font.init()
pygame.mixer.init()

# Configuración de pantalla
WIDTH, HEIGHT = 800, 600
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("🎧 Audio Retrieval GUI")
font = pygame.font.SysFont("Arial", 24)
small_font = pygame.font.SysFont("Arial", 18)

# Cargar índice TF-IDF e invertido
with open(TFIDF_PATH, "rb") as f:
    tfidf_index = pickle.load(f)
idf_vector = np.log((1 + len(tfidf_index)) / (1 + np.sum([v > 0 for v in np.array(list(tfidf_index.values()))], axis=0))) + 1
index = AudioInvertedIndex.load(INDEX_PATH)
centroids = load_centroids()

# Procesar query
start_time = time.time()
mfcc = extract_mfcc_from_wav(QUERY_PATH)
query_hist = build_histogram_from_mfcc(mfcc, centroids)
query_vec = tfidf_transform(query_hist, idf_vector).reshape(1, -1)
nonzero_words = np.nonzero(query_hist)[0]
candidates = index.get_candidates(nonzero_words)

# Comparar con candidatos
results = []
for audio_id in candidates:
    vec = tfidf_index.get(audio_id)
    if vec is not None:
        sim = cosine_similarity(query_vec, vec.reshape(1, -1))[0][0]
        results.append((sim, audio_id))

top = heapq.nlargest(TOP_K, results)
query_time = time.time() - start_time

# UI loop
running = True
selected_audio = None

def draw_gui():
    screen.fill((30, 30, 30))
    title = font.render("🔍 Resultados de búsqueda por contenido (Audio)", True, (255, 255, 255))
    screen.blit(title, (50, 20))

    query_label = small_font.render("Query:", True, (255, 255, 255))
    screen.blit(query_label, (50, 80))

    q_file = os.path.basename(QUERY_PATH)
    screen.blit(small_font.render(q_file, True, (200, 200, 0)), (120, 80))

    for i, (score, audio_id) in enumerate(top):
        y = 140 + i * 70
        fname = f"{audio_id}.mp3"
        display_text = f"{i+1}. {fname} | Score: {score:.3f}"
        screen.blit(small_font.render(display_text, True, (255, 255, 255)), (80, y))

        pygame.draw.rect(screen, (100, 180, 255), pygame.Rect(600, y, 80, 30))
        screen.blit(small_font.render("▶ Reproducir", True, (0, 0, 0)), (605, y + 5))

    tiempo = small_font.render(f"⏱ Tiempo búsqueda: {query_time:.2f} s", True, (255, 255, 0))
    screen.blit(tiempo, (50, HEIGHT - 60))

    pygame.display.flip()

while running:
    draw_gui()

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

        elif event.type == pygame.MOUSEBUTTONDOWN:
            x, y = pygame.mouse.get_pos()
            for i, (_, audio_id) in enumerate(top):
                btn_rect = pygame.Rect(600, 140 + i * 70, 80, 30)
                if btn_rect.collidepoint(x, y):
                    selected_audio = get_audio_path_from_id(audio_id, AUDIO_DIR)
                    if os.path.exists(selected_audio):
                        pygame.mixer.music.load(selected_audio)
                        pygame.mixer.music.play()
                    else:
                        print(f"❌ Archivo no encontrado: {selected_audio}")

pygame.quit()