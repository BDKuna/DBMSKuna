import os
import sys
import re
import string
import pickle
import csv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from indexes.invertedindex import InvertedFile, BUCKET_LIMIT

# --- Configuración fija ---
CSV_PATH     = 'data/dataset.csv'        
INDEX_PATH   = 'table_column_texts.dat'  


import nltk
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
# nltk.download('stopwords') 
# CORRE ESTO LA PRIMERA VEZ

_CLEAN_RE   = re.compile(r'[^a-z0-9]')           # deja sólo letras y dígitos
_STOPWORDS  = set(stopwords.words('english'))    # stop-words inglés
_STEMMER    = SnowballStemmer('english')         # stemmer inglés

# TODO Quenta
def bagOfWords(text:str):
    """
    1) Llama a preprocess()
    2) Pasa a minúsculas, limpia puntuación
    3) Filtra stop-words en inglés
    4) Aplica stemming
    5) Cuenta frecuencias → {stem: tf}
    """
    tf = {}
    for tok in preprocess(text):
        w = tok.lower()                  # minúsculas
        w = _CLEAN_RE.sub('', w)         # quita signos, deja alfanuméricos
        if not w or w in _STOPWORDS:     # descartar
            continue
        w = _STEMMER.stem(w)             # stemming
        tf[w] = tf.get(w, 0) + 1
    return tf

def preprocess(text:str):
    #Sergio: Esto lo uso, asi q pls devuelve así :D
    #Te lo devuelvo igual >:D
    return text.split()

# read dataset, save with InvertedFile
def saveDatasetOnInvertedFile():
    """
    1) Lee CSV (id, description)
    2) Por cada doc, obtiene bow = bagOfWords(...)
    3) Inserta en bucket {w: {doc_id: tf}}
    4) Si pickle.dumps(bucket) > BUCKET_LIMIT:
         a) Quita aportes del doc actual
         b) Ordena alfabéticamente palabras y doc_ids
         c) inv.append(ordered)
         d) Reinicia bucket con sólo el bow de este doc
    5) Al final, flush si queda algo.
    """
    inv = InvertedFile(INDEX_PATH)
    bucket = {}

    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc_id = int(row['id'])
            bow    = bagOfWords(row['description'])

            # 1) añadir bow al bucket
            for w, freq in bow.items():
                bucket.setdefault(w, {})[doc_id] = freq

            # 2) si supera limite, flush parcial
            raw = pickle.dumps(bucket)
            if len(raw) > BUCKET_LIMIT:
                # retira aportes del doc actual
                for w in bow:
                    bucket[w].pop(doc_id, None)
                    if not bucket[w]:
                        del bucket[w]

                # ordena:
                ordered = {
                    w: {did: bucket[w][did] for did in sorted(bucket[w])}
                    for w in sorted(bucket)
                }
                inv.append(ordered)

                # reinicia bucket con sólo este doc
                bucket = {w: {doc_id: tf} for w, tf in bow.items()}

    # 3) flush final
    if bucket:
        ordered = {
            w: {did: bucket[w][did] for did in sorted(bucket[w])}
            for w in sorted(bucket)
        }
        inv.append(ordered)

#if __name__ == "__main__":
#    saveDatasetOnInvertedFile()
#    print("Proceso completado. Buckets guardados en", INDEX_PATH)