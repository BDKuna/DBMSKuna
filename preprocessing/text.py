import os
import sys
import re
import string
import pickle
import csv
from typing import Dict, List, Optional, Iterator, Tuple, OrderedDict


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from indexes.invertedindex import InvertedFile, BUCKET_LIMIT, InvertedIndex

# --- Configuración fija ---
CSV_PATH     = 'data/True.csv'        
INDEX_PATH   = 'table_column_texts.dat'  

BType = Dict[str, Dict[str, int]]

import nltk
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
# nltk.download('stopwords') 
# CORRE ESTO LA PRIMERA VEZ

_CLEAN_RE   = re.compile(r'[^a-z0-9]')           # deja sólo letras y dígitos
_STOPWORDS  = set(stopwords.words('english'))    # stop-words inglés
_STEMMER    = SnowballStemmer('english')         # stemmer inglés

def bagOfWords(text:str) -> Dict[str, int]:
    """
    1) Llama a preprocess()
    2) Pasa a minúsculas, limpia puntuación
    3) Filtra stop-words en inglés
    4) Aplica stemming
    5) Cuenta frecuencias → {stem: tf}
    """
    tf = {}
    for tok in text.split():
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
def saveDatasetOnInvertedFile() -> InvertedFile:
    """
    1. Lee CSV con columnas: title,text,subject,date
    2. Para cada fila, usa `subject` y genera ID como t-0, t-1, ...
    3. Genera bag of words
    4. Inserta en bucket hasta que pase el límite, entonces hace flush parcial
    5. Al final, flush final
    """
    inv = InvertedFile(INDEX_PATH)
    bucket: BType = {}

    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if idx % 1000 == 0:
                print(f"Processing {idx} text")
            if idx == 5000 : break
            doc_id = f"t-{idx}"
            full_bow = bagOfWords(row["text"])

            # Lista de palabras por insertar
            pending_terms = list(full_bow.items())
            term_idx = 0

            while term_idx < len(pending_terms):
                word, freq = pending_terms[term_idx]
                # Insertar la palabra en el bucket
                bucket.setdefault(word, {})[doc_id] = freq

                # Verificar si excede el límite
                raw = pickle.dumps(bucket)
                if len(raw) > BUCKET_LIMIT:
                    # Deshacer inserción de esta palabra
                    bucket[word].pop(doc_id, None)
                    if not bucket[word]:
                        del bucket[word]

                    inv.append(bucket)

                    # Reiniciar bucket
                    bucket = {}
                    # No aumentar term_idx, este término no fue insertado aún
                else:
                    term_idx += 1  # Esta palabra sí se insertó, pasar a la siguiente

    # Flush final
    if bucket:
        inv.append(bucket)
    
    return inv

"""
if __name__ == "__main__":
    if os.path.exists(INDEX_PATH):
        os.remove(INDEX_PATH)
    inv : InvertedFile = saveDatasetOnInvertedFile()
    
    index : InvertedIndex = InvertedIndex(INDEX_PATH)
    print(inv._read_header())
    index.buildIndex()
    inv.show()
"""
#    print("Proceso completado. Buckets guardados en", INDEX_PATH)