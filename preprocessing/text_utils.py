import nltk
import re
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from typing import Dict

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
