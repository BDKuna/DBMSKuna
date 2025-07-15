import nltk
import re
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from typing import Dict

# Ensure stopwords are available. If not, download them at runtime.
try:
    _STOPWORDS = set(stopwords.words("english"))
except LookupError:  # pragma: no cover - only runs on first execution
    nltk.download("stopwords", quiet=True)
    _STOPWORDS = set(stopwords.words("english"))

_CLEAN_RE = re.compile(r"[^a-z0-9]")  # leave only alphanumeric chars
_STEMMER = SnowballStemmer("english")

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
