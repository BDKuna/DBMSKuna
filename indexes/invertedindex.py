import struct
import os
import sys
import pickle
from typing import Dict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from preprocessing.text import *
import math

import logger

BUCKET_LIMIT = 1024

class InvertedFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger(f"INVERTED-FILE-{filename}".upper())

        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                self.logger.info(f"Archivo {filename} creado.")

    def _serialize(self, d: Dict) -> bytes:
        data = pickle.dumps(d)
        if len(data) > BUCKET_LIMIT:
            raise ValueError(f"El diccionario serializado excede BUCKET_LIMIT de {BUCKET_LIMIT} bytes.")
        return data.ljust(BUCKET_LIMIT, b'\x00')

    def _deserialize(self, b: bytes) -> Dict:
        try:
            return pickle.loads(b.rstrip(b'\x00'))
        except Exception as e:
            self.logger.error(f"Error de deserialización: {e}")
            return {}

    def read(self, pos: int) -> Dict:
        with open(self.filename, 'rb') as f:
            f.seek(pos * BUCKET_LIMIT)
            data = f.read(BUCKET_LIMIT)
            if not data:
                self.logger.warning(f"Intento de lectura en posición vacía: {pos}")
                return {}
            return self._deserialize(data)

    def write(self, pos: int, d: Dict):
        data = self._serialize(d)
        with open(self.filename, 'r+b') as f:
            f.seek(pos * BUCKET_LIMIT)
            f.write(data)
        self.logger.info(f"Escrito bucket en posición {pos}.")

    def append(self, d: Dict) -> int:
        data = self._serialize(d)
        with open(self.filename, 'ab') as f:
            f.write(data)
        pos = os.path.getsize(self.filename) // BUCKET_LIMIT - 1
        self.logger.info(f"Append en posición {pos}.")
        return pos

class InvertedIndex:
    def __init__(self):
        #self.filename = InvertedFile()
        pass

    def buildIndex(self):
        # TODO spimi PACA
        pass

    def getByWord(self,word):
        # se necesita devolver los documentos con su tf, y el idf, en el q se encuntra la palabra
        index = {1:1,2:2,3:1,4:4,5:3}
        total_docs = 100 #guardar en header
        idf = math.log(total_docs / len(index))
        return index, idf

    def getLengthDoc(self,doc_id):
        #returns the lenght of the document (plis)
        return 1000


    def search(self, consulta:str):
        query_tf = bagOfWords(consulta)
        vector_doc = [self.getByWord(word) for word in query_tf]

        # Calcular TF-IDF de la consulta
        query_tf_idf = {word: (tf * vector_doc[i][1]) for i,(word, tf) in enumerate(query_tf.items())}
        query_norm = math.sqrt((sum(value ** 2 for value in query_tf_idf.values())))

        score = {}

        # Calcular similitud de cosenos para cada documento
        for i, word in enumerate(query_tf):
            for doc_id, tf in vector_doc[i][0].items():
                if doc_id not in score:
                    score[doc_id] = 0
                # Calcular TF-IDF del documento
                doc_tf_idf = tf * vector_doc[i][1]
                score[doc_id] += query_tf_idf[word] * doc_tf_idf # Producto punto

        # Normalizar por la longitud de los vectores para la definición de similitud de cosenos
        for doc_id in score:
            score[doc_id] /= (query_norm * self.getLengthDoc(doc_id))

        # Ordenar los resultados por similitud
        result = sorted(score.items(), key=lambda tup: tup[1], reverse=True)
        return result

if __name__ == "__main__":
    index = InvertedIndex()
    print(index.search("hola que tal que que pasa pasa pasa pasa"))
