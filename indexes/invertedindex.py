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
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger("INVERTED-INDEX")

        if not os.path.exists(filename):
            self.logger.error(f"Archivo {filename} no existe.")
            raise FileNotFoundError(f"Archivo {filename} no encontrado.")

        self.logger.info(f"Inicializando InvertedIndex con archivo {filename}.")
        self.file = InvertedFile(filename)

    def _sort_dict(self, d: Dict) -> Dict:
        return {
            word: dict(sorted(postings.items()))
            for word, postings in sorted(d.items())
        }

    def _generate_merged_buckets(self, d1: Dict, d2: Dict):
        """
        Generador que devuelve múltiples buckets resultado de merge entre d1 y d2,
        donde cada bucket serializado no supera BUCKET_LIMIT.
        """
        from collections import OrderedDict

        merged = OrderedDict()
        keys1 = sorted(d1.keys())
        keys2 = sorted(d2.keys())
        i, j = 0, 0

        def flush_bucket(bucket):
            yield dict(bucket)

        bucket = OrderedDict()

        while i < len(keys1) and j < len(keys2):
            k1, k2 = keys1[i], keys2[j]

            if k1 < k2:
                word = k1
                postings = dict(sorted(d1[k1].items()))
                i += 1
            elif k1 > k2:
                word = k2
                postings = dict(sorted(d2[k2].items()))
                j += 1
            else:
                word = k1
                postings1 = d1[k1]
                postings2 = d2[k2]
                merged_postings = {}
                for doc in sorted(set(postings1) | set(postings2)):
                    merged_postings[doc] = postings1.get(doc, 0) + postings2.get(doc, 0)
                postings = merged_postings
                i += 1
                j += 1

            bucket[word] = postings

            # Si se pasa el límite al serializar, rendimos y comenzamos otro
            if len(pickle.dumps(bucket)) > BUCKET_LIMIT:
                bucket.pop(word)
                yield from flush_bucket(bucket)
                bucket = OrderedDict()
                bucket[word] = postings

        # Resto de claves
        for k in keys1[i:]:
            bucket[k] = dict(sorted(d1[k].items()))
            if len(pickle.dumps(bucket)) > BUCKET_LIMIT:
                bucket.pop(k)
                yield from flush_bucket(bucket)
                bucket = OrderedDict()
                bucket[k] = dict(sorted(d1[k].items()))

        for k in keys2[j:]:
            bucket[k] = dict(sorted(d2[k].items()))
            if len(pickle.dumps(bucket)) > BUCKET_LIMIT:
                bucket.pop(k)
                yield from flush_bucket(bucket)
                bucket = OrderedDict()
                bucket[k] = dict(sorted(d2[k].items()))

        if bucket:
            yield from flush_bucket(bucket)

    def _merge_bucket_range(self, l1: int, r1: int, l2: int, r2: int, output_file: InvertedFile) -> list:
        """
        Fusiona los buckets desde [l1, r1] con [l2, r2] y los escribe en output_file.
        Devuelve las nuevas posiciones en output_file.
        """
        positions = []
        i, j = l1, l2
        while i <= r1 and j <= r2:
            d1 = self.file.read(i)
            d2 = self.file.read(j)
            for bucket in self._generate_merged_buckets(d1, d2):
                pos = output_file.append(bucket)
                positions.append(pos)
            i += 1
            j += 1

        # Copiar los buckets restantes sin cambios
        for k in range(i, r1 + 1):
            pos = output_file.append(self.file.read(k))
            positions.append(pos)
        for k in range(j, r2 + 1):
            pos = output_file.append(self.file.read(k))
            positions.append(pos)

        return positions

    def buildIndex(self):
        self.logger.info("Iniciando construcción del índice con SPIMI por rondas.")
        num_buckets = os.path.getsize(self.filename) // BUCKET_LIMIT

        # Ordenar cada bucket individualmente
        for i in range(num_buckets):
            d = self.file.read(i)
            d_sorted = self._sort_dict(d)
            self.file.write(i, d_sorted)

        current_file = self.file
        current_name = self.filename
        round_num = 1

        group_size = 1
        while group_size < num_buckets:
            self.logger.info(f"--- Ronda #{round_num} con grupo de tamaño {group_size} ---")
            temp_name = current_name + f".tmp"
            if os.path.exists(temp_name):
                os.remove(temp_name)

            output_file = InvertedFile(temp_name)
            new_positions = []

            for i in range(0, num_buckets, 2 * group_size):
                l1 = i
                r1 = min(i + group_size - 1, num_buckets - 1)
                l2 = i + group_size
                r2 = min(i + 2 * group_size - 1, num_buckets - 1)

                if l2 > r2:
                    # No hay pareja, copiar directo
                    for k in range(l1, r1 + 1):
                        pos = output_file.append(current_file.read(k))
                        new_positions.append(pos)
                else:
                    # Fusionar los dos rangos
                    positions = self._merge_bucket_range(l1, r1, l2, r2, output_file)
                    new_positions.extend(positions)

            # Reemplazar archivos
            os.remove(current_name)
            os.rename(temp_name, current_name)
            current_file = InvertedFile(current_name)
            self.file = current_file
            num_buckets = len(new_positions)
            group_size *= 2
            round_num += 1

        self.logger.info("Índice invertido completamente construido y ordenado.")

    def getByWord(self,word)->(dict[int,int],int):
        # se necesita devolver los documentos con su tf, y el idf, en el q se encuntra la palabra
        index = {1:1,2:2,3:1,4:4,5:3}
        total_docs = 100 #guardar en header
        idf = math.log(total_docs / len(index))
        return index, idf

    def getLengthDoc(self,doc_id):
        #returns the lenght of the document (plis)
        return 1000


    def search(self, consulta:str)->list[int]:
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
