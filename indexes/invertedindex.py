import os
import sys
import pickle
import struct
from typing import Dict, List, Optional, Iterator, Tuple, OrderedDict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from preprocessing.text import *
import math

import logger

BUCKET_LIMIT = 120

BType = Dict[str, Dict[str, int]]

class InvertedFile:
    HEADER_FORMAT = "i"
    HEADER_SIZE = 4
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger(f"INVERTED-FILE-{filename}".upper())
        import logging
        self.logger.logger.setLevel(logging.INFO)
        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                header = struct.pack(self.HEADER_FORMAT, 0)
                f.write(header)
            self.logger.info(f"Archivo {filename} creado con header estructurado.")

    def _serialize(self, d: BType) -> bytes:
        data = pickle.dumps(d)
        if len(data) > BUCKET_LIMIT:
            raise ValueError(f"El diccionario serializado excede BUCKET_LIMIT de {BUCKET_LIMIT} bytes.")
        return data.ljust(BUCKET_LIMIT, b'\x00')

    def _deserialize(self, b: bytes) -> BType:
        try:
            return pickle.loads(b.rstrip(b'\x00'))
        except Exception as e:
            self.logger.error(f"Error de deserialización: {e}")
            return {}

    def _write_header(self, num_buckets: int):
        packed = struct.pack(self.HEADER_FORMAT, num_buckets)
        with open(self.filename, 'r+b') as f:
            f.seek(0)
            f.write(packed)

    def _read_header(self) -> int:
        with open(self.filename, 'rb') as f:
            f.seek(0)
            data = f.read(struct.calcsize(self.HEADER_FORMAT))
            if len(data) < 4:
                return 0
            return struct.unpack(self.HEADER_FORMAT, data)[0]

    def read(self, pos: int) -> BType:
        with open(self.filename, 'rb') as f:
            f.seek(self.HEADER_SIZE + pos * BUCKET_LIMIT)
            data = f.read(BUCKET_LIMIT)
            if not data:
                self.logger.warning(f"Intento de lectura en posición vacía: {pos}")
                return {}
            return self._deserialize(data)

    def write(self, pos: int, d: BType):
        data = self._serialize(d)
        with open(self.filename, 'r+b') as f:
            f.seek(self.HEADER_SIZE + pos * BUCKET_LIMIT)
            f.write(data)
        self.logger.debug(f"Escrito bucket en posición {pos}.")

    def append(self, d: BType) -> int:
        num_buckets = self._read_header()
        data = self._serialize(d)

        with open(self.filename, 'r+b') as f:
            f.seek(self.HEADER_SIZE + num_buckets * BUCKET_LIMIT)
            f.write(data)

        self._write_header(num_buckets + 1)
        self.logger.debug(f"Append en posición {num_buckets}.")
        return num_buckets
    
    def show(self):
        num_buckets = self._read_header()
        self.logger.info(f"Mostrando contenido de {self.filename} ({num_buckets} buckets):")

        for i in range(num_buckets):
            bucket = self.read(i)
            print(f"Bucket {i}: {bucket}")
            print(len(pickle.dumps(bucket)))


class InvertedIndex:
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger("INVERTED-INDEX")

        if not os.path.exists(filename):
            self.logger.error(f"Archivo {filename} no existe.")
            raise FileNotFoundError(f"Archivo {filename} no encontrado.")

        self.logger.info(f"Inicializando InvertedIndex con archivo {filename}.")
        self.file = InvertedFile(filename)

    def insert_buckets(self, buckets : list[BType]):
        for b in buckets:
            self.file.append(b)
    
    def _sort_dict(self, d: Dict) -> Dict:
        return {
            word: dict(sorted(postings.items()))
            for word, postings in sorted(d.items())
        }
        
    def _merge_postings_limited(
        self, 
        current: BType,
        term: str, 
        p1: Dict[str, int], 
        p2: Dict[str, int]
    ) -> Tuple[bool, Dict[str, int], Optional[Dict[str, int]]]:
        """
        Intenta insertar postings fusionados de p1 y p2 para el término `term` dentro de `current`.
        Si no cabe todo, devuelve el fragmento que falta para ser insertado en otro bucket.
        
        Return:
            - inserted: bool → si algo se insertó
            - merged_partial: los postings que se insertaron
            - rest: el resto (None si todo fue insertado)
        """

        all_docs = sorted(set(p1) | set(p2))
        merged = OrderedDict()
        rest = OrderedDict()

        for doc in all_docs:
            freq = p1.get(doc, 0) + p2.get(doc, 0)
            merged[doc] = freq
            temp = current.copy()
            temp[term] = dict(merged)
            if len(pickle.dumps(temp)) > BUCKET_LIMIT:
                # Saca el último doc y lo pasa a rest
                last_doc = list(merged.keys())[-1]
                rest[last_doc] = merged.pop(last_doc)
                break

        inserted = bool(merged)
        rest = dict(rest) if rest else None
        return inserted, dict(merged), rest

# funciona pero hay que indicar si obligadamente debe 
    def _merge_until_limit(
        self, 
        current: BType, 
        b1: Optional[BType], 
        b2: Optional[BType]
    ) -> Tuple[BType, Optional[BType], Optional[BType]]:
        keys1 = list(b1.keys()) if b1 else []
        keys2 = list(b2.keys()) if b2 else []
        i = j = 0

        while i < len(keys1) or j < len(keys2):
            # Caso: term solo en b1
            if i < len(keys1) and (j >= len(keys2) or keys1[i] < keys2[j]):
                term = keys1[i]
                p1 = b1[term]
                p2 = {}

                inserted, merged_partial, rest_postings = self._merge_postings_limited(current, term, p1, p2)

                if not inserted:
                    break

                current[term] = merged_partial

                if rest_postings:
                    # No terminamos b1, pero hay overflow
                    rest_b1 = {term: rest_postings, **{k: b1[k] for k in keys1[i + 1:]}} if i + 1 < len(keys1) else {term: rest_postings}
                    rest_b2 = {k: b2[k] for k in keys2[j:]} if j < len(keys2) else None
                    return current, rest_b1, rest_b2

                i += 1

                # Si se terminó b1, devolver current y hacer que el llamador lea el siguiente
                if i == len(keys1):
                    rest_b2 = {k: b2[k] for k in keys2[j:]} if j < len(keys2) else None
                    return current, None, rest_b2

            # Caso: term solo en b2
            elif j < len(keys2) and (i >= len(keys1) or keys2[j] < keys1[i]):
                term = keys2[j]
                p1 = {}
                p2 = b2[term]

                inserted, merged_partial, rest_postings = self._merge_postings_limited(current, term, p1, p2)

                if not inserted:
                    break

                current[term] = merged_partial

                if rest_postings:
                    rest_b1 = {k: b1[k] for k in keys1[i:]} if i < len(keys1) else None
                    rest_b2 = {term: rest_postings, **{k: b2[k] for k in keys2[j + 1:]}} if j + 1 < len(keys2) else {term: rest_postings}
                    return current, rest_b1, rest_b2

                j += 1

                if j == len(keys2):
                    rest_b1 = {k: b1[k] for k in keys1[i:]} if i < len(keys1) else None
                    return current, rest_b1, None

            # Caso: term está en ambos
            else:
                term = keys1[i]
                p1 = b1[term]
                p2 = b2[term]

                inserted, merged_partial, rest_postings = self._merge_postings_limited(current, term, p1, p2)

                if not inserted:
                    break

                current[term] = merged_partial

                if rest_postings:
                    rest_b1 = {k: b1[k] for k in keys1[i + 1:]} if i + 1 < len(keys1) else None
                    rest_b2 = {term: rest_postings, **({k: b2[k] for k in keys2[j + 1:]} if j + 1 < len(keys2) else {})}
                    return current, rest_b1, rest_b2

                i += 1
                j += 1

                if i == len(keys1) or j == len(keys2):
                    rest_b1 = {k: b1[k] for k in keys1[i:]} if i < len(keys1) else None
                    rest_b2 = {k: b2[k] for k in keys2[j:]} if j < len(keys2) else None
                    return current, rest_b1, rest_b2

        # Se procesaron todos los términos de ambos buckets
        return current, None, None

    def _merge_bucket_range(self, l1: int, r1: int, l2: int, r2: int, output_file: InvertedFile):
        """
        Fusiona todos los buckets en los rangos [l1, r1] y [l2, r2], generando buckets fusionados
        de tamaño BUCKET_LIMIT que se escriben secuencialmente en output_file.
        La cantidad total de buckets generados se conserva igual que la suma original si es necesario,
        rellenando con buckets vacíos al final.
        """
        i, j = l1, l2
        b1 = self.file.read(i) if i <= r1 else None
        b2 = self.file.read(j) if j <= r2 else None
        current: BType = {}

        generated = 0
        original_total = (r1 - l1 + 1) + (r2 - l2 + 1)

        while b1 or b2:
            current, new_b1, new_b2 = self._merge_until_limit(current, b1, b2)
            output_file.append(current)
            generated += 1
            current = {}

            # Solo leer el siguiente bucket si el actual fue completamente consumido
            if not new_b1 and i <= r1:
                i += 1
                b1 = self.file.read(i) if i <= r1 else None
            else:
                b1 = new_b1

            if not new_b2 and j <= r2:
                j += 1
                b2 = self.file.read(j) if j <= r2 else None
            else:
                b2 = new_b2

        # Rellenar con buckets vacíos si generamos menos que el total original
        while generated < original_total:
            output_file.append({})
            generated += 1

    def buildIndex(self):
        self.logger.info("Iniciando construcción del índice con SPIMI por rondas.")
        num_buckets = self.file._read_header()

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
            self.file.show()
            self.logger.info(f"--- Ronda #{round_num} con grupo de tamaño {group_size} ---")
            temp_name = current_name[:-4] + f"_tmp.dat"
            if os.path.exists(temp_name):
                os.remove(temp_name)

            output_file = InvertedFile(temp_name)

            for i in range(0, num_buckets, 2 * group_size):
                l1 = i
                r1 = min(i + group_size - 1, num_buckets - 1)
                l2 = i + group_size
                r2 = min(i + 2 * group_size - 1, num_buckets - 1)

                if l2 > r2:
                    # No hay pareja, copiar directo
                    for k in range(l1, r1 + 1):
                        output_file.append(current_file.read(k))
                else:
                    # Fusionar los dos rangos
                    self._merge_bucket_range(l1, r1, l2, r2, output_file)

            # Reemplazar archivos
            os.remove(current_name)
            os.rename(temp_name, current_name)
            current_file = InvertedFile(current_name)
            self.file = current_file
            group_size *= 2
            round_num += 1

        self.logger.info("Índice invertido completamente construido y ordenado.")

    def getByWord(self,word)->Tuple[dict[int,int],int]:
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



import random

def generate_random_buckets(num_buckets=4, terms_count=10, docs_count=10, bucket_limit=BUCKET_LIMIT) -> List[Dict[str, Dict[str, int]]]:
    term_pool = [f"w{i}" for i in range(terms_count)]
    doc_pool = [f"d{i}" for i in range(docs_count)]

    buckets = []
    for _ in range(num_buckets):
        bucket = {}
        while True:
            term = random.choice(term_pool)
            postings = {}
            for _ in range(random.randint(1, 5)):
                doc = random.choice(doc_pool)
                postings[doc] = random.randint(1, 3)
            bucket[term] = postings
            # Probar si cabe
            if len(pickle.dumps(bucket)) > bucket_limit:
                del bucket[term]  # quitar el último que causó overflow
                break
        buckets.append(bucket)
    return buckets

def test_hard_merge():
    filename = "test_hard.dat"
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'a').close()

    buckets = generate_random_buckets()
    index = InvertedIndex(filename)
    index.insert_buckets(buckets)
    index.buildIndex()
    index.file.show()
    # Verificación del orden global de los términos
    all_terms = []
    num_buckets = index.file._read_header()

    for i in range(num_buckets):
        bucket = index.file.read(i)
        all_terms.extend(bucket.keys())

    if all_terms == sorted(all_terms):
        print("✅ Todos los términos están ordenados globalmente.")
    else:
        print("❌ Los términos no están ordenados globalmente.")


def test_merge_until_limit():
    b1 = {'w1': {'d0': 3, 'd1': 2, 'd3': 3, 'd5': 1, 'd8': 3}, 'w2': {'d0': 1, 'd1': 3, 'd6': 2, 'd7': 2, 'd8': 1}, 'w3': {'d0': 2, 'd5': 2}}
    b2 = {'w0': {'d2': 2, 'd3': 1, 'd4': 5}, 'w6': {'d1': 3, 'd6': 3, 'd8': 1}, 'w7': {'d0': 3, 'd2': 1, 'd3': 4, 'd4': 3, 'd6': 3}}
    
    index = InvertedIndex("test_hard.dat")
    cur, b1, b2 = index._merge_until_limit({}, b1, b2)
    
    print(cur)
    print(b1)
    print(b2)
    print()

    cur, b1, b2 = index._merge_until_limit({}, b1, b2)
    print(cur)
    print(b1)
    print(b2)

def manual_test():
    buckets = [
        {'w1': {'d0': 2, 'd1': 1, 'd3': 3, 'd5': 1}, 'w2': {'d1': 3, 'd6': 1, 'd7': 2}, 'w8': {'d1': 1, 'd2': 1, 'd3': 3}},
        {'w1': {'d0': 1, 'd1': 1, 'd8': 3}, 'w2': {'d0': 1, 'd6': 1, 'd8': 1}, 'w3': {'d0': 2, 'd5': 2, 'd7': 1, 'd8': 1}},
        {'w0': {'d4': 2}, 'w7': {'d0': 3, 'd3': 1, 'd6': 3, 'd7': 3}, 'w9': {'d0': 1, 'd2': 1, 'd4': 2, 'd8': 3}},
        {'w0': {'d2': 2, 'd3': 1, 'd4': 3}, 'w6': {'d1': 3, 'd6': 3, 'd8': 1}, 'w7': {'d2': 1, 'd3': 3, 'd4': 3, 'd7': 3, 'd8': 2}, 'w9': {'d8': 2}}
    ]
    filename = "test_manual.dat"
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'a').close()

    index = InvertedIndex(filename)
    index.insert_buckets(buckets)
    index.buildIndex()
    index.file.show()
    

if __name__ == "__main__":
    test_merge_until_limit()


