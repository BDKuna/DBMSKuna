import struct
import os
import sys
import pickle
from typing import Dict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
        self.filename = InvertedFile()
        pass

    def buildIndex():
        # TODO spimi PACA
        pass

    def search(self, consulta:str):
        # TODO 
        pass
