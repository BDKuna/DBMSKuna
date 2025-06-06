import struct
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logger


class InvertedFile:
    def __init__(self, filename:str):
        pass

    def read(self, pos: int) -> dict:
        pass
    
    def write(self, pos: int, d : dict):
        pass

    def append(self, d: dict):
        pass

    def searchByWord(w: str) -> dict:
        pass


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
