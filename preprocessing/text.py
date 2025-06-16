import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# TODO Quenta
def bagOfWords(text:str):
    proces = preprocess(text)
    return {word:text.count(word) for word in proces}

def preprocess(text:str):
    #Sergio: Esto lo uso, asi q pls devuelve así :D
    return text.split()

# read dataset, save with InvertedFile
def saveDatasetOnInvertedFile():
    pass