import importlib, subprocess, sys
import os
import math
try:
    import rtree
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Rtree"])
    import rtree
    
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.insert(0, root_path)


from rtree import index
from core.record_file import RecordFile
from core import utils
from core.schema import IndexType

class RTreeIndex:
    """
    Integración con DBManager y TableSchema:
      - __init__(table_schema, column)
      - insert(pos:int, key)       # key = (x,y) o "(x,y)"
      - delete(key) -> bool
      - search(key) -> list[int]
      - rangeSearch(xmin,ymin,xmax,ymax) -> list[int]
      - getAll() -> list[int]
    """

    def __init__(self, table_schema, column):
        # Guardamos referencias
        self.table_schema = table_schema
        self.column       = column
        self.col_idx      = table_schema.columns.index(column)

        # 1) Ruta base de índice (.idx/.dat)
        path = utils.get_index_file_path(
            table_schema.table_name,
            column.name,
            IndexType.RTREE
        )

        # 2) Si no existen los archivos, limpiamos restos antiguos
        if not (os.path.exists(path + '.idx') or os.path.exists(path + '.dat')):
            for ext in ('.idx', '.dat'):
                try: os.remove(path + ext)
                except OSError: pass

        # 3) RecordFile de la tabla
        self.rf = RecordFile(table_schema)

        # 4) Abrimos/creamos el R-Tree 2D en disco
        props = index.Property()
        props.dimension = 2
        self.idx = index.Index(path, properties=props)

        # 5) Reconstruimos el mapeo clave→puntero de entradas previas
        self._key_to_pos = {}
        self._rebuild_mapping()

    def _parse_key(self, key):
        """Convierte "(x,y)" o (x,y) en floats (x,y)."""
        if isinstance(key, str):
            x_str, y_str = key.strip("()").split(",")
            return float(x_str), float(y_str)
        return key  # iterable de dos floats

    def _rebuild_mapping(self):
        """
        Cuando se instancia sobre un índice ya existente, recorre
        sus entradas y reconstruye _key_to_pos. Evita bounds inválidos.
        """
        try:
            b = self.idx.bounds
            if not b:
                return
            xmin, ymin, xmax, ymax = b
            # descartar si bounds no tienen sentido
            if xmin > xmax or ymin > ymax:
                return
            for pos in self.idx.intersection((xmin, ymin, xmax, ymax)):
                rec = self.rf.read(pos)
                if rec is None:
                    continue
                key = rec.values[self.col_idx]
                self._key_to_pos[key] = pos
        except (RTreeError, ValueError):
            # índice vacío o bounds inválidos: no hay nada que reconstruir
            return

    def insert(self, pos: int, key) -> bool:
        """Inserta pos usando key=(x,y) o "(x,y)"."""
        x, y = self._parse_key(key)
        bbox = (x, y, x, y)
        self.idx.insert(pos, bbox)
        self._key_to_pos[key] = pos
        return True

    def delete(self, key) -> bool:
        """Elimina la entrada por clave. Devuelve True si existía."""
        pos = self._key_to_pos.get(key)
        if pos is None:
            return False
        x, y = self._parse_key(key)
        bbox = (x, y, x, y)
        try:
            self.idx.delete(pos, bbox)
        except RTreeError:
            # si falla el delete en la librería, seguimos adelante
            pass
        del self._key_to_pos[key]
        return True

    def search(self, key) -> list[int]:
        """
        Lista de punteros (0 o 1 elemento) para homogeneidad con EH/B+Tree.
        """
        pos = self._key_to_pos.get(key)
        return [] if pos is None else [pos]

    def rangeSearch(self, xmin, ymin, xmax, ymax) -> list[int]:
        """Devuelve lista de posiciones dentro del rectángulo dado."""
        try:
            return list(self.idx.intersection((xmin, ymin, xmax, ymax)))
        except (RTreeError, ValueError):
            return []
        
    def circleSearch(self, cx: float, cy: float, r: float) -> list[int]:
        """
        Busca todos los registros cuya coordenada (x,y) esté
        dentro del círculo de centro (cx,cy) y radio r.
        Devuelve lista de punteros (posiciones en el RecordFile).
        """
        # 1) Limites del MBR
        xmin, ymin = cx - r, cy - r
        xmax, ymax = cx + r, cy + r

        # 2) Candidatos dentro del rectángulo
        try:
            candidatos = list(self.idx.intersection((xmin, ymin, xmax, ymax)))
        except:
            return []

        # 3) Filtrar por distancia real al centro
        resultado = []
        for pos in candidatos:
            rec = self.rf.read(pos)
            # rec.values[self.col_idx] es la clave original: "(x,y)" o tupla
            x, y = self._parse_key(rec.values[self.col_idx])
            if (x - cx)**2 + (y - cy)**2 <= r*r:
                resultado.append(pos)

        return resultado

    def getAll(self) -> list[int]:
        """Devuelve todos los punteros indexados."""
        return list(self._key_to_pos.values())

    def printBuckets(self):
        print("Indexed keys:", sorted(self._key_to_pos.keys()))