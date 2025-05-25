import subprocess, sys
import os

# Intentar instalar e importar Rtree
try:
    from rtree import index
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Rtree"])
    from rtree import index

# Asegurar acceso al paquete raíz
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from core.record_file import RecordFile
from core import utils
from core.schema import IndexType
import logger


# -------------------------
# Clases de geometría      
# -------------------------
class Point:
    """
    Representa un punto 2D.
    """
    def __init__(self, x: float, y: float):
        self.x = x
        self.y = y

    def __iter__(self):
        return iter((self.x, self.y))

    def __repr__(self):
        return f"Point({self.x}, {self.y})"

class MBR:
    """
    Minimum Bounding Rectangle en 2D.
    """
    def __init__(self, xmin: float, ymin: float, xmax: float, ymax: float):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        if(xmin > xmax  or ymin > ymax):
            raise Exception("Coordinates must not have minimums more than maximums")

    def bounds(self):
        return (self.xmin, self.ymin, self.xmax, self.ymax)

    def __repr__(self):
        return f"MBR(({self.xmin}, {self.ymin}), ({self.xmax}, {self.ymax}))"

class Circle:
    """
    Círculo definido por centro y radio.
    """
    def __init__(self, cx: float, cy: float, r: float):
        self.cx = cx
        self.cy = cy
        self.r = r

    def mbr(self):
        """
        Devuelve el MBR que encierra al círculo.
        """
        return (self.cx - self.r, self.cy - self.r,
                self.cx + self.r, self.cy + self.r)

    def contains(self, x: float, y: float) -> bool:
        """
        Verifica si el punto (x,y) está dentro del círculo.
        """
        return (x - self.cx)**2 + (y - self.cy)**2 <= self.r**2

    def __repr__(self):
        return f"Circle(center=({self.cx}, {self.cy}), r={self.r})"

# --------------------------------------------
# Clase principal: RTreeIndex                  
# --------------------------------------------
class RTreeIndex:
    """
    Índice R-Tree 2D integrado con RecordFile y DBManager.
    Inserciones, borrados, búsquedas puntuales y búsquedas por región (MBR o círculo).
    """
    def __init__(self, table_schema, column):
        # referencias internas
        self.table_schema = table_schema
        self.column = column
        self.col_idx = table_schema.columns.index(column)
        
        
        try:
            self.logger = logger.CustomLogger(
                f"RTreeIndex-{table_schema.table_name}-{column.name}".upper()
            )
        except Exception:
            self.logger = None


        # ruta base para .idx/.dat
        path = utils.get_index_file_path(
            table_schema.table_name,
            column.name,
            IndexType.RTREE
        )
        path = path[:-4]  # quitar .dat
        
        # RecordFile de la tabla
        self.rf = RecordFile(table_schema)

        # crear/abrir R-Tree en disco
        props = index.Property()
        props.dimension = 2
        
        if os.path.exists(path + '.idx') and os.path.exists(path + '.dat'):
            self.idx = index.Index(path)
        else:
            self.idx = index.Index(path, properties=props)

        # reconstruir mapeo key->pos
        self._key_to_pos = {}
        self._rebuild_mapping()

    def _parse_key(self, key):
        """
        Convierte distintos formatos de clave a coordenadas (x,y).
        Acepta string "(x,y)", tupla/lista o Point.
        """
        # Point
        if hasattr(key, 'x') and hasattr(key, 'y'):
            return key.x, key.y
        # string "(x,y)"
        if isinstance(key, str):
            x_str, y_str = key.strip('()').split(',')
            return float(x_str), float(y_str)
        # iterable (x,y)
        return tuple(map(float, key))

    def _rebuild_mapping(self):
        try:
            b = self.idx.bounds
            if not b:
                return
            xmin, ymin, xmax, ymax = b
            if xmin > xmax or ymin > ymax:
                return
            for pos in self.idx.intersection((xmin, ymin, xmax, ymax)):
                rec = self.rf.read(pos)
                if rec is None:
                    continue
                key = rec.values[self.col_idx]
                self._key_to_pos[key] = pos
        except Exception:
            return

    def insert(self, *args) -> bool:
        """
        Inserta la posición `pos` asociada a `key` (Point, tupla o string).
        """
        if len(args) != 2:
            raise TypeError("insert requiere key y pos")
        a, b = args
        if isinstance(a, int):
            pos, key = a, b
        else:
            key, pos = a, b
        self.logger.warning(f"INSERTING: {key}")
        x, y = self._parse_key(key)
        bbox = (x, y, x, y)
        self.idx.insert(pos, bbox)
        self._key_to_pos[key] = pos
        return True

    def delete(self, key) -> bool:
        """
        Elimina la entrada asociada a `key`. Retorna True si existía.
        """
        if self.logger: self.logger.warning(f"DELETING: {key}")
        pos = self._key_to_pos.get(key)
        if pos is None:
            return False
        x, y = self._parse_key(key)
        bbox = (x, y, x, y)
        try:
            self.idx.delete(pos, bbox)
        except Exception:
            pass
        del self._key_to_pos[key]
        return True

    def search(self, key) -> list[int]:
        """
        Búsqueda puntual: retorna lista con 0 o 1 posiciones.
        """
        if self.logger: self.logger.warning(f"SEARCHING: {key}")
        pos = self._key_to_pos.get(key)
        return [] if pos is None else [pos]

    def rangeSearch(self, region) -> list[int]:
        """Rango espacial: MBR o Circle"""
        self.logger.warning(f"RANGE SEARCHING: {region}")
        if isinstance(region, MBR):
            return list(self.idx.intersection(region.bounds()))
        if isinstance(region, Circle):
            # Filtrado circular
            cand = list(self.idx.intersection(region.mbr()))
            res = []
            for pos in cand:
                rec = self.rf.read(pos)
                x, y = self._parse_key(rec.values[self.col_idx])
                if region.contains(x, y):
                    res.append(pos)
            return res
        raise TypeError('rangeSearch requiere MBR o Circle')

    def getAll(self) -> list[int]:
        """Retorna todas las posiciones indexadas."""
        return list(self._key_to_pos.values())

    def printBuckets(self):
        print("Indexed keys:", sorted(self._key_to_pos.keys()))
    
    def knnSearch(self, x0: float, y0: float, k: int) -> list[int]:
        """k vecinos más cercanos a (x0,y0)"""
        return list(self.idx.nearest((x0, y0, x0, y0), num_results=k))
