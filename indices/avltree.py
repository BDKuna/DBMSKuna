import struct
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import logger
from core.record_file import RecordFile, Record
from core.schema import TableSchema, Column, DataType, IndexType
from core import utils


class AVLNode:
    FORMAT = "iiiii"
    STRUCT = struct.Struct(FORMAT)
    NODE_SIZE = struct.calcsize(FORMAT)
    def __init__(self, val:int = -1, pointer:int = -1, left:int = -1, right:int = -1, height:int = 0):
        self.val = val
        self.pointer = pointer
        self.left = left
        self.right = right
        self.height = height
        self.logger = logger.CustomLogger("AVL_NODE")

    def debug(self):
        self.logger.debug(f"AVL Node with val: {self.val} left: {self.left}, right: {self.right}, height: {self.height}")

    def pack(self) -> bytes:
        return self.STRUCT.pack(self.val, self.pointer, self.left, self.right, self.height)

    @staticmethod
    def unpack(node: bytes):
        if node is None:
            raise Exception("Node is None")
        val, pointer, left, right, height = AVLNode.STRUCT.unpack(node)
        return AVLNode(val, pointer, left, right, height)

class AVLFile:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    HEADER_STRUCT = struct.Struct(HEADER_FORMAT)
    def __init__(self, schema: TableSchema, column: Column):
        if column.index_type != IndexType.AVL:
            raise Exception("column index type doesn't match with AVL")
        self.filename = utils.get_index_file_path(schema.table_name, column.name, IndexType.AVL)
        self.logger = logger.CustomLogger(f"AVLFIlE-{schema.table_name}-{column.name}")
        self.root = -1
        if not os.path.exists(self.filename):
            self.logger.fileNotFound(self.filename)
            open(self.filename, 'ab+').close()
        self._initialize_file()

    def _initialize_file(self):
        with open(self.filename, 'rb+') as file:
            header = file.read(self.HEADER_SIZE)
            if not header:
                self.logger.fileIsEmpty(self.filename)
                self.root = -1
                header = struct.pack(self.HEADER_FORMAT, self.root)
                file.write(header)
            else:
                self.root = struct.unpack(self.HEADER_FORMAT, header)[0]

    def read(self,pos:int) -> AVLNode | None:
        with open(self.filename, "rb") as file:
            offset = self.HEADER_SIZE + pos * AVLNode.NODE_SIZE
            file.seek(offset)
            data = file.read(AVLNode.NODE_SIZE)
            if not data or len(data) < AVLNode.NODE_SIZE:
                return None
            node = AVLNode.unpack(data)
            self.logger.readingNode(self.filename, pos)
            return node

    def write(self, node:AVLNode, pos:int = -1)-> int:
        data = node.pack()
        with open(self.filename, "rb+") as file:
            if pos == -1:
                file.seek(0, 2)  # ir al final
                offset = file.tell()
                pos = (offset - self.HEADER_SIZE) // AVLNode.NODE_SIZE
            else:
                offset = self.HEADER_SIZE + pos * AVLNode.NODE_SIZE
                file.seek(offset)
            file.write(data)
            self.logger.writingNode(self.filename, pos, node.val, node.right, node.left, node.height)
            return pos

    def delete(self, pos: int):
        node = AVLNode()
        node.height = -1
        self.write(node, pos)

    def get_header(self) -> int:
        if self.root != -1:
            return self.root
        else :
            with open(self.filename, "rb") as file:
                file.seek(0)
                data = file.read(self.HEADER_SIZE)
                self.root = struct.unpack("i", data)[0]
                self.logger.readingHeader(self.filename, self.root)
                return self.root

    def write_header(self, root_pos: int):
        self.root = root_pos
        with open(self.filename, "rb+") as file:
            file.seek(0)
            file.write(struct.pack("i", self.root))
            self.logger.writingHeader(self.filename, self.root)

class AVLTree:
    indexFile: AVLFile
    recordFile: RecordFile

    def __init__(self, schema: TableSchema, column: Column):
        self.column = column
        self.indexFile = AVLFile(schema, column)
        self.recordFile = RecordFile(schema)
        self.NODE_SIZE = AVLNode.NODE_SIZE
        self.logger = logger.CustomLogger(f"AVL-Tree-{schema.table_name}-{column.name}")

    def clear(self):
        self.logger.info("Cleaning data, removing files")
        os.remove(self.indexFile.filename)

    # --- Funciones auxiliares ---

    def _seek(self, key:int, pos:int = -2):
        if pos == -2: # get header
            pos = self.indexFile.get_header()
        if pos == -1: # no existe
            return pos
        punt = self.indexFile.read(pos) # obtenemos la tupla y realizamos busqueda binaria
        if punt is None:
            return -1
        if key == punt.val:
            return pos
        if key > punt.val:
            return self._seek(key, punt.right)
        if key < punt.val:
            return self._seek(key, punt.left)

    def _seek_ant(self, key:int, pos:int = -2, pred:int = -1):
        if pos == -2:
            pos = self.indexFile.get_header()
        if pos == -1:
            return pred,-1
        pointer = self.indexFile.read(pos)
        if pointer is None:
            return pred,-1
        if key == pointer.val:
            return pred,pos
        if key > pointer.val:
            return self._seek_ant(key, pointer.right, pos)
        if key < pointer.val:
            return self._seek_ant(key, pointer.left, pos)

    def _update_height(self, n: AVLNode) -> int:
        if not n:
            return -1
        left_height = -1 if n.left == -1 else self.indexFile.read(n.left).height
        right_height = -1 if n.right == -1 else self.indexFile.read(n.right).height
        return max(left_height, right_height) + 1

    def _get_balance(self, n: AVLNode) -> int:
        if not n:
            return 0
        left_height = -1 if n.left == -1 else self.indexFile.read(n.left).height
        right_height = -1 if n.right == -1 else self.indexFile.read(n.right).height
        return left_height - right_height

    def _right_rotate(self, y:AVLNode, pos_y:int, x:AVLNode, pos_x:int):
        t2 = x.right
        x.right = pos_y
        y.left = t2
        y.height = self._update_height(y)
        self.indexFile.write(y, pos_y) # actualizamos la tupla en el archivo
        x.height = self._update_height(x)
        self.indexFile.write(x,pos_x) # actualizamos la tupla en el archivo
        return pos_x

    def _left_rotate(self, x: AVLNode, pos_x: int, y: AVLNode, pos_y: int):
        t2 = y.left
        y.left = pos_x
        x.right = t2
        x.height = self._update_height(x)
        self.indexFile.write(y, pos_y) # actualizamos la tupla en el archivo
        y.height = self._update_height(y)
        self.indexFile.write(x, pos_x) # actualizamos la tupla en el archivo
        return pos_y

    def _balance(self, n:AVLNode, pos:int):
        # una vez insertado, actualizamos la altura y balance del nodo
        n.height = self._update_height(n)
        balance = self._get_balance(n)

        if balance > 1:
            left = self.indexFile.read(n.left)
            # Caso 1
            if self._get_balance(left) >= 0:
                self.logger.warning(f"RIGHT ROTATE: {n.val}")
                if pos == self.indexFile.get_header():
                    self.indexFile.write_header(n.left)
                return self._right_rotate(n,pos,left,n.left)


            # Caso 2
            else:
                self.logger.warning(f"LEFT - RIGHT ROTATE: {n.val}")
                left_right = self.indexFile.read(left.right)
                n.left = self._left_rotate(left, n.left, left_right, left.right)
                if pos == self.indexFile.get_header():
                    self.indexFile.write_header(n.left)
                return self._right_rotate(n, pos, left_right, n.left)


        elif balance < -1:
            right = self.indexFile.read(n.right)
            # Caso 1
            if self._get_balance(right) <= 0:
                self.logger.warning(f"LEFT ROTATE: {n.val}")
                if pos == self.indexFile.get_header():  # Si es la raíz, actualizamos la raíz
                    self.indexFile.write_header(n.right)
                return self._left_rotate(n, pos, right, n.right)

            # Caso 2
            else:
                self.logger.warning(f"RIGHT - LEFT ROTATE: {n.val}")
                right_left = self.indexFile.read(right.left)
                n.right = self._right_rotate(right, n.right,right_left , right.left)
                if pos == self.indexFile.get_header():  # Si la raíz cambia, actualizamos el encabezado
                    self.indexFile.write_header(n.right)
                return self._left_rotate(n, pos, right_left, n.right)

        # else
        self.indexFile.write(n, pos)
        return pos


    def _add_aux(self, n: AVLNode, pos:int = -2):
        if pos == -2:
            pos = self.indexFile.get_header()
        if pos == -1: # no hay ningun registro
            self.indexFile.write_header(self.indexFile.write(n))
            return self.indexFile.get_header()

        point = self.indexFile.read(pos)
        if not point:
            return self.indexFile.write(n)
        #buscamos recursivamente
        if n.val == point.val:
            self.logger.error("DUPLICATE NODE")
            return pos
        if n.val > point.val:
            point.right = self._add_aux(n, point.right)
        elif n.val < point.val:
            point.left = self._add_aux(n, point.left)

        return self._balance(point, pos)

    def _range_search_aux(self, r: list[int], i:int, j:int, pos:int = -2):
        if pos == -2:
            pos = self.indexFile.get_header()
        if pos == -1:  # no hay ningun registro
            return
        punt = self.indexFile.read(pos)
        if i <= punt.val <= j:
            r.append(punt.pointer)
        if i < punt.val:
            self._range_search_aux(r, i, j, punt.left)
        if j > punt.val:
            self._range_search_aux(r, i, j, punt.right)

    def _load_ord(self, r:list[int], pos:int = -2):
        if pos == -2:
            pos = self.indexFile.get_header()
        if pos == -1:
            return
        punt = self.indexFile.read(pos)
        self._load_ord(r, punt.left)
        if punt.pointer != -1:
            r.append(punt.pointer)
        self._load_ord(r, punt.right)

    def _predecessor(self, pos:int) -> AVLNode | None:
        if pos == -1:
            self.logger.error("NO HAY PREDECESOR")
            return None
        node = self.indexFile.read(pos)
        if node.right != -1:
            return self._predecessor(node.right)
        else:
            return node

    def _aux_delete(self, key:int, pos: int = -2) -> int:
        if pos == -2:
            pos = self.indexFile.get_header()
        if pos == -1:
            self.logger.warning("The id is not on the tree")
            return pos

        node = self.indexFile.read(pos)
        if key < node.val:
            node.left = self._aux_delete(key, node.left)
        elif key > node.val:
            node.right = self._aux_delete(key, node.right)
        else:
            if node.left == -1:
                self.indexFile.delete(pos)
                return node.right
            elif node.right == -1:
                self.indexFile.delete(pos)
                return node.left

            pred = self._predecessor(node.left)
            node.val = pred.val
            node.pointer = pred.pointer
            node.left = self._aux_delete(pred.val, node.left)

        if pos == -1:
            self.logger.error("weird error")
            return pos

        return self._balance(node, pos)

    # --- Funciones principales ---

    def insert(self, record: Record, pointer: int):
        key = record.values[record.schema.columns.index(self.column)]
        self.logger.warning(f"INSERTING: {key}")
        node = AVLNode(key, pointer)
        new_root = self._add_aux(node)
        if new_root != self.indexFile.get_header():
            self.indexFile.write_header(new_root)

    def delete(self,  key:int):
        self.logger.warning(f"DELETING: {key}")
        new_root = self._aux_delete(key)
        if new_root != self.indexFile.get_header():
            self.indexFile.write_header(new_root)

    def rangeSearch(self, i:int, j:int) -> list[int]:
        r = []
        self._range_search_aux(r, i, j)
        return r

    def search(self, key:int) -> int:
        self.logger.warning(f"SEARCHING: {key}")
        pos = self._seek(key)
        if pos == -1:
            self.logger.warning("The id is not on the tree")
            return -1
        record_pos = self.indexFile.read(pos).pointer
        return record_pos

    def getAll(self) -> list[int]:
        r = []
        self._load_ord(r)
        return r

    def __str__(self):
        print(f"AVL Tree - {self.column.name}")
        print("Header:", self.indexFile.get_header())
        i = 0
        while True:
            node = self.indexFile.read(i)
            if node is None:
                break
            print(f"Node {i}: val={node.val}, pointer={node.pointer}, left={node.left}, right={node.right}, height={node.height}")
            i += 1
        print("Posiciones ord: ", self.getAll())
        print("ids: ", [self.recordFile.read(i).id for i in self.getAll()])
        return "---"