import struct
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logger
from core.schema import TableSchema, Column, DataType, IndexType
from core import utils
from core import stats

class NodeBPlus:
	BLOCK_FACTOR = 3
	def __init__(self, column: Column, keys=None, pointers=None, isLeaf:bool = False, size:int = 0, nextNode:int = -1):
		if pointers is None:
			pointers = []
		if keys is None:
			keys = []
		self.column = column
		self.FORMAT = "<" + str(utils.calculate_column_format(column) * self.BLOCK_FACTOR) + str("i" * (self.BLOCK_FACTOR + 1)) + "iii" # num keys + 1 = num pointers, + isLeaf, size, nextNode
		self.NODE_SIZE = struct.calcsize(self.FORMAT)
		if isLeaf:
			if len(pointers) != len(keys):
				raise Exception("Creating leaf node, number of keys and pointers must be equal")
		else:
			if len(pointers) != len(keys) + 1:
				raise Exception("Creating internal node, number of pointers must be one more than number of keys")
		

		empty_key = utils.get_empty_value(self.column)
		while len(keys) < self.BLOCK_FACTOR:
			keys.append(empty_key)
		while len(pointers) < self.BLOCK_FACTOR + 1:
			pointers.append(-1)
		
		self.keys = keys
		self.pointers = pointers
		self.isLeaf = isLeaf
		self.size = size
		self.nextNode = nextNode
		self.logger = logger.CustomLogger("NODEBPLUS")
	
	def addLeafId(self, key:any, pointer:int):
		self.logger.debug(f"Adding id: {key} and pointer: {pointer} in bucket")
		#if len(self.pointers) != len(self.keys):
		#	raise Exception("In leaf node, number of keys and pointers must be equal")
		
		if not self.isFull():
			self.keys[self.size] = key
			self.pointers[self.size] = pointer
			self.size += 1
		else:
			raise Exception("Node is full")
	
	def addInternalId(self, key:any, pointer:int):
		self.logger.debug(f"Adding id: {key} and pointer: {pointer} in bucket")
		if len(self.pointers) != len(self.keys) + 1:
			raise Exception("In intern node, number of keys and pointers must be differ in 1")
		
		if not self.isFull():
			self.keys[self.size] = key
			self.pointers[self.size+1] = pointer
			self.size += 1
		else:
			raise Exception("Node is full")
	
	def insertInLeaf(self, key: any, pointer: int):
		assert(self.isLeaf)
		self.logger.debug(f"Inserting in leaf: key={key}, pointer={pointer}")
		self.addLeafId(key, pointer)

		i = self.size - 1
		while i > 0 and self.keys[i] < self.keys[i - 1]:
			self.keys[i], self.keys[i - 1] = self.keys[i - 1], self.keys[i]
			self.pointers[i], self.pointers[i - 1] = self.pointers[i - 1], self.pointers[i]
			i -= 1

	def insertInInternalNode(self, key: any, rightChildPtr: int):
		assert(not self.isLeaf)
		self.logger.debug(f"Inserting in internal self: key={key}, rightPtr={rightChildPtr}")
		self.addInternalId(key, rightChildPtr)

		i = self.size - 1
		while i > 0 and self.keys[i] < self.keys[i - 1]:
			self.keys[i], self.keys[i - 1] = self.keys[i - 1], self.keys[i]
			self.pointers[i+1], self.pointers[i] = self.pointers[i], self.pointers[i + 1]
			i -= 1

	def isFull(self) -> bool:
		return self.size == len(self.keys)

	def pack(self) -> bytes:
		data_buf = b''
		type = utils.calculate_column_format(self.column)
		for key in self.keys:
			data_buf += struct.pack(type, key.encode() if self.column.data_type == DataType.VARCHAR else key)
		for pointer in self.pointers:
			data_buf += struct.pack('i', pointer)
		data_buf += struct.pack('iii', self.isLeaf, self.size, self.nextNode)
		return data_buf

	def debug(self):
		print(f"Node with keys: {self.keys}, pointers: {self.pointers}, isLeaf: {self.isLeaf}, size: {self.size}, nextNode: {self.nextNode}")

	@staticmethod
	def unpack(record:bytes, column: Column):
		if(record == None):
			raise Exception("record is None")
		isLeaf, size, nextNode = struct.unpack('iii', record[-12:])

		blockFactor = NodeBPlus.BLOCK_FACTOR
		
		key_fmt = utils.calculate_column_format(column)
		key_size = struct.calcsize(key_fmt)

		keys = []
		pointers = []

		for i in range(size):
			start = i * key_size
			end = start + key_size
			val = struct.unpack(key_fmt, record[start:end])[0]
			if column.data_type == DataType.FLOAT:
				val = round(val,6) #float precision
			if column.data_type == DataType.VARCHAR:
				val = val.decode().strip("\x00")
			keys.append(val)

		ptr_start = blockFactor * key_size
		
		for i in range(size + 1 - isLeaf):
			start = ptr_start + i * 4
			end = start + 4
			ptr = struct.unpack('i', record[start:end])[0]
			pointers.append(ptr)

		return NodeBPlus(column, keys, pointers, isLeaf, size, nextNode)

class BPlusFile:
	HEADER_SIZE = 4

	def __init__(self, schema:TableSchema, column:Column):
		self.column = column
		if(column.index_type != IndexType.BTREE):
			raise Exception("column index type doesn't match with BTREE")
		self.filename = utils.get_index_file_path(schema.table_name, column.name, IndexType.BTREE)
		self.logger = logger.CustomLogger(f"BPLUSFILE-{schema.table_name}-{column.name}".upper())
		
		self.NODE_SIZE = struct.calcsize("<" + (utils.calculate_column_format(column) * NodeBPlus.BLOCK_FACTOR) + ("i" * (NodeBPlus.BLOCK_FACTOR + 1)) + "iii")
		#self.logger.logger.setLevel(logging.WARNING)

		if not os.path.exists(self.filename):
			self.logger.fileNotFound(self.filename)
			self.initialize_file(self.filename) # if archive not exists
		else:
			with open(self.filename, "rb+") as file:
				file.seek(0,2)
				if(file.tell() == 0):
					self.logger.fileIsEmpty(self.filename)
					self.initialize_file(self.filename) # if archive is empty

	def initialize_file(self, filename):
		with open(filename, "wb") as file:
			header = -1 # root
			file.write(struct.pack("i", header))
			stats.count_write()
	
	def readBucket(self, pos: int) -> NodeBPlus:
		if(pos == -1):
			raise Exception(f"Error reading bucket at pos {pos}")
		with open(self.filename, "rb") as file:
			offset = self.HEADER_SIZE + pos * self.NODE_SIZE
			file.seek(offset)
			data = file.read(self.NODE_SIZE)
			stats.count_read()
			if not data or len(data) < self.NODE_SIZE:
				self.logger.invalidPosition(self.filename, pos)
				raise Exception(f"Invalid bucket position: {pos}")
			node = NodeBPlus.unpack(data, self.column)
			self.logger.readingBucket(self.filename, pos, node.keys)
			return node

	def writeBucket(self, pos: int, node: NodeBPlus) -> int:
		data = node.pack()
		with open(self.filename, "rb+") as file:
			if pos == -1:
				file.seek(0, 2)  # ir al final
				offset = file.tell()
				pos = (offset - self.HEADER_SIZE) // self.NODE_SIZE
			else:
				offset = self.HEADER_SIZE + pos * self.NODE_SIZE
				file.seek(offset)
			file.write(data)
			stats.count_write()
			self.logger.writingBucket(self.filename, pos, node.keys)
			return pos		

	def getHeader(self) -> int:
		with open(self.filename, "rb") as file:
			file.seek(0)
			data = file.read(self.HEADER_SIZE)
			stats.count_read()
			rootPosition = struct.unpack("i", data)[0]
			self.logger.readingHeader(self.filename, rootPosition)
			return rootPosition

	def writeHeader(self, rootPosition: int):
		with open(self.filename, "rb+") as file:
			file.seek(0)
			file.write(struct.pack("i", rootPosition))
			stats.count_write()
			self.logger.writingHeader(self.filename, rootPosition)


class BPlusTree:
	indexFile: BPlusFile

	def __init__(self, schema:TableSchema, column:Column):
		self.column = column
		self.empty_key = utils.get_empty_value(self.column)
		if column.index_type != IndexType.BTREE:
			raise Exception("column index type doesn't match with BTREE")
		self.indexFile = BPlusFile(schema, column)
		self.BLOCK_FACTOR = NodeBPlus.BLOCK_FACTOR
		self.logger = logger.CustomLogger(f"BPLUSTREE-{schema.table_name}-{column.name}".upper())
	
	def insert(self, pos:int, val:any):
		self.logger.warning(f"INSERTING: {val}")

		rootPos = self.indexFile.getHeader()
		if(rootPos == -1):
			self.logger.info(f"Creating new root, first record with id: {val}")
			root = NodeBPlus(column=self.column, isLeaf=True)
			root.addLeafId(val, pos)
			rootPos = self.indexFile.writeBucket(-1, root) # new bucket
			self.indexFile.writeHeader(rootPos)
			return
		
		split, newKey, newPointer = self.insertAux(rootPos, val, pos)

		if not split:
			self.logger.successfulInsertion(self.indexFile.filename, val)
			return
		
		# ¡SI HAY SPLIT, CREAMOS NUEVA RAIZ DIRECTAMENTE!
		self.logger.info(f"Root was split, Creating new root")
		newRoot = NodeBPlus(
			self.column,
			keys=[newKey],
			pointers=[rootPos, newPointer],
			isLeaf=False,
			size=1
		)
		newRootPos = self.indexFile.writeBucket(-1, newRoot)
		self.indexFile.writeHeader(newRootPos)
		self.logger.info(f"New root created with keys: {newRoot.keys}")
		self.logger.successfulInsertion(self.indexFile.filename, val)
	
	def insertAux(self, nodePos:int, key:any, pointer:int) -> tuple[bool, any, int]: # split?, key, pointer
		node:NodeBPlus = self.indexFile.readBucket(nodePos)
		if(node.isLeaf): # if is leaf, insert
			node.insertInLeaf(key, pointer)
			if(not node.isFull()):
				self.indexFile.writeBucket(nodePos, node)
				self.logger.info(f"node leaf with keys: {node.keys} is not full, not splitting")
				return False, self.empty_key, -1
			
			self.logger.info(f"node leaf is full, splitting node with keys: {node.keys}")
			mid = node.size // 2
			leftKeys, rightKeys = node.keys[:mid], node.keys[mid:]
			leftPointers, rightPointers = node.pointers[:mid], node.pointers[mid:-1]
			newNode = NodeBPlus(self.column, rightKeys, rightPointers, True, len(rightKeys), node.nextNode)
			pos = self.indexFile.writeBucket(-1, newNode)
			node = NodeBPlus(self.column, leftKeys, leftPointers, True, len(leftKeys), pos)
			self.indexFile.writeBucket(nodePos, node)
			self.logger.info(f"node leaf spplitted into left node with keys: {node.keys} and right node with keys: {newNode.keys}")

			return True, newNode.keys[0], pos

		else:
			ite = 0
			while(ite < node.size and node.keys[ite] < key): # finding where to insert id
				ite += 1
			split, newKey, newPointer = self.insertAux(node.pointers[ite], key, pointer)

			if not split:
				return False, self.empty_key, -1
			
			node.insertInInternalNode(newKey, newPointer)

			if not node.isFull():
				self.indexFile.writeBucket(nodePos, node)
				self.logger.info(f"node intern with keys: {node.keys} is not full, not splitting")
				return False, self.empty_key, -1
			
			self.logger.info(f"node intern is full, splitting node with keys: {node.keys}")
			mid = node.size // 2
			leftKeys, rightKeys = node.keys[:mid], node.keys[mid+1:] # split keys but one is going up
			upKey = node.keys[mid]
			leftPointers, rightPointers = node.pointers[:mid+1], node.pointers[mid+1:] # pointers split but maintain for them
			
			newNode = NodeBPlus(self.column, rightKeys, rightPointers, False, len(rightKeys), -1) # no next node
			upPointer = self.indexFile.writeBucket(-1, newNode)
			node = NodeBPlus(self.column, leftKeys, leftPointers, False, len(leftKeys), -1)
			self.indexFile.writeBucket(nodePos, node)
			self.logger.info(f"node intern spplitted into left node with keys: {node.keys} and right node with keys: {newNode.keys}")

			return True, upKey, upPointer
	
	def getAll(self) -> list[int]:
		self.logger.warning(f"GET ALL RECORDS")
		firstPos:int = self.indexFile.getHeader()
		if firstPos == -1:
			self.logger.info(f"File: {self.indexFile.filename} is empty: []")
			return []
		
		node:NodeBPlus = self.indexFile.readBucket(firstPos)
		while(not node.isLeaf):
			firstPos = node.pointers[0]
			node = self.indexFile.readBucket(firstPos)

		pointers: list[int] = []
		while(True):
			assert(node.isLeaf)
			for i in range(node.size):
				pointers.append(node.pointers[i])
			if(node.nextNode == -1): break
			node = self.indexFile.readBucket(node.nextNode)

		self.logger.info(f"Successful operation, found records with ids: {pointers}")
		self.printBuckets()
		return pointers
	
	def search(self, key:any) -> list[int]:
		self.logger.warning(f"SEARCHING: {key}")
		rootPos = self.indexFile.getHeader()
		if(rootPos == -1):
			self.logger.fileIsEmpty(self.indexFile.filename)
			self.logger.info(f"NOT FOUND record with id: {key}")
			return []

		return self.rangeSearchAux(key, key)

	def rangeSearch(self, ini:any, end:any) -> list[int]:
		if(ini == None):
			ini = utils.get_min_value(self.column)
		if(end == None):
			end = utils.get_max_value(self.column)
		self.logger.warning(f"RANGE-SEARCH: {ini}, {end}")

		return self.rangeSearchAux(ini, end)

	def delete(self, key:any):
		self.logger.warning(f"DELETING: {key}")
		pass

	def rangeSearchAux(self, ini, end) -> list[int]:
		rootPos = self.indexFile.getHeader()
		if(rootPos == -1):
			self.logger.fileIsEmpty(self.indexFile.filename)
			self.logger.info(f"NOT FOUND records in range start: {ini} and end: {end}")
			return []
		
		leafPos = self.searchAux(rootPos, ini)
		
		ite = 0
		result = []
		leafNode = self.indexFile.readBucket(leafPos)
		while(ite < leafNode.size and leafNode.keys[ite] < ini):
			ite += 1

		if(leafNode.nextNode != -1 and ite == leafNode.size):
			leafNode = self.indexFile.readBucket(leafNode.nextNode)
			ite = 0
			
		while(leafNode.keys[ite] <= end):
			result.append(leafNode.pointers[ite])
			ite += 1
			if(ite == leafNode.size):
				if(leafNode.nextNode == -1):
					break
				leafNode = self.indexFile.readBucket(leafNode.nextNode)
				ite = 0
		return result
	
	def searchAux(self, nodePos:int, key) -> int:
		node:NodeBPlus = self.indexFile.readBucket(nodePos)
		if(node.isLeaf):
			return nodePos
		else:
			self.logger.info(f"Searching in internal node: key={key}")
			ite = 0
			while(ite < node.size and node.keys[ite] < key):
				ite += 1
			if(ite < node.size and node.keys[ite] == key):
				ite += 1
			self.logger.info(f"Going to pointer: {node.pointers[ite]}")
			return self.searchAux(node.pointers[ite], key)
	
	def clear(self):
		self.logger.info("Cleaning data, removing files")
		os.remove(self.indexFile.filename)

	def printBuckets(self):
		rootPos = self.indexFile.getHeader()
		if rootPos == -1:
			print("Tree is empty.")
			return

		queue = [(rootPos, 0)]  # (position, level)
		currentLevel = 0

		print(f"Level {currentLevel}: ", end="")
		while queue:
			nodePos, level = queue.pop(0)
			node = self.indexFile.readBucket(nodePos)

			if level != currentLevel:
				currentLevel = level
				print()  # salto de línea
				print(f"Level {currentLevel}: ", end="")

			# mostrar el bucket
			keys = [k for k in node.keys if k != self.empty_key]
			print(f" {keys}", end="  ")

			# meter hijos a la cola
			if not node.isLeaf:
				for ptr in node.pointers[:node.size+1]:
					if ptr != -1:
						queue.append((ptr, level + 1))
