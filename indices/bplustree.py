import struct
import os
import sys 
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logger
from core.record_file import RecordFile, Record

class NodeBPlus:
	BLOCK_FACTOR = 3
	FORMAT = "i" + "ii" * BLOCK_FACTOR + "iii"
	NODE_SIZE = struct.calcsize(FORMAT)
	def __init__(self, keys:list = [], pointers:list = [], isLeaf:bool = False, size:int = 0, nextNode:int = -1):
		if isLeaf:
			if len(pointers) != len(keys):
				raise Exception("Creating leaf node, number of keys and pointers must be equal")
		else:
			if len(pointers) != len(keys) + 1:
				raise Exception("Creating internal node, number of pointers must be one more than number of keys")
		
		while len(keys) < self.BLOCK_FACTOR:
			keys.append(-1)
		while len(pointers) < self.BLOCK_FACTOR + 1:
				pointers.append(-1)
		
		self.keys = keys
		self.pointers = pointers
		self.isLeaf = isLeaf
		self.size = size
		self.nextNode = nextNode
		self.logger = logger.CustomLogger("NODEBPLUS")
	
	def addLeafId(self, key:int, pointer:int):
		self.logger.debug(f"Adding id: {key} and pointer: {pointer} in bucket")
		#if len(self.pointers) != len(self.keys):
		#	raise Exception("In leaf node, number of keys and pointers must be equal")
		
		if not self.isFull():
			self.keys[self.size] = key
			self.pointers[self.size] = pointer
			self.size += 1
		else:
			raise Exception("Node is full")
	
	def addInternalId(self, key:int, pointer:int):
		self.logger.debug(f"Adding id: {key} and pointer: {pointer} in bucket")
		if len(self.pointers) != len(self.keys) + 1:
			raise Exception("In intern node, number of keys and pointers must be differ in 1")
		
		if not self.isFull():
			self.keys[self.size] = key
			self.pointers[self.size+1] = pointer
			self.size += 1
		else:
			raise Exception("Node is full")
	
	def insertInLeaf(self, key: int, pointer: int):
		assert(self.isLeaf)
		self.logger.debug(f"Inserting in leaf: key={key}, pointer={pointer}")
		self.addLeafId(key, pointer)

		i = self.size - 1
		while i > 0 and self.keys[i] < self.keys[i - 1]:
			self.keys[i], self.keys[i - 1] = self.keys[i - 1], self.keys[i]
			self.pointers[i], self.pointers[i - 1] = self.pointers[i - 1], self.pointers[i]
			i -= 1

	def insertInInternalNode(self, key: int, rightChildPtr: int):
		assert(not self.isLeaf)
		self.logger.debug(f"Inserting in internal self: key={key}, rightPtr={rightChildPtr}")
		self.addInternalId(key, rightChildPtr)

		i = self.size - 1
		while i > 0 and self.keys[i] < self.keys[i - 1]:
			self.keys[i], self.keys[i - 1] = self.keys[i - 1], self.keys[i]
			self.pointers[i+1], self.pointers[i] = self.pointers[i], self.pointers[i + 1]
			i -= 1

	
	def deleteLeafId(self, key: int) -> int:
		self.logger.debug(f"Deleting key: {key} from leaf node")
		found = False
		deletePos = -1
		for i in range(self.size):
			if self.keys[i] == key:
				found = True
				deletePos = self.pointers[i]
				for j in range(i, self.size - 1):
					self.keys[j] = self.keys[j + 1]
					self.pointers[j] = self.pointers[j + 1]
				self.keys[self.size - 1] = -1
				self.pointers[self.size - 1] = -1
				self.size -= 1
				break
		if not found:
			self.logger.debug(f"Key {key} not found in leaf node")
		return deletePos

	def deleteInternalId(self, key: int):
		self.logger.debug(f"Deleting key: {key} from internal node")
		found = False
		for i in range(self.size):
			if self.keys[i] == key:
				found = True
				for j in range(i, self.size - 1):
					self.keys[j] = self.keys[j + 1]
					self.pointers[j + 1] = self.pointers[j + 2]
				self.keys[self.size - 1] = -1
				self.pointers[self.size] = -1
				self.size -= 1
				break
		if not found:
			self.logger.debug(f"Key {key} not found in internal node")


	def isFull(self) -> bool:
		return self.size == len(self.keys)

	def pack(self) -> bytes:
		data_buf = b''
		for key in self.keys:
			data_buf += struct.pack('i', key)
		for pointer in self.pointers:
			data_buf += struct.pack('i', pointer)
		data_buf += struct.pack('iii', self.isLeaf, self.size, self.nextNode)
		return data_buf

	def debug(self):
		self.logger.debug(f"Node with keys: {self.keys}, pointers: {self.pointers}, isLeaf: {self.isLeaf}, size: {self.size}, nextNode: {self.nextNode}")

	@staticmethod
	def unpack(record:bytes):
		if(record == None):
			raise Exception("record is None")
		isLeaf, size, nextNode = struct.unpack('iii', record[-12:])

		blockFactor = NodeBPlus.BLOCK_FACTOR
		keys = [-1 for _ in range(size)]
		pointers = [-1 for _ in range(size + 1 - isLeaf)]
		for i in range(size):
			keys[i] = struct.unpack('i',record[4*i : 4*(i+1)])[0]
		for i in range(size + 1 - isLeaf):
			pointers[i] = struct.unpack('i',record[4*blockFactor + 4*i : 4*blockFactor + 4*(i+1)])[0]
		
		node = NodeBPlus(keys, pointers, isLeaf, size, nextNode)
		return node

class BPlusFile:
	HEADER_SIZE = 4

	def __init__(self, filename):
		self.filename = filename
		self.logger = logger.CustomLogger("BPLUSFILE")
		self.logger.logger.setLevel(logging.WARNING)

		if not os.path.exists(self.filename):
			self.logger.fileNotFound(self.filename)
			self.initialize_file(filename) # if archive not exists
		else:
			with open(filename, "rb+") as file:
				file.seek(0,2)
				if(file.tell() == 0):
					self.logger.fileIsEmpty(self.filename)
					self.initialize_file(filename) # if archive is empty

	def initialize_file(self, filename):
		with open(filename, "wb") as file:
			header = -1 # root
			file.write(struct.pack("i", header))
	
	def readBucket(self, pos: int) -> NodeBPlus:
		with open(self.filename, "rb") as file:
			offset = self.HEADER_SIZE + pos * NodeBPlus.NODE_SIZE
			file.seek(offset)
			data = file.read(NodeBPlus.NODE_SIZE)
			if not data or len(data) < NodeBPlus.NODE_SIZE:
				self.logger.invalidPosition(self.filename, pos)
				raise Exception(f"Invalid bucket position: {pos}")
			node = NodeBPlus.unpack(data)
			self.logger.readingBucket(self.filename, pos, node.keys)
			return node

	def writeBucket(self, pos: int, node: NodeBPlus) -> int:
		data = node.pack()
		with open(self.filename, "rb+") as file:
			if pos == -1:
				file.seek(0, 2)  # ir al final
				offset = file.tell()
				pos = (offset - self.HEADER_SIZE) // NodeBPlus.NODE_SIZE
			else:
				offset = self.HEADER_SIZE + pos * NodeBPlus.NODE_SIZE
				file.seek(offset)
			file.write(data)
			self.logger.writingBucket(self.filename, pos, node.keys)
			return pos		
		
	def freeBucket(self, pos: int):
		node = NodeBPlus()
		self.writeBucket(pos, node)

	def getHeader(self) -> int:
		with open(self.filename, "rb") as file:
			file.seek(0)
			data = file.read(self.HEADER_SIZE)
			rootPosition = struct.unpack("i", data)[0]
			self.logger.readingHeader(self.filename, rootPosition)
			return rootPosition

	def writeHeader(self, rootPosition: int):
		with open(self.filename, "rb+") as file:
			file.seek(0)
			file.write(struct.pack("i", rootPosition))
			self.logger.writingHeader(self.filename, rootPosition)


class BPlusTree:
	indexFile: BPlusFile
	recordFile: RecordFile

	def __init__(self):
		self.indexFile = BPlusFile("BPlusFile.dat")
		self.recordFile = RecordFile("RecordFile.dat")
		self.BLOCK_FACTOR = NodeBPlus.BLOCK_FACTOR
		self.logger = logger.CustomLogger("BPLUSTREE")
	
	def insert(self, record:Record):
		self.logger.info(f"INSERT record with id: {record.id}")
		pos = self.recordFile.append(record)
		rootPos = self.indexFile.getHeader()
		if(rootPos == -1):
			self.logger.info(f"Creating new root, first record with id: {record.id}")
			root = NodeBPlus(isLeaf=True)
			root.addLeafId(record.id, pos)
			rootPos = self.indexFile.writeBucket(-1, root) # new bucket
			self.indexFile.writeHeader(rootPos)
			return
		
		split, newKey, newPointer = self.insertAux(rootPos, record.id, pos)

		if not split:
			self.logger.successfulInsertion(self.indexFile.filename, record.id)
			return
		
		# ¡SI HAY SPLIT, CREAMOS NUEVA RAIZ DIRECTAMENTE!
		self.logger.info(f"Root was split, Creating new root")
		newRoot = NodeBPlus(
			keys=[newKey],
			pointers=[rootPos, newPointer],
			isLeaf=False,
			size=1
		)
		newRootPos = self.indexFile.writeBucket(-1, newRoot)
		self.indexFile.writeHeader(newRootPos)
		self.logger.info(f"New root created with keys: {newRoot.keys}")
		self.logger.successfulInsertion(self.indexFile.filename, record.id)
	
	def insertAux(self, nodePos:int, key:int, pointer:int) -> tuple[bool, int, int]: # split?, key, pointer
		node:NodeBPlus = self.indexFile.readBucket(nodePos)
		if(node.isLeaf): # if is leaf, insert
			node.insertInLeaf(key, pointer)
			if(not node.isFull()):
				self.indexFile.writeBucket(nodePos, node)
				self.logger.info(f"node leaf with keys: {node.keys} is not full, not splitting")
				return False, -1, -1
			
			self.logger.info(f"node leaf is full, splitting node with keys: {node.keys}")
			mid = node.size // 2
			leftKeys, rightKeys = node.keys[:mid], node.keys[mid:]
			leftPointers, rightPointers = node.pointers[:mid], node.pointers[mid:-1]
			newNode = NodeBPlus(rightKeys, rightPointers, True, len(rightKeys), node.nextNode)
			pos = self.indexFile.writeBucket(-1, newNode)
			node = NodeBPlus(leftKeys, leftPointers, True, len(leftKeys), pos)
			self.indexFile.writeBucket(nodePos, node)
			self.logger.info(f"node leaf spplitted into left node with keys: {node.keys} and right node with keys: {newNode.keys}")

			return True, newNode.keys[0], pos

		else:
			ite = 0
			while(ite < node.size and node.keys[ite] < key): # finding where to insert id
				ite += 1
			split, newKey, newPointer = self.insertAux(node.pointers[ite], key, pointer)

			if not split:
				return False, -1, -1
			
			node.insertInInternalNode(newKey, newPointer)

			if not node.isFull():
				self.indexFile.writeBucket(nodePos, node)
				self.logger.info(f"node intern with keys: {node.keys} is not full, not splitting")
				return False, -1, -1
			
			self.logger.info(f"node intern is full, splitting node with keys: {node.keys}")
			mid = node.size // 2
			leftKeys, rightKeys = node.keys[:mid], node.keys[mid+1:] # split keys but one is going up
			upKey = node.keys[mid]
			leftPointers, rightPointers = node.pointers[:mid+1], node.pointers[mid+1:] # pointers split but maintain for them
			
			newNode = NodeBPlus(rightKeys, rightPointers, False, len(rightKeys), -1) # no next node
			upPointer = self.indexFile.writeBucket(-1, newNode)
			node = NodeBPlus(leftKeys, leftPointers, False, len(leftKeys), -1)
			self.indexFile.writeBucket(nodePos, node)
			self.logger.info(f"node intern spplitted into left node with keys: {node.keys} and right node with keys: {newNode.keys}")

			return True, upKey, upPointer
	
	def getAll(self) -> list[int]:
		self.logger.info(f"GET ALL RECORDS")
		firstPos:int = self.indexFile.getHeader()
		if firstPos == -1:
			self.logger.info(f"File: {self.indexFile.filename} is empty: []")
			return []
		
		node:NodeBPlus = self.indexFile.readBucket(firstPos)
		while(not node.isLeaf):
			firstPos = node.pointers[0]
			node = self.indexFile.readBucket(firstPos)
		
		records:list[Record] = []
		ids:list[int] = []
		while(True):
			assert(node.isLeaf)
			for i in range(node.size):
				records.append(self.recordFile.read(node.pointers[i]))
				ids.append(records[-1].id)
			if(node.nextNode == -1): break
			node = self.indexFile.readBucket(node.nextNode)

		self.logger.info(f"Successful operation, found records with ids: {ids}")
		return ids
	
	def search(self, id:int) -> Record | None:
		self.logger.info(f"SEARCH record with id: {id}")
		rootPos = self.indexFile.getHeader()
		if(rootPos == -1):
			self.logger.fileIsEmpty(self.recordFile.filename)
			self.logger.info(f"NOT FOUND record with id: {id}")
			return None
		
		record = self.searchAux(rootPos, id)
		if(record):
			self.logger.info(f"FOUND record with id: {id}")
			record.debug()
		else:
			self.logger.info(f"NOT FOUND record with id: {id}")
		return record
	
	def searchAux(self, nodePos:int, key:int) -> Record | None:
		node:NodeBPlus = self.indexFile.readBucket(nodePos)
		if(node.isLeaf):
			self.logger.info(f"Searching in leaf: key={key}")
			for i in range(node.size):
				if(node.keys[i] == key):
					self.logger.info(f"Record with key={key} was found on leaf")
					return self.recordFile.read(node.pointers[i])
			self.logger.info(f"Record with key={key} was not found on leaf")
			return None

		else:
			self.logger.info(f"Searching in internal node: key={key}")
			ite = 0
			while(ite < node.size and node.keys[ite] <= key):
				ite += 1
			self.logger.info(f"Going to pointer: {node.pointers[ite]}")
			return self.searchAux(node.pointers[ite], key)
	
	def clear(self):
		self.logger.info("Cleaning data, removing files")
		os.remove(self.indexFile.filename)
		os.remove(self.recordFile.filename)

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
			keys = [k for k in node.keys if k != -1]
			print(f" {keys}", end="  ")

			# meter hijos a la cola
			if not node.isLeaf:
				for ptr in node.pointers[:node.size+1]:
					if ptr != -1:
						queue.append((ptr, level + 1))
		print()
