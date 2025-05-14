import logging


class CustomLogger:
	def __init__(self, name):
		self.logger = logging.getLogger(name)
		self.logger.setLevel(logging.WARNING)

		# 👇 Check to avoid duplicate handlers
		if not self.logger.handlers:
			handler = logging.StreamHandler()
			formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
			handler.setFormatter(formatter)
			self.logger.addHandler(handler)
	
	def foundRecord(self, file, pos, id):
		file = file.split("\\")[-1]
		self.logger.info(f"Found record with id: {id} in file: {file} at position: {pos}")

	def notFoundRecord(self, file, pos):
		file = file.split("\\")[-1]
		self.logger.warning(f"Not found record at position: {pos} in file: {file}")
	
	def invalidPosition(self, file, pos):
		file = file.split("\\")[-1]
		self.logger.error(f"Trying to access invalid position: {pos} in file: {file}")

	def writingRecord(self, file, pos, id, nextDel):
		file = file.split("\\")[-1]
		self.logger.info(f"Writing record with id: {id} in file: {file} at position: {pos} with NextDel: {nextDel}")

	def fileIsEmpty(self, file):
		file = file.split("\\")[-1]
		self.logger.warning(f"File: {file} is empty, initializing it")
	
	def fileNotFound(self, file):
		file = file.split("\\")[-1]
		self.logger.warning(f"File: {file} doesn't exists, initializing it")

	def writingBucket(self, file, pos, keys):
		file = file.split("\\")[-1]
		self.logger.info(f"Writing bucket in file: {file} at position: {pos} with keys: {keys}")

	def readingBucket(self, file, pos, keys):
		file = file.split("\\")[-1]
		self.logger.info(f"Reading bucket from file: {file} at position: {pos} with keys: {keys}")

	def writingHeader(self, file, rootPos):
		file = file.split("\\")[-1]
		self.logger.info(f"Writing root position: {rootPos} into header of file: {file}")

	def readingHeader(self, file, rootPos):
		file = file.split("\\")[-1]
		self.logger.info(f"Reading root position: {rootPos} from header of file: {file}")

	def successfulInsertion(self, file, id):
		file = file.split("\\")[-1]
		self.logger.info(f"Successful insertion on file: {file} of record with id: {id}")

	def patchingNextDel(self, file, id, next_del):
		file = file.split("\\")[-1]
		self.logger.info(f"Patched NextDel of record with id: {id} in file: {file} to: {next_del}")

	def readingNode(self, file, pos): #AVL
		file = file.split("\\")[-1]
		self.logger.info(f"Reading node from file: {file} at position: {pos}")

	def writingNode(self, file, pos, val, right, left, height): # AVL
		file = file.split("\\")[-1]
		self.logger.info(f"Writing node in file: {file} at position: {pos} with val: {val}, right: {right}, left: {left}, height: {height}")
	def warning(self, text):
		self.logger.warning(text)
	
	def error(self, text):
		self.logger.error(text)
	
	def info(self, text):
		self.logger.info(text)
	
	def debug(self, text):
		self.logger.debug(text)