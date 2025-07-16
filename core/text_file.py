import struct

class TextFile:
    def __init__(self, file_path):
        self.data_path = f"{file_path}_data.dat"
        self.lengths_path = f"{file_path}_lengths.dat"
    
    def initialize(self):
        with open(self.data_path, "wb") as _:
            pass
        with open(f"{self.lengths_path}_lengths.dat", "wb") as _:
            pass
    
    def read(self, pos: int, length: int) -> str:
        with open(self.data_path, "rb") as f:
            f.seek(pos)
            text = f.read(length).decode('utf-8')
            return text
    
    def write(self, text: str) -> tuple[int, int]:
        with open(self.data_path, "ab") as f:
            pos = f.tell()
            encoded_text = text.encode('utf-8')
            length = len(encoded_text)
            f.write(encoded_text)
        with open(self.lengths_path, "ab") as f:
            f.write(struct.pack('i', length))
        return (pos, length)
    
    def read_all(self) -> list[str]:
        res = []
        with open(self.data_path, "rb") as data:
            with open(self.lengths_path, "rb") as lengths:
                while True:
                    packed_length = lengths.read(4)
                    if not packed_length:
                        break
                    length = struct.unpack('i', packed_length)[0]
                    text = data.read(length).decode('utf-8')
                    res.append(text)
        return res
