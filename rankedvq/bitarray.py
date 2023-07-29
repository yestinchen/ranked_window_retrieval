from bitarray import bitarray as _bitarray
class bitarray(_bitarray):
  def __hash__(self):
    return self.tobytes().__hash__()