from rankedvq.online.topk_holder import TopkBookKeeperBreakTie
from rankedvq.bitarray import bitarray

def init_window_score_arr(pg_start, pg_end, w, start_window):
  size = pg_end - pg_start - w + 2 - start_window
  return [0] * size if size > 0 else [0]

def map_bitset(bitset, mapping, size):
  mapped = bitarray(size)
  mapped.setall(0)
  for key, value in mapping.items():
    if bitset[key] == 1:
      mapped[value] = 1
  return mapped

def select_bitset_objs(objs, bitset):
  selected = [objs[pos] for pos in bitset.itersearch(1)]
  # _start = 0
  # selected = []
  # while True:
  #   pos = bitset.find(1, _start)
  #   if pos < 0:
  #     break
  #   selected.append(objs[pos])
  #   _start = pos + 1
  return selected
  # return [obj for i,obj in enumerate(objs) \
  #         if bitset[i] == 1]

def count_frames_in_interval(intervals):
  count = 0
  for interval in intervals:
    count += interval[1] - interval[0] + 1
  return count

def update_topk(bookkeeper, window_score_arr, base_idx, payload_arr=None):
  for i in range(len(window_score_arr)):
    if window_score_arr[i] >= bookkeeper.min:
      bookkeeper.update(base_idx+i, window_score_arr[i], \
        None if payload_arr is None else payload_arr[i])