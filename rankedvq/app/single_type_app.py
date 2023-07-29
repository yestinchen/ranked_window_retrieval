from collections import defaultdict
from rankedvq.io import obtain_frames_as_obj_sets, read_type_grouped_file
from rankedvq.offline.bitset_builder import BitsetIndexBuilder
from rankedvq.online.bitset_single_processor import BitsetSingleProcessor
import time
import logging
import cProfile

def run_single_type_bitset_index(file_path, read_type, p_size, obj_num, w, k, pg):

  # logging.basicConfig(level=logging.INFO)

  frames = read_type_grouped_file(file_path)
  frames_w_obj_set = obtain_frames_as_obj_sets(frames, read_type)

  start = time.process_time()
  builder = BitsetIndexBuilder()
  index = builder.build(frames_w_obj_set, defaultdict(lambda: read_type), p_size, False)
  end = time.process_time()

  build_time = end - start
  print('build done, time: ', build_time)
  print('partitions num:', len(index))

  start = time.process_time()
  processor = BitsetSingleProcessor(index, p_size, len(frames), True)
  results = processor.topk(k, w, obj_num, pg)
  end = time.process_time()

  query_time = end - start
  
  print('time', build_time, query_time)
  print('len result', len(results))
  # print('result', results)
  # print('result', results[767])
  

if __name__ == "__main__":
  # cProfile.run("run_single_type_bitset_index('data/MOT16-06.txt', 'person', 200, 6, 300, 1, None)")
  run_single_type_bitset_index('data/d3.txt', 'car', 600, 6, 1500, 10, None)
  # cProfile.run("run_single_type_bitset_index('data/d3.txt', 'car', 600, 6, 1500, 10, None)")
  # run_single_type_bitset_index('data/MOT16-06.txt', 'person', 200, 2, 300, 100, None)
