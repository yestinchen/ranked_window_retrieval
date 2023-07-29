from collections import defaultdict
from rankedvq.io import filter_frames_with_types, grouped_frames_to_multi_labels, filter_frames_with_types, read_type_grouped_file
from rankedvq.offline.multilabel_bitset_builder import MultiLabelBitsetIndexBuilder
from rankedvq.online.multi_label_processor import BitsetMultiLabelProcessor
# from rankedvq.online.multi_label_processor_w_buffer import BitsetMultiLabelProcessor
import time
import logging
import cProfile
import argparse
import os

def run_multilabel_bitset_index(file_path, read_type, p_size, obj_num, w, k, pg_size, output_path):
  if pg_size is not None and pg_size <= 0:
    pg_size = None

  # logging.basicConfig(level=logging.INFO)

  frames = read_type_grouped_file(file_path)
  # filter based on type
  frames_with_read_type = filter_frames_with_types(frames, [read_type])
  
  frames_multi_labels = grouped_frames_to_multi_labels(frames_with_read_type)

  start = time.process_time()
  builder = MultiLabelBitsetIndexBuilder()
  index = builder.build(frames_multi_labels, defaultdict(lambda: frozenset([read_type])), p_size)
  end = time.process_time()

  build_time = end - start
  # print('build done, time: ', build_time)
  # print('partitions num:', len(index))

  start = time.process_time()
  query = [
    [(set([read_type]), '>=', obj_num)]
  ]
  processor = BitsetMultiLabelProcessor(index, p_size, len(frames), True)
  execute_times = 3
  for i in range(execute_times):
    results = processor.topk(query, k, w, pg_size)
  end = time.process_time()

  query_time = (end - start)/execute_times

  # print('time', build_time, query_time)
  # print('len result', len(results))
  # print('result', results)
  # for window, score, _ in results:
  #   if window == 77633:
  #     print(window, score)
  # print('result', results[391])
  
  output_parent = os.path.dirname(output_path)
  if not os.path.exists(output_parent):
    os.makedirs(output_parent)

  with open(output_path, 'w') as f:
    for result in results:
      f.write('{}:{};{}\n'.format(result[0], result[1], result[2]))
  
  # write report.
  with open(output_path+'.report', 'w') as f:
    f.write('build time: {}\n'.format(build_time))
    f.write('query time: {}\n'.format(query_time))
    if processor.metrics is not None:
      for key, value in processor.metrics.data.items():
        f.write('metrics {}: {}\n'.format(key, value))


if __name__ == '__main__':
  # cProfile.run("run_multilabel_bitset_index('data/MOT16-06.txt', 'person', 200, 6, 300, 600, None)")
  parser = argparse.ArgumentParser("app")
  parser.add_argument('--file_path')
  parser.add_argument('--read_type')
  parser.add_argument('--p', type=int)
  parser.add_argument('--num', type=int)
  parser.add_argument('--w', type=int)
  parser.add_argument('--k', type=int)
  parser.add_argument('--pg', type=int)
  parser.add_argument('--output_path')
  args = parser.parse_args()

  run_multilabel_bitset_index(
    args.file_path,
    args.read_type,
    args.p,
    args.num, 
    args.w,
    args.k,
    args.pg,
    args.output_path
  )
