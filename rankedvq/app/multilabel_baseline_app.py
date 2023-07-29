import time
import os
import argparse
from rankedvq.io import filter_frames_with_types, grouped_frames_to_multi_labels, read_type_grouped_file
from rankedvq.baseline.multi_baseline import compute_all_window_scores


def run_multilabel_baseline(file_path, obj_type, obj_num, w, output_path):
  frames = read_type_grouped_file(file_path)
  frames_with_read_type = filter_frames_with_types(frames, [obj_type])
  frames_multi_labels = grouped_frames_to_multi_labels(frames_with_read_type)

  query = [
    [(set([obj_type]), '>=', obj_num)]
  ]

  start = time.process_time()
  window_scores = compute_all_window_scores(frames_multi_labels, query, w)
  end = time.process_time()
  # print('time', end - start)
  # print('scores', window_scores)
  # print('min score:', min(window_scores.values()))

  output_parent = os.path.dirname(output_path)
  if not os.path.exists(output_parent):
    os.makedirs(output_parent)
  
  with open(output_path, 'w') as f:
    for window, score in window_scores.items():
      f.write('{}:{}\n'.format(window, score))
  
  with open(output_path+'.report', 'w') as f:
    f.write('time: {}'.format(end - start))

if __name__ == '__main__':
  # run_multilabel_baseline('data/MOT16-06.txt', 'person', 6, 300)
  parser = argparse.ArgumentParser("baseline")
  parser.add_argument('--file_path')
  parser.add_argument('--read_type')
  parser.add_argument('--num', type=int)
  parser.add_argument('--w', type=int)
  parser.add_argument('--output_path')
  args = parser.parse_args()

  run_multilabel_baseline(
    args.file_path,
    args.read_type,
    args.num,
    args.w,
    args.output_path
  )
