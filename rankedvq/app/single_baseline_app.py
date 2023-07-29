from rankedvq.io import obtain_frames_as_obj_sets, read_type_grouped_file
from rankedvq.baseline.single_baseline import compute_all_window_scores
import time
import argparse

def run_single_type_baseline(file_path, obj_type, obj_num, w):
  frames = read_type_grouped_file(file_path)
  frames_w_obj_set = obtain_frames_as_obj_sets(frames, obj_type)
  start = time.process_time()
  window_scores = compute_all_window_scores(frames_w_obj_set, obj_num, w)
  end = time.process_time()
  print('time', end - start)
  print('window_scores', window_scores)

if __name__ == '__main__':
  # args.
  # parser = argparse.ArgumentParser(description='run baseline')
  # parser.add_argument('file_path')
  # parser.add_argument('--obj_type', default='person')
  # parser.add_argument('--obj_num', default='6')
  # run_single_type_baseline('data/news1.txt', 'person', 6, 300)
  run_single_type_baseline('data/MOT16-06.txt', 'person', 6, 300)
