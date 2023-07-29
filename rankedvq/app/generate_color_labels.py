# generate color labels.
import random
import argparse
from rankedvq.io import filter_frames_with_types, grouped_frames_to_multi_labels, read_type_grouped_file, write_multi_label_file

def generate_color_labels(file_path, read_type, labels, seed, output_path):
  frames = read_type_grouped_file(file_path)
  if read_type is not None:
    frames = filter_frames_with_types(frames, [read_type])
  
  frames_labels = grouped_frames_to_multi_labels(frames)
  obj_color_dict = dict()

  if seed is not None:
    random.seed(seed)    

  converted_frames = []
  for frame in frames_labels:
    new_frame = dict()
    for obj, label_set in frame.items():
      if obj not in obj_color_dict:
        obj_color_dict[obj] = random.choice(labels)
      new_frame[obj] = frozenset(label_set).union([obj_color_dict[obj]])
    converted_frames.append(new_frame)
  
  # write back to file.
  write_multi_label_file(converted_frames, output_path)


if __name__ == '__main__':
  parser = argparse.ArgumentParser("generate data with colors")
  parser.add_argument('--file_path')
  parser.add_argument('--read_type')
  parser.add_argument('--labels')
  parser.add_argument('--seed', type=int)
  parser.add_argument('--output_path')
  args = parser.parse_args()

  generate_color_labels(
    args.file_path,
    args.read_type,
    [x.strip() for x in args.labels.split(',')],
    args.seed,
    args.output_path
  )
