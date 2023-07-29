from rankedvq.io import grouped_frames_to_multi_labels, read_type_grouped_file, write_multi_label_file
from scripts import configs

def convert_data(video_file):
  print('converting data', video_file)
  frames = read_type_grouped_file(configs.dataset_grouped_template.format(video_file = video_file))
  frames = grouped_frames_to_multi_labels(frames)
  write_multi_label_file(frames, configs.dataset_multilabel_template.format(video_file=video_file))

if __name__ == '__main__':
  for video_file, _ in configs.video_files:
    convert_data(video_file)