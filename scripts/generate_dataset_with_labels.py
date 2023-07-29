from scripts import configs
import os

generate_labels = ['blue', 'black', 'white', 'red', 'yellow']
seed = 42

def generate_dataset(video_file, read_type):
  print('generating color labels for [{}] with type [{}]'.format(video_file, read_type))
  tokens = [
    'python', 'rankedvq/app/generate_color_labels.py',
    '--file_path', configs.dataset_grouped_template.format(video_file=video_file),
    '--read_type', read_type,
    '--labels', ','.join(generate_labels),
    '--seed', str(seed),
    '--output_path', configs.dataset_with_colors_template.format(video_file=video_file)
  ]
  command = ' '.join(tokens)
  print('executing ', command)
  os.system(command)  
  

if __name__ == '__main__':
  for video_file, read_type in configs.video_files:
    generate_dataset(video_file, read_type)