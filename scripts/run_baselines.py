import os
from scripts import configs

def execute(file_path, obj_type, params):
  tokens = [
    'python', 'rankedvq/app/multilabel_baseline_app.py',
    '--file_path', 'data/{}.txt'.format(file_path),
    '--read_type', obj_type,
    '--num', str(params['num']),
    '--w', str(params['w']),
    '--output_path', configs.baseline_output_template.format(
      video_file=file_path,
      type=obj_type,
      w=params['w'],
      num=params['num'],
    )
  ]
  command = ' '.join(tokens)
  print('executing: ', command)
  os.system(command)

def eval_video(file_path, obj_type):
  # use default settings
  settings = dict(configs.default_settings)
  
  # default setting.
  execute(file_path, obj_type, settings)

  for key in ['w', 'num']:
    settings = dict(configs.default_settings)
    for value in configs.vary_params[key]:
      settings[key] = value
      execute(file_path, obj_type, settings)

if __name__ == '__main__':
  for video_file, obj_type in configs.video_files:
    print("processing video [{}] with type [{}]".format(video_file, obj_type))
    eval_video(video_file, obj_type)