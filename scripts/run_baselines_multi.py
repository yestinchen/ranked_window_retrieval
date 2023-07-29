import os
from scripts import configs

def execute(file_path, obj_type, params):
  tokens = [
    'python', 'rankedvq/app/multilabel_baseline_multi_app.py',
    '--file_path', params['file_path_template'].format(video_file=file_path),
    '--read_type', obj_type,
    '--query_id', str(params['query_id']),
    '--w', str(params['w']),
    '--output_path', configs.baseline_multi_output_template.format(
      video_file=file_path,
      type=obj_type,
      w=params['w'],
      query_id=params['query_id'],
    )
  ]
  command = ' '.join(tokens)
  print('executing: ', command)
  os.system(command)

def eval_video(file_path, obj_type):
  
  # default setting.
  execute(file_path, obj_type, configs.multi_settings_0)
  execute(file_path, obj_type, configs.multi_settings_1)

  # for key in ['w', 'num']:
  #   settings = dict(configs.default_settings)
  #   for value in configs.vary_params[key]:
  #     settings[key] = value
  #     execute(file_path, obj_type, settings)

if __name__ == '__main__':
  for video_file, obj_type in configs.video_files:
    print("processing video [{}] with type [{}]".format(video_file, obj_type))
    eval_video(video_file, obj_type)