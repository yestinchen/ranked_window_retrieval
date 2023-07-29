import os
from scripts import configs

def execute(file_path, obj_type, params):
  tokens = [
    'python', 'rankedvq/app/multilabel_type_app.py',
    '--file_path', 'data/{}.txt'.format(file_path),
    '--read_type', obj_type,
    '--p', str(params['p']),
    '--num', str(params['num']),
    '--w', str(params['w']),
    '--k', str(params['k']),
    '--pg', str(params['pg']),
    '--output_path', configs.proposed_output_template.format(
      video_file=file_path,
      type=obj_type,
      w=params['w'],
      num=params['num'],
      p=params['p'],
      k=params['k'],
      pg=params['pg']
    )
  ]
  command = ' '.join(tokens)
  print('executing: ', command)
  os.system(command)

def eval_video(file_path, obj_type, default_settings, vary_params):
  # use default settings
  settings = dict(default_settings)
  
  # default setting.
  execute(file_path, obj_type, settings)

  for key in vary_params.keys():
    settings = dict(default_settings)
    for value in vary_params[key]:
      settings[key] = value
      execute(file_path, obj_type, settings)

if __name__ == '__main__':
  for video_file, obj_type in configs.video_files:
    print("processing video [{}] with type [{}]".format(video_file, obj_type))
    eval_video(video_file, obj_type, configs.default_settings_1, configs.vary_params_1)
    eval_video(video_file, obj_type, configs.default_settings_2, configs.vary_params_2)