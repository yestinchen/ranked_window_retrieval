from scripts import configs

def validate_result(top_k_dict, all_window_scores):
  # sort
  window_scores = [(k,v) for k, v in all_window_scores.items()]
  window_scores.sort(key=lambda x: x[1], reverse=True)

  for i, (k, v) in enumerate(top_k_dict.items()):
    assert v == window_scores[i][1], \
      "found error result at position {}, expecting score: {}(window {}), got: {} (window {})"\
        .format(i, window_scores[i][1], window_scores[i][0], v, k)

def read_results_as_dict(file_path):
  results = dict()
  with open(file_path, 'r') as f:
    for line in f.readlines():
      if ';' in line:
        line = line.split(';')[0]
      arr = line.split(':')
      results[int(arr[0])] = int(arr[1])
  return results

def validate_multi(video_file, obj_type):
  baseline_template = configs.baseline_multi_output_template
  proposed_template = configs.proposed_multi_output_template
  settings = dict(configs.multi_settings_0)
  validate_with_baseline(video_file, obj_type, settings, baseline_template, proposed_template)
  settings = dict(configs.multi_settings_1)
  validate_with_baseline(video_file, obj_type, settings, baseline_template, proposed_template)

def validate_single(video_file, obj_type):
  baseline_template = configs.baseline_output_template
  proposed_template = configs.proposed_output_template
  settings = dict(configs.default_settings)
  validate_with_baseline(video_file, obj_type, settings, baseline_template, proposed_template)

  # vary w, num
  for key in configs.vary_params.keys():
    settings = dict(configs.default_settings)
    for value in configs.vary_params[key]:
      settings[key] = value
      validate_with_baseline(video_file, obj_type, settings, baseline_template, proposed_template)

def validate_with_baseline(video_file, obj_type, params, baseline_template, proposed_template):
  print('validating with baseline', video_file, 'with', params)

  settings = params
  baseline_result_path = baseline_template.format(
    video_file = video_file,
    type=obj_type,
    **settings
  )

  baseline_result = read_results_as_dict(baseline_result_path)

  print('baseline path: ', baseline_result_path)

  our_result_path = proposed_template.format(
    video_file = video_file,
    type=obj_type,
    **settings,
  )

  print('check with our output: ', our_result_path)
  our_result = read_results_as_dict(our_result_path)

  validate_result(our_result, baseline_result)

if __name__ == '__main__':
  for video_file, obj_type in [
    ('news1', 'person'), 
    ('news2', 'person'), 
    ('news3', 'person'),
    ('traffic1', 'car'), 
    ('traffic2', 'car'), 
    ('traffic3', 'car'),
    ('ff', 'person'), 
    ('inception', 'person'), 
    ('midway', 'person'), 
    # ('joker', 'person'),
    ('d1', 'car'), 
    ('d2', 'car'), 
    ('d3', 'car'),
    ]:
    
    # validate_multi(video_file, obj_type)
    validate_single(video_file, obj_type)
  # validate_result(
  #   read_results_as_dict('out/test_news2_out-1000.txt'), 
  #   read_results_as_dict('results/news2/baseline-person-600-6.txt')
  # )
  print('validation complete')