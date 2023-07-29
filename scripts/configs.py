default_settings_1 = dict(
  w=600,
  p=600,
  num=6,
  k=10,
  pg=-1
)

default_settings_2 = dict(
  w = 900,
  p = 300,
  num=6,
  k=10,
  pg=-1
)

vary_params_1 = dict(
  w = [300, 900, 1200, 1500],
  p = [300, 900, 1200, 1500],
  num = [2, 4, 8, 10],
  k=[1, 100, 1000, 2000],
)

vary_params_2 = dict(
  pg = [4, 8, 12, 16]
)

video_files = [
  ('news1', 'person'), 
  ('news2', 'person'), 
  ('news3', 'person'), 
  ('traffic1', 'car'), 
  ('traffic2', 'car'), 
  ('traffic3', 'car'),
  ('ff', 'person'), 
  ('midway', 'person'), 
  ('inception', 'person'), 
  ('d1', 'car'), 
  ('d2', 'car'), 
  ('d3', 'car'),
]

baseline_output_template = './results/{video_file}/baseline-{type}-{w}-{num}.txt'

baseline_multi_output_template = './results/{video_file}_multi/baseline-{type}-{w}-{query_id}.txt'

proposed_output_template = './results/{video_file}/oursa-{type}-{w}-{p}-{num}-{k}-{pg}.txt'

proposed_multi_output_template = './results/{video_file}_multi/oursa-{type}-{w}-{p}-{query_id}-{k}-{pg}.txt'

dataset_with_colors_template = './data/with_colors/{video_file}.txt'

dataset_grouped_template = './data/{video_file}.txt'

dataset_multilabel_template = './data/multi_label/{video_file}.txt'


multi_settings_0 = dict(
  w=600,
  p=600,
  query_id=0,
  k=100,
  pg=0,
  file_path_template = dataset_with_colors_template
)

multi_settings_1 = dict(
  w=600,
  p=600,
  query_id=1,
  k=100,
  pg=0,
  file_path_template = dataset_multilabel_template
)
