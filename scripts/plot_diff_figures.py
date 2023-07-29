from collections import defaultdict
import os
import seaborn as sns
import matplotlib.pyplot as plt
from scripts import configs

all_series_names = ['N1', 'N2', 'N3', 'D1', 'D2', 'D3', 'S1', 'S2', 'S3', 'M1', 'M2', 'M3']
all_markers = ['o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'P', 'X']
all_colors = ['#FFBE33','#6FAD51','#A24618', '#A5A5A5','#145F8F','#3774C0', '#D25930','#F37A3B','#FFBE33', '#426831','#FFBE33','#3774C0']
all_video_files = [
    ('news1', 'person'), ('news2', 'person'), ('news3', 'person'),
    ('d1', 'car'), ('d2', 'car'), ('d3', 'car'),
    ('traffic1', 'car'), ('traffic2', 'car'), ('traffic3', 'car'),
    ('ff', 'person'), ('midway', 'person'), ('inception', 'person'),
]
# a: partition group
approach_a_template = './results/{video_file}/oursa-{type}-{w}-{p}-{num}-{k}-{pg}.txt'
# b: partition buffer
# 3: partition buffer with estimation based on one partition
# b2: partition buffer with estimation based on # of ub partitions
approach_b_template = './results/{video_file}/ours3-{type}-{w}-{p}-{num}-{k}-{pg}.txt'

fig_output_folder = './figures-diff/'

default_configs = dict(
  linewidth=4,
  markersize=10,
)

def read_float_with_prefix(file_path, prefix):
  with open(file_path) as f:
    for line in f.readlines():
      if line.startswith(prefix):
        return float(line[len(prefix):].strip())


def read_data_tuple(video_file, read_type, key, line_prefix, params, template):
  # read 
  read_file = template.format(
    video_file = video_file,
    type = read_type,
    **params,
  )+'.report'
  x_value = params[key]
  y_value = read_float_with_prefix(read_file, line_prefix)
  return (x_value, y_value)


def _save_current_plot(output_path):
  # plt.show()
  if not os.path.isdir(fig_output_folder):
    os.makedirs(fig_output_folder)
  # plt.savefig(fig_output_folder+output_path+'.pdf')
  plt.savefig(fig_output_folder+output_path+'.jpg')
  plt.clf()
  plt.cla()

def plot_query_time_diff(series_names, vary_key, output_path):

  video_files = [all_video_files[all_series_names.index(s)] for s in series_names]
  line_prefix = 'query time: '
  table_data = defaultdict(list)

  def add_data_row(video_file, read_type, params, name, template):
    x_value, y_value = read_data_tuple(video_file, read_type, vary_key, \
      line_prefix, params, template)
    table_data['name'].append(name)
    table_data[vary_key].append(x_value)
    table_data['time'].append(y_value)
  
  for (video_file, read_type), name in zip(video_files, series_names):
    params = dict(configs.default_settings)
    add_data_row(video_file, read_type, params, name+'_A', approach_a_template)
    add_data_row(video_file, read_type, params, name+'_B', approach_b_template)

    for vary_value in configs.vary_params[vary_key]:
      params[vary_key] = vary_value
      add_data_row(video_file, read_type, params, name+'_A', approach_a_template)
      add_data_row(video_file, read_type, params, name+'_B', approach_b_template)
  ax = plt.gca()
  sns.lineplot(x=vary_key, y="time", hue="name", style='name', dashes=False,\
     data=table_data, ax=ax, **default_configs)
  # remove 'name' title.
  ax.legend(title='')
  ax.set_ylabel('time(s)')
  ax.yaxis.set_ticks_position('left')
  ax.xaxis.set_ticks_position('bottom')
  _save_current_plot(output_path)

if __name__ == '__main__':
  for vary_key in configs.vary_params.keys():
    for series_name in all_series_names:
    # for series_name in ['N1', 'N2', 'N3']:
      plot_query_time_diff([series_name], vary_key, \
        "/query-{}-{}".format(vary_key, series_name))
    # break