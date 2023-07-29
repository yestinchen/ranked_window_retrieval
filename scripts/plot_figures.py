from collections import defaultdict
import os
from scripts import configs
from rankedvq import io
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure, xlabel
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D

all_avaliable_markers = [',', '.', 'o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'H', 'D', 'd', 'P', 'X']
all_series_names = ['N1', 'N2', 'N3', 'D1', 'D2', 'D3', 'S1', 'S2', 'S3', 'M1', 'M2', 'M3']
all_markers = ['o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'P', 'X']
all_colors = ['#FFBE33','#6FAD51','#A24618', '#A5A5A5','#145F8F','#3774C0', '#D25930','#F37A3B','#FFBE33', '#426831','#FFBE33','#3774C0']
all_video_files = [
    ('news1', 'person'), ('news2', 'person'), ('news3', 'person'),
    ('d1', 'car'), ('d2', 'car'), ('d3', 'car'),
    ('traffic1', 'car'), ('traffic2', 'car'), ('traffic3', 'car'),
    ('ff', 'person'), ('midway', 'person'), ('inception', 'person'),
]

fig_output_folder = './figuresa/'

default_configs = dict(
  linewidth=4,
  markersize=10,
)

def read_float_with_prefix(file_path, prefix):
  with open(file_path) as f:
    for line in f.readlines():
      if line.startswith(prefix):
        return float(line[len(prefix):].strip())

def read_data_tuple(video_file, read_type, key, line_prefix, params, template=None):
  if template is None:
    template = configs.proposed_output_template
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

def _save_data(output_path, data):
  with open(fig_output_folder+output_path+'.csv', 'w') as f:
    for key, value in data.items():
      f.write('{},{}\n'.format(key, ','.join(list(map(str, value)))))

def plot_building_time(series_names, vary_key, x_ticks, y_ticks, output_path, ylog=False):
  markers = [all_markers[all_series_names.index(s)] for s in series_names]
  palette = [all_colors[all_series_names.index(s)] for s in series_names]
  video_files = [all_video_files[all_series_names.index(s)] for s in series_names]
  line_prefix = 'build time: '
  table_data = defaultdict(list)
  def add_data_row(video_file, read_type, params, name):
    # read 
    x_value, y_value = read_data_tuple(video_file, read_type, vary_key, line_prefix, params)
    table_data['name'].append(name)
    table_data[vary_key].append(x_value)
    table_data['time'].append(y_value)
  for (video_file, read_type), name in zip(video_files, series_names):
    params = dict(configs.default_settings_1)
    add_data_row(video_file, read_type, params, name)
    for vary_value in configs.vary_params_1[vary_key]:
      params[vary_key] = vary_value
      add_data_row(video_file, read_type, params, name)
  # print(table_data)
  ax = plt.gca()
  sns.lineplot(x=vary_key, y="time", hue="name", style='name', dashes=False, palette=palette,
    markers=markers, data=table_data, ax=ax, **default_configs)
  # remove 'name' title.
  ax.legend(title='')
  ax.set_ylabel('time(s)')
  ax.set_xticks(x_ticks)
  ax.set_yticks(y_ticks)
  if ylog:
    ax.set_yscale('log')
  # Only show ticks on the left and bottom spines
  ax.yaxis.set_ticks_position('left')
  ax.xaxis.set_ticks_position('bottom')
  _save_current_plot(output_path)
  _save_data(output_path, table_data)

def plot_query_time(series_names, vary_key, output_path,\
   default_settings, vary_params, x_label=None, x_ticks=None,\
   y_ticks=None, y_log=False, x_log=False, skip_default_setting=False):
  markers = [all_markers[all_series_names.index(s)] for s in series_names]
  palette = [all_colors[all_series_names.index(s)] for s in series_names]
  video_files = [all_video_files[all_series_names.index(s)] for s in series_names]
  line_prefix = 'query time: '
  table_data = defaultdict(list)
  def add_data_row(video_file, read_type, params, name):
    # read 
    x_value, y_value = read_data_tuple(video_file, read_type, vary_key, line_prefix, params)
    table_data['name'].append(name)
    table_data[vary_key].append(x_value)
    table_data['time'].append(y_value)
  for (video_file, read_type), name in zip(video_files, series_names):
    params = dict(default_settings)
    if not skip_default_setting:
      add_data_row(video_file, read_type, params, name)
    for vary_value in vary_params[vary_key]:
      params[vary_key] = vary_value
      add_data_row(video_file, read_type, params, name)
  # print(table_data)
  ax = plt.gca()
  sns.lineplot(x=vary_key, y="time", hue="name", style='name', dashes=False, palette=palette,
    markers=markers, data=table_data, ax=ax, **default_configs)
  # remove 'name' title.
  ax.legend(title='')
  if x_label is not None:
    ax.set_xlabel(x_label)
  ax.set_ylabel('time(s)')
  if x_ticks is not None:
    ax.set_xticks(x_ticks)
  if y_ticks is not None:
    ax.set_yticks(y_ticks)
  if y_log:
    ax.set_yscale('log')
  if x_log:
    ax.set_xscale('log')
  # Only show ticks on the left and bottom spines
  ax.yaxis.set_ticks_position('left')
  ax.xaxis.set_ticks_position('bottom')
  _save_current_plot(output_path)
  _save_data(output_path, table_data)

def plot_building_times():
  plt.subplots_adjust(left=0.18, right=0.95, top=0.95, bottom=0.18)
  x_ticks = [300, 600, 900, 1200, 1500]
  plot_building_time(['N1', 'N2', 'N3'], 'p', x_ticks, [1, 2, 3], 'building_time_N')
  plot_building_time(['S1', 'S2', 'S3'], 'p', x_ticks, [1,10,100], 'building_time_S', ylog=True)
  plot_building_time(['D1', 'D2', 'D3'], 'p', x_ticks, [10, 20, 30], 'building_time_D')
  plot_building_time(['M1', 'M2', 'M3'], 'p', x_ticks, [2, 4, 6, 8], 'building_time_M')

def plot_varying_w():
  x_ticks = [300, 600, 900, 1200, 1500]
  _s = configs.default_settings_1
  _v = configs.vary_params_1
  plot_query_time(['N1', 'N2', 'N3'], 'w', 'varying_w_N', _s, _v, x_ticks=x_ticks, y_ticks=[0.05, 0.1])
  plot_query_time(['S1', 'S2', 'S3'], 'w', 'varying_w_S', _s, _v, x_ticks=x_ticks, y_ticks=[1, 2])
  plot_query_time(['D1', 'D2', 'D3'], 'w', 'varying_w_D', _s, _v, x_ticks=x_ticks, y_ticks=[2, 4, 6])
  plot_query_time(['M1', 'M2', 'M3'], 'w', 'varying_w_M', _s, _v, x_ticks=x_ticks, y_ticks=[0.2, 0.4])

def plot_varying_p():
  x_ticks = [300, 600, 900, 1200, 1500]
  _s = configs.default_settings_1
  _v = configs.vary_params_1
  plot_query_time(['N1', 'N2', 'N3'], 'p', 'varying_p_N', _s, _v, x_ticks=x_ticks, y_ticks=[0.05, 0.1])
  plot_query_time(['S1', 'S2', 'S3'], 'p', 'varying_p_S', _s, _v, x_ticks=x_ticks, y_ticks=[0.5, 1, 1.5])
  plot_query_time(['D1', 'D2', 'D3'], 'p', 'varying_p_D', _s, _v, x_ticks=x_ticks, y_ticks=[0.5, 1, 1.5])
  plot_query_time(['M1', 'M2', 'M3'], 'p', 'varying_p_M', _s, _v, x_ticks=x_ticks, y_ticks=[0.1, 0.2, 0.3])

def plot_varying_num():
  x_ticks = [2, 4, 6, 8, 10]
  _s = configs.default_settings_1
  _v = configs.vary_params_1
  plot_query_time(['N1', 'N2', 'N3'], 'num', 'varying_num_N', _s, _v, x_ticks=x_ticks, y_ticks=[0.03, 0.06, 0.09])
  plot_query_time(['S1', 'S2', 'S3'], 'num', 'varying_num_S', _s, _v, x_ticks=x_ticks, y_ticks=[0.5, 1, 1.5])
  plot_query_time(['D1', 'D2', 'D3'], 'num', 'varying_num_D', _s, _v, x_ticks=x_ticks, y_ticks=[0.3, 0.6, 0.9])
  plot_query_time(['M1', 'M2', 'M3'], 'num', 'varying_num_M', _s, _v, x_ticks=x_ticks, y_ticks=[0.1, 0.2, 0.3])

def plot_varying_k():
  x_ticks = [1, 10, 100, 1000, 2000]
  _s = configs.default_settings_1
  _v = configs.vary_params_1
  plot_query_time(['N1', 'N2', 'N3'], 'k', 'varying_k_N', _s, _v, x_ticks=x_ticks, x_log=True, y_ticks=[0.3, 0.6, 0.9])
  plot_query_time(['S1', 'S2', 'S3'], 'k', 'varying_k_S', _s, _v, x_ticks=x_ticks, x_log=True, y_ticks=[1, 2, ])
  plot_query_time(['D1', 'D2', 'D3'], 'k', 'varying_k_D', _s, _v, x_ticks=x_ticks, x_log=True,  y_ticks=[1, 2])
  plot_query_time(['M1', 'M2', 'M3'], 'k', 'varying_k_M', _s, _v, x_ticks=x_ticks, x_log=True, y_ticks=[0.5, 1, 1.5])

def plot_varying_pg():
  # x_ticks = [4, 6, 8, 10]
  x_ticks = [4, 8, 12, 16]
  _s = configs.default_settings_2
  _v = configs.vary_params_2
  plot_query_time(['N1', 'N2', 'N3'], 'pg', 'varying_pg_N', _s, _v, x_ticks=x_ticks, y_ticks=[0.05, 0.1, 0.15], skip_default_setting=True)
  plot_query_time(['S1', 'S2', 'S3'], 'pg', 'varying_pg_S', _s, _v, x_ticks=x_ticks, y_ticks=[0.5, 1, 1.5, 2], skip_default_setting=True)
  plot_query_time(['D1', 'D2', 'D3'], 'pg', 'varying_pg_D', _s, _v, x_ticks=x_ticks, y_ticks=[2, 4, 6], skip_default_setting=True)
  plot_query_time(['M1', 'M2', 'M3'], 'pg', 'varying_pg_M', _s, _v, x_ticks=x_ticks, y_ticks=[0.3, 0.6, 0.9], skip_default_setting=True)


def plot_single_baseline(series_names, x_ticks, output_path):
  markers = [all_markers[all_series_names.index(s)] for s in series_names]
  palette = [all_colors[all_series_names.index(s)] for s in series_names]
  video_files = [all_video_files[all_series_names.index(s)] for s in series_names]
  vary_key = 'w'
  line_prefix= 'time: '

  table_data = defaultdict(list)
  def add_data_row(video_file, read_type, params, name):
    # read 
    x_value, y_value = read_data_tuple(video_file, read_type, vary_key, line_prefix, params, \
      template=configs.baseline_output_template)
    table_data['name'].append(name)
    table_data[vary_key].append(x_value)
    table_data['time'].append(y_value)
  for (video_file, read_type), name in zip(video_files, series_names):
    params = dict(configs.default_settings_1)
    add_data_row(video_file, read_type, params, name)
    for vary_value in configs.vary_params_1[vary_key]:
      params[vary_key] = vary_value
      add_data_row(video_file, read_type, params, name)
  # print(table_data)
  ax = plt.gca()
  sns.lineplot(x=vary_key, y="time", hue="name", style='name', dashes=False, palette=palette,
    markers=markers, data=table_data, ax=ax, **default_configs)
  # remove 'name' title.
  ax.legend(title='', bbox_to_anchor=(0.5, 1.18), ncol=3, loc='center')
  # Only show ticks on the left and bottom spines
  ax.yaxis.set_ticks_position('left')
  ax.xaxis.set_ticks_position('bottom')
  ax.set_yscale('log')
  ax.set_ylabel('time(s)')
  ax.set_xticks(x_ticks)
  _save_current_plot(output_path)
  _save_data(output_path, table_data)

def plot_single_baselines():
  x_ticks = [300, 600, 900, 1200, 1500]
  plt.subplots_adjust(left=0.18, right=0.95, top=0.8, bottom=0.18)
  plot_single_baseline(['S1', 'S2', 'S3', 'N1', 'N2', 'N3'], x_ticks, 'baselines_1')
  plot_single_baseline(['M1', 'M2', 'M3', 'D1', 'D2', 'D3'], x_ticks, 'baselines_2')


def plot_time_span(video_file, read_type, xticks, yticks, bins, output_path):
  # read all objects.
  frames = io.read_type_grouped_file(configs.dataset_grouped_template.format(video_file=video_file))
  frames = io.filter_frames_with_types(frames, [read_type])
  obj_start_end_dict = dict()
  for i, frame in enumerate(frames):
    for type, objs in frame.items():
      for obj in objs:
        if obj in obj_start_end_dict:
          obj_start_end_dict[obj][1] = i
        else:
          obj_start_end_dict[obj] = [i, i]
  # compute span.
  table_data = dict(obj=[], span=[])
  for obj, start_end in obj_start_end_dict.items():
    span = start_end[1] - start_end[0] + 1
    table_data['obj'].append(obj)
    table_data['span'].append(span)
  # plot distribution map.
  ax = plt.gca()
  sns.histplot(table_data, x='span', ax=ax, bins=bins)
  ax.set_ylabel('# of objects')
  ax.set_xlabel('time span')
  ax.yaxis.set_ticks_position('left')
  ax.xaxis.set_ticks_position('bottom')
  ax.set_xticks(xticks)
  ax.set_yticks(yticks)
  ax.set_xlim([xticks[0], xticks[-1]])
  ax.set_ylim([yticks[0], yticks[-1]])

  _save_current_plot(output_path)
  _save_data(output_path, table_data)

def plot_time_spans():
  figure(figsize=(5, 2), dpi=80)
  plt.subplots_adjust(left=0.18, right=0.95, top=0.95, bottom=0.28)
  plot_time_span('news1', 'person', [0, 200, 400, 600] , [0, 100, 200, 300, 400, 500], 120, 'time_span_N1')
  plot_time_span('d1', 'car', [0, 100, 200, 300] , [0, 400, 800, 1200, 1600, 2000], 50, 'time_span_D1')


def plot_multi_label(series_names, settings, output_path, speedup_ticks=None):
  video_files = [all_video_files[all_series_names.index(s)] for s in series_names]
  baseline_prefix = 'time: '
  ours_prefix = 'query time: '
  table_data = dict(video=[], baseline=[], ours = [], speedup=[])
  for (video_file, read_type), name in zip(video_files, series_names):
    # get baseline.
    baseline_report_path = configs.baseline_multi_output_template.format(
      video_file = video_file, 
      type=read_type,
      **settings
    )+'.report'
    baseline_time = read_float_with_prefix(baseline_report_path, baseline_prefix)
    table_data['video'].append(name)
    table_data['baseline'].append(baseline_time)

    # get ours.
    ours_report_path = configs.proposed_multi_output_template.format(
      video_file=video_file,
      type=read_type,
      **settings
    )+'.report'
    ours_time = read_float_with_prefix(ours_report_path, ours_prefix)
    table_data['ours'].append(ours_time)
    table_data['speedup'].append(baseline_time/ours_time)
  
  ax = plt.gca()
  sns.barplot(data=table_data, x='video', y='baseline', ax=ax, color='#c4c4c4')
  sns.barplot(data=table_data, x='video', y='ours', ax=ax, color='#4f7db8', hatch='/')
  ax.set_ylabel('time(s)')
  ax.set_yscale('log')
  ax2 = plt.twinx()
  ax.yaxis.set_ticks_position('left')
  ax.xaxis.set_ticks_position('bottom')
  sns.lineplot(data=table_data, color="#e3775f", ax=ax2, x='video',\
   y='speedup', marker='*', **default_configs)
  if speedup_ticks is not None:
    ax2.set_yticks(speedup_ticks)
  
  baseline = mpatches.Patch(color='#c4c4c4', label='baseline')
  ours = mpatches.Patch(facecolor='#4f7db8', hatch='/', label='our method')
  speedup = Line2D([0], [0], color='#e3775f', marker='*', label='speedup')
  plt.legend(handles=[baseline, ours, speedup], ncol=3, loc='upper center', bbox_to_anchor=(0.5, 1.22))
  _save_current_plot(output_path)
  _save_data(output_path, table_data)


def plot_multi_labels():
  sns.set_context(font_scale=2)
  plt.subplots_adjust(left=0.16, right=0.85, top=0.85, bottom=0.1)
  plot_multi_label(
    ['S1', 'S2', 'S3', 'D1', 'D2', 'D3', 'M1', 'M2', 'M3', 'N1', 'N2', 'N3'],
    configs.multi_settings_0, 'multi_labels_q0',
    speedup_ticks=[10, 20, 30]
  )
  plot_multi_label(
    ['S1', 'S2', 'S3', 'D1', 'D2', 'D3', 'M1', 'M2', 'M3', 'N1', 'N2', 'N3'],
    configs.multi_settings_1, 'multi_labels_q1',
    speedup_ticks=[50, 100, 150, 200, 250]
  )


if __name__ == '__main__':
  # create folder.
  import os
  if not os.path.isdir(fig_output_folder):
    os.makedirs(fig_output_folder)

  figure(figsize=(5, 3), dpi=80)
  sns.set_theme(font_scale=1.5, context='paper', style='white', palette='deep')

  plot_time_spans()
  figure(figsize=(5, 3), dpi=80)
  
  plot_single_baselines()
  plot_building_times()

  plt.subplots_adjust(left=0.18, right=0.95, top=0.95, bottom=0.18)
  plot_varying_w()
  plot_varying_p()
  plot_varying_num()
  plot_varying_k()
  plot_varying_pg()
  plot_multi_labels()
  