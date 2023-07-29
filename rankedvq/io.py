import os

def write_multi_label_file(frames, file_path):
  parent_path = os.path.dirname(file_path)
  if not os.path.isdir(parent_path):
    os.makedirs(parent_path)

  with open(file_path, 'w') as f:
    for frame in frames:
      tuples = []
      for obj, label_set in frame.items():
        tuples.append('{}:{}'.format(obj, ','.join(label_set)))
      f.write('{}\n'.format(';'.join(tuples)))

def read_multi_label_file(file_path):
  result = []
  with open(file_path) as f:
    for line in f.readlines():
      objs_with_labels = line.split(';')
      obj_labels_dict = dict()
      for obj_wl in objs_with_labels:
        if len(obj_wl.strip()) == 0:
          # skip empty
          continue
        obj_wl_arr = [x.strip() for x in obj_wl.split(':')]
        obj_labels_dict[obj_wl_arr[0]] = frozenset([x.strip() for x in obj_wl_arr[1].split(',')])
      result.append(obj_labels_dict)
  return result

def split_frame_ids_and_type_dict(frames):
  frame_idsets = []
  type_dict = dict()
  for frame in frames:
    frame_idsets.append(frozenset(frame.keys()))
    for obj, label_set in frame.items():
      if obj not in type_dict:
        type_dict[obj] = label_set
      else:
        assert len(type_dict[obj].intersection(label_set)) == len(label_set), \
          "label set not match! {}, {}, {}".format(obj, type_dict[obj], label_set)
  return frame_idsets, type_dict

def read_type_grouped_file(file_path):
  # return [type: [ids, ],]
  result = []
  with open(file_path) as f:
    for line in f.readlines():
      line = line.strip()
      typeitems = line.split(';')
      tmp_dict = dict()
      for typeitem in typeitems:
        if len(typeitem) == 0:
          continue
        pair = typeitem.split(':')
        # print(pair)
        tmp_dict[pair[0].strip()] = [v.strip() for v in pair[1][1:-1].split(',')]
      result.append(tmp_dict)
  return result

def grouped_frames_to_multi_labels(frames):
  results = []
  for frame in frames:
    tmp_dict = dict()
    for obj_type, ids in frame.items():
      for id in ids:
        tmp_dict[id] = frozenset([obj_type])
    results.append(tmp_dict)
  return results

def filter_frames_with_types(frames, obj_types):
  results = []
  for frame in frames:
    _dict = dict()
    for type in obj_types:
      if type in frame:
        _dict[type] = frame[type]
    results.append(_dict)
  return results

def obtain_frames_as_obj_sets(frames, obj_type):
  results = []
  for frame in frames:
    results.append(frozenset(frame[obj_type] if obj_type in frame else []))
  return results

def validate_for_multi_labels(frames):
  # the same id appear in all frmaes should share the same set of labels
  seen_objs = dict()
  for frame in frames:
    for id, labels in frame.items():
      assert type(labels) == frozenset
      if id in seen_objs:
        assert seen_objs[id] == labels, \
          'labels for id [{}] does not match: {} vs {}'.format(id, seen_objs[id], labels)
      else:
        seen_objs[id] = labels

if __name__ == '__main__':
  # frames = read_type_grouped_file('data/MOT16-06.txt')
  # for frame in frames:
  #   print(frame)
  # print(frames[3])
  frames = read_multi_label_file('data/test-multi-label.txt')
  for frame in frames:
    print(frame)
  validate_for_multi_labels(frames)