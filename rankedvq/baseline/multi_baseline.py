
def eval_query(obj_set, type_dict, query):
  for sub_query in query:
    satisfied = False
    for labels, op, value in sub_query:
      # count.
      satisfied_num = 0
      for obj in obj_set:
        if len(type_dict[obj].intersection(labels)) == len(labels):
          satisfied_num += 1
      if satisfied_num >= value:
        satisfied = True
        break
    if not satisfied:
      return False
  return True

def compute_all_window_scores(frames, query, w):
  assert type(frames[0]) == dict, \
    'expecting the type of each frame to be a dict'

  objs_score_map = dict()
  window_scores = dict()

  for i in range(len(frames)):
    max_score = 0
    max_score_objs = None
    frame = frames[i]
    frame_obj_set = frozenset(frame.keys())

    # 1. clear expired frames
    if i > w:
      removed_keys = set()
      for key, obj_set_frames in objs_score_map.items():
        obj_set_frames.discard(i-w)
        if len(obj_set_frames) == 0:
          removed_keys.add(key)
        elif len(obj_set_frames) > max_score:
          max_score = len(obj_set_frames)
          max_score_objs = key
      # clear
      for key in removed_keys:
        objs_score_map.pop(key)

    # 2. generate new object sets
    for key in list(objs_score_map.keys()):
      obj_set_frames = objs_score_map[key]
      inter_set = frozenset(frame_obj_set.intersection(key))
      # test 
      if eval_query(inter_set, frame, query):
        if inter_set not in objs_score_map:
          objs_score_map[inter_set] = set()
        new_frame_set = objs_score_map[inter_set]
        new_frame_set.update(obj_set_frames)
        new_frame_set.add(i)
        if len(new_frame_set) > max_score:
          max_score = len(new_frame_set)
          max_score_objs = inter_set
    
    # add the current frame if needed.
    if len(frame_obj_set) > 0 and eval_query(frame_obj_set, frame, query):
      if frame_obj_set not in objs_score_map:
        objs_score_map[frame_obj_set] = set([i])
      else:
        objs_score_map[frame_obj_set].add(i)
      frames_for_this_frame = objs_score_map[frame_obj_set]
      if len(frames_for_this_frame) > max_score:
        max_score = len(frames_for_this_frame)
        max_score_objs = frame
    
    # window score
    if i - w + 1 >=0:
      window_scores[i-w+1] = max_score
  return window_scores