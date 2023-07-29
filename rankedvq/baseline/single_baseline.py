
def compute_all_window_scores(frames, obj_num, w):
  assert type(frames[0]) == frozenset

  objs_score_map = dict()
  window_scores = dict()
  for i in range(len(frames)):
    max_score = 0
    max_score_objs = None
    frame = frames[i]
    # clear expired frames
    if i > w:
      removed_keys = set()
      for key, obj_set_frames in objs_score_map.items():
        obj_set_frames.discard(i - w) 
        if len(obj_set_frames) == 0:
          removed_keys.add(key)
        elif len(obj_set_frames) > max_score:
          max_score = len(obj_set_frames)
          max_score_objs = key
      # clear
      for key in removed_keys:
        objs_score_map.pop(key)
    # add the new frame
    for key in list(objs_score_map.keys()):
      obj_set_frames = objs_score_map[key]
      inter_set = frozenset(frame.intersection(key))
      if len(inter_set) >= obj_num:
        if inter_set not in objs_score_map:
          objs_score_map[inter_set] = set()
        new_frame_set = objs_score_map[inter_set]
        new_frame_set.update(obj_set_frames)
        new_frame_set.add(i)
        if len(new_frame_set) > max_score:
          max_score = len(new_frame_set)
          max_score_objs = inter_set
    # add the current frame if needed.
    if len(frame) > 0 and len(frame) >= obj_num:
      if frame not in objs_score_map:
        objs_score_map[frame] = set([i])
      else:
        objs_score_map[frame].add(i)
      frames_for_this_frame = objs_score_map[frame]
      if len(frames_for_this_frame) > max_score:
        max_score = len(frames_for_this_frame)
        max_score_objs = frame
    # put window score
    if i - w + 1 >=0:
      window_scores[i - w + 1] = max_score
  return window_scores
