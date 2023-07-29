def update_window_score_arr_1(self, set_and_interval):
  sorted_intervals = sorted(set_and_interval[1],key=lambda x: x[0])
  for j in range(len(self.window_score_arr)):
    _start = j + self.base_idx
    _end = _start + self.w - 1
    _score = 0
    for interval in sorted_intervals:
      if interval[1] < _start:
        continue
      if interval[0] > _end:
        break
      _max_start = max(_start, interval[0])
      _min_end = min(_end, interval[1])
      if _min_end >= _max_start:
        _score += _min_end - _max_start + 1
    # if _score == 60:
    #   print('score', _score)
    #   print(set_and_interval)
    if _score > self.window_score_arr[j]:
      self.window_score_arr[j] = _score
      self.score_obj_sets[j] = set_and_interval

def update_window_score_arr_2(self, set_and_interval):
  sorted_intervals = sorted(set_and_interval[1],key=lambda x: x[0])
  if len(sorted_intervals) > 0:
    # expand interval to arr.
    interval_start = sorted_intervals[0][0]
    interval_end = sorted_intervals[-1][1]
    expanded_interval = [0] * (interval_end - interval_start + 1)
    for interval in sorted_intervals:
      for i in range(interval[0], interval[1]+1):
        expanded_interval[i - interval_start] = 1
    # sliding widow count.
    window_start = max(self.base_idx, interval_start-self.w+1) # include
    window_end = window_start + self.w - 1
    window_score = 0
    # 1. fill initial window.
    for j in range(window_start, window_end):
      if j < interval_start:
        continue
      if j <= interval_end:
        window_score += expanded_interval[j-interval_start]
    # 2. sliding window.
    last_window_end = min(interval_end + self.w -1, self.base_idx + len(self.window_score_arr) -1 + self.w - 1)
    for j in range(window_end, last_window_end+1):
      # update window score
      window_arr_pos = j-self.base_idx-self.w + 1
      # try to add new score
      if j <= interval_end:
        window_score += expanded_interval[j - interval_start]
      if window_score > self.window_score_arr[window_arr_pos]:
        self.window_score_arr[window_arr_pos] = window_score
        self.score_obj_sets[window_arr_pos] = set_and_interval
      # try to remove earliest score.
      expired_frame = window_start - interval_start
      if expired_frame >= 0:
        window_score -= expanded_interval[expired_frame]
      window_start += 1

def update_window_score_arr_3(self, set_and_interval, bookkeeper):
  pass