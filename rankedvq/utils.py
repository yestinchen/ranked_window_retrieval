def to_interval(frames):
  # assume ordered.
  interval_list = []
  current_interval = None
  last = -2
  for fid in frames:
    if last + 1 < fid:
      if current_interval is not None:
        current_interval[1] = last
      # new one
      current_interval = [fid, -1]
      interval_list.append(current_interval)
    last = fid
  current_interval[1] = last
  return interval_list



if __name__ == '__main__':
  print(to_interval([1, 2, 3, 4, 5, 7, 9]))
