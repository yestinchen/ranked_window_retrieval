class TopkBookKeeperBreakTie:
  def __init__(self, k) -> None:
    self.k = k
    self.cache_list = []
    self.min = -1
    self.window_dict = dict()

  def __add_to_cache(self, window_start, score, payload):
    # if score == 203:
    #   print('updating score', window_start, score)
    new_tuple = (window_start, score, payload)
    self.window_dict[window_start] = new_tuple
    self.cache_list.append(new_tuple)
    self.cache_list.sort(key=lambda x: x[1], reverse=True)
    if len(self.cache_list) > self.k:
      # remove the last one.
      removed_tuple = self.cache_list.pop(-1)
      # remove from window_dict
      self.window_dict.pop(removed_tuple[0])
      
    if len(self.cache_list) == self.k:
      # update min
      self.min = self.cache_list[-1][1]

  def update(self, window_start, score, payload=None):
    if score < self.min:
      return
    # see if there is already a window with the starting frame.
    if window_start in self.window_dict:
      existing_one = self.window_dict[window_start]
      if existing_one[1] < score:
        # remove existing one.
        self.cache_list.remove(existing_one)
        # update
        self.__add_to_cache(window_start, score, payload)
    else:
      # new one.
      self.__add_to_cache(window_start, score, payload)

if __name__ == '__main__':
  bookkeeper = TopkBookKeeperBreakTie(2)
  bookkeeper.update(2, 2)
  print(bookkeeper.cache_list)
  bookkeeper.update(2, 3)
  print(bookkeeper.cache_list)
  bookkeeper.update(1, 3)
  print(bookkeeper.cache_list)
  bookkeeper.update(3, 4)
  print(bookkeeper.cache_list)