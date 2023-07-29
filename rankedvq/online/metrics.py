class OnlineMetrics:
  def __init__(self) -> None:
      self.data = dict()

  def inc(self, key, value=1):
    self.data[key] = self.data.get(key, 0) + value

  def reset(self):
    self.data = dict()
  