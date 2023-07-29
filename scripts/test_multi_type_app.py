from rankedvq.app.multilabel_type_app import run_multilabel_bitset_index as run_single
from rankedvq.app.multilabel_type_multi_app import run_multilabel_bitset_index as run_multi
import cProfile

if __name__ == '__main__':
  run_single(
      'data/inception.txt',
      'person',
      1200, 
      6,
      600,
      10,
      None,
      'out/test_m3_out-1200-10.txt'
    )
  # cProfile.run(
  #   """run_single(
  #     'data/d3.txt',
  #     'car',
  #     600, 
  #     6,
  #     1500,
  #     10,
  #     None,
  #     'out/test_d3_out-1500-10.txt'
  #   )
  #   """
  # )

  # run_multi(
  #   'data/with_colors/news2.txt',
  #   'person',
  #   600,
  #   0,
  #   600,
  #   100,
  #   None,
  #   'out/test_multi_news2.txt'
  # )