from rankedvq.app.pre_defined_queries import get_predefined_queries
from rankedvq.bitarray import bitarray

def eval_query(mapped_bitset, query, mapped_label_masks):
  '''
  return True if the bitset satisfies the query
  '''
  for sub_query in query:
    satisfied = False
    print(sub_query)
    for labels, op, value in sub_query:
      # eval
      bitset_result = bitarray(mapped_bitset)
      missing_label = False
      for label in labels:
        mapped_mask = mapped_label_masks.get(label)
        if mapped_mask is not None:
          bitset_result &= mapped_mask
        else:
          # label is missing
          missing_label = True
      if bitset_result.count() >= value and not missing_label:
        satisfied = True
        break
    if not satisfied:
      return False
    print('result True')
  return True


if __name__ == '__main__':
  bitset = bitarray('1111')

  queries = get_predefined_queries('car')

  car_bitset = bitarray('1100')
  truck_bitset = bitarray('0011')

  print(eval_query(bitset, queries[1], dict(
    car = car_bitset, truck = truck_bitset
  )))