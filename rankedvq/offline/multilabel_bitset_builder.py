import math
from dataclasses import dataclass
from rankedvq.utils import to_interval
from rankedvq.bitarray import bitarray
from collections import defaultdict
from rankedvq.offline.partition_builder import Node, Partition, IntervalPayload, \
  NodeCreator, PartitionCreator, PartitionIndexBuilder
from rankedvq.offline.bitset_builder import BitsetNodeCreator

@dataclass
class MultiLabelBitsetPartitionPayload:
  label_masks: dict
  obj_index: dict

class MultiLabelBitsetPartitionCreator(PartitionCreator):
  def __init__(self, ordered_objs, keep_graph = False) -> None:
    self.ordered_objs = ordered_objs
    self.keep_graph = keep_graph

  def create(self, roots, connected_obj_list, frames, type_dict, 
      top1_dict, start_fid, partition_size):
    # sort all nodes.
    to_visit = roots[:]
    
    # obj_dict: obj -> (obj_mask, ordered_node_list)
    obj_index = dict()
    # label -> label_mask
    label_masks = dict()

    # 
    obj_pos_dict = dict()
    for i, obj in enumerate(self.ordered_objs):
      obj_pos_dict[obj] = i

    while len(to_visit) > 0:
      node = to_visit.pop(0)
      assert type(node.payload.labels) == frozenset, 'expecting frozenset for node labels'
      for label in node.payload.labels:
        # get label_mask
        label_mask = label_masks.get(label)
        if label_mask is None:
          # init bitarray
          _bitarray = bitarray(len(self.ordered_objs))
          _bitarray.setall(0)
          label_mask = [_bitarray, set()]
          label_masks[label] = label_mask
        # update label_mask
        label_mask[0][obj_pos_dict[node.obj]] = True
        label_mask[1].add(node.obj)

        node_list_for_obj = obj_index.get(node.obj)
        if node_list_for_obj is None:
          obj_mask = bitarray(len(self.ordered_objs))
          obj_mask.setall(0)
          node_list_for_obj = [obj_mask, list()]
          obj_index[node.obj] = node_list_for_obj
        node_list_for_obj[1].append(node)
        # node_list_for_obj[0] |= node.payload.bitset
      # continue
      if len(node.next_nodes) > 0:
        to_visit.extend(node.next_nodes)
    for obj, [mask , node_list] in obj_index.items():
      node_list.sort(key= lambda x: x.payload.count)
    
    partition = Partition(start_fid, partition_size, 
      roots if self.keep_graph else [], self.ordered_objs, top1_dict, 
      MultiLabelBitsetPartitionPayload(label_masks, obj_index))
    return partition
  

class MultiLabelBitsetIndexBuilder:
  
  def build(self, frames, type_dict, partition_size, keep_graph=False):
    '''
    frames: {type: [ids]}
    '''
    partitions = []
    partition_num = math.ceil(len(frames) / partition_size)
    for partition_id in range(partition_num):
      bound = min(len(frames), partition_size*(partition_id+1))
      # get all objects.
      all_objs = set()
      sub_frames = frames[partition_id * partition_size:bound]
      for frame in sub_frames:
        # TODO: should we consider ids in this?
        for id in frame:
          all_objs.add(id)
      ordered_objs = sorted(all_objs)

      builder = PartitionIndexBuilder(partition_id*partition_size, 
        BitsetNodeCreator(ordered_objs), MultiLabelBitsetPartitionCreator(ordered_objs, keep_graph))
      partitions.append(builder.build(sub_frames, type_dict))
    return partitions


if __name__ == '__main__':
  frames = [
    ['a', 'c', 'd', 'e', 'f', 'g'],
    ['a', 'b', 'd'],
    ['a', 'c']
  ]
  type_dict = dict((x, 'person') for x in set([obj for frame in frames for obj in frame]))

  builder = MultiLabelBitsetIndexBuilder()
  partitions = builder.build(frames, type_dict, 3, True)
  print('partition num: ', len(partitions))
  for p in partitions:
    payload = p.payload
    print("# of nodes", len(payload.all_sorted_nodes), "sorted objs:", p.objs)
    for root in p.roots:
      print(root.recursive_str())
