import math
from dataclasses import dataclass
from rankedvq.utils import to_interval
from rankedvq.bitarray import bitarray
from collections import defaultdict
from rankedvq.offline.partition_builder import Node, Partition, IntervalPayload, \
  NodeCreator, PartitionCreator, PartitionIndexBuilder

@dataclass
class BitsetIntervalPayload(IntervalPayload):
  bitset: bitarray

@dataclass
class BitsetPartitionPayload():
  all_sorted_nodes: list
  obj_node_dict: dict

class BitsetNodeCreator(NodeCreator):
  def __init__(self, ordered_objs):
    self.ordered_objs = ordered_objs

  def create(self, obj_id, count, id_frames, remaining_objs, frames, 
      start_fid, prev_node, index_in_current_level, label):
    node = Node(obj_id)

    # construct the bitset
    bitset = bitarray(len(self.ordered_objs))
    bitset.setall(0)
    if prev_node is not None:
      bitset |= prev_node.payload.bitset
    # add the current one
    pos = self.ordered_objs.index(obj_id)
    bitset[pos] = True

    node.payload = BitsetIntervalPayload(count, label, to_interval(id_frames), bitset)
    # if node.id == 11944:
    #   print('creating node', node)
    return node

class BitsetPartitionCreator(PartitionCreator):
  def __init__(self, ordered_objs, keep_graph = False) -> None:
    self.ordered_objs = ordered_objs
    self.keep_graph = keep_graph
  
  def create(self, roots, connected_obj_list, frames, type_dict, 
      top1_dict, start_fid, partition_size):
    # sort all nodes.
    all_nodes = []
    obj_node_dict = defaultdict(list)
    to_visit = roots[:]
    while len(to_visit) > 0:
      node = to_visit.pop(0)
      obj_node_dict[node.obj].append(node)
      all_nodes.append(node)
      if len(node.next_nodes) > 0:
        to_visit.extend(node.next_nodes)
    # sort all nodes
    for obj in obj_node_dict:
      obj_node_dict[obj] = sorted(obj_node_dict[obj], 
        key=lambda x: x.payload.count, reverse=False) # FIXME?
    sorted_all_nodes = sorted(all_nodes, key=lambda x: x.payload.count, reverse=True)
    
    partition = Partition(start_fid, partition_size, 
      roots if self.keep_graph else [], self.ordered_objs, top1_dict, 
      BitsetPartitionPayload(sorted_all_nodes, obj_node_dict))
    return partition
  

class BitsetIndexBuilder:
  
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
        BitsetNodeCreator(ordered_objs), BitsetPartitionCreator(ordered_objs, keep_graph))
      partitions.append(builder.build(sub_frames, type_dict))
    return partitions


if __name__ == '__main__':
  frames = [
    ['a', 'c', 'd', 'e', 'f', 'g'],
    ['a', 'b', 'd'],
    ['a', 'c']
  ]
  type_dict = dict((x, 'person') for x in set([obj for frame in frames for obj in frame]))

  builder = BitsetIndexBuilder()
  partitions = builder.build(frames, type_dict, 3, True)
  print('partition num: ', len(partitions))
  for p in partitions:
    payload = p.payload
    print("# of nodes", len(payload.all_sorted_nodes), "sorted objs:", p.objs)
    for root in p.roots:
      print(root.recursive_str())
