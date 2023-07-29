from collections import defaultdict
from abc import ABC, abstractmethod
from dataclasses import dataclass
from rankedvq.utils import to_interval

@dataclass
class IntervalPayload:
  count: int
  labels: frozenset
  intervals: list

@dataclass
class Partition:
  start_fid: int
  size: int
  roots: list
  objs: list
  top1_dict: dict
  payload: object


class IncIdGenerator:
  def __init__(self):
    self.id = 0
  
  def next_id(self):
    self.id += 1
    return self.id

class Node:
  id_gen = IncIdGenerator()
  def __init__(self, obj, next_nodes=None, payload=None, id_=None):
    self.obj = obj
    self.next_nodes = next_nodes if next_nodes is not None else []
    self.payload = payload
    self.id = id_ if id_ is not None else Node.id_gen.next_id()

  def add_next(self, next):
    self.next_nodes.append(next)
  
  def __repr__(self) -> str:
    return '#{}, {}, next: [{}], payload: {};'.format(self.id, self.obj, 
      ','.join(['#{}'.format(node.id) for node in self.next_nodes]), self.payload)

  def recursive_str(self, depth=0) -> str:
      strs = [self.__repr__()]
      depth += 2
      for next in self.next_nodes:
        strs.append('{}{}'.format(' '*depth, next.recursive_str(depth)))
      return '\n'.join(strs)

class NodeCreator(ABC):
  @abstractmethod
  def create(self, obj_id, count, id_frames, remaining_objs, frames, start_fid, 
      prev_node, index_in_current_level, labels):
    pass  

class PartitionCreator(ABC): 
  @abstractmethod
  def create(self, roots, connected_obj_list, frames, type_dict, top1_dict, start_fid, partition_size):
    pass

class SimpleNodeWIntervalCreator(NodeCreator):
  
  def create(self, obj_id, count, id_frames, remaining_objs, frames, start_fid, prev_node, index_in_current_level, labels):
    node = Node(obj_id)
    node.payload = IntervalPayload(count, labels, to_interval(id_frames))
    return node

class SimplePartitionCreator(PartitionCreator):

  def create(self, roots, connected_obj_list, frames, type_dict, top1_dict, start_fid, partition_size):
    return Partition(start_fid, partition_size, roots, connected_obj_list, top1_dict, None)

class PartitionIndexBuilder:

  def __init__(self, start_fid: int, node_creator: NodeCreator, partition_creator: PartitionCreator):
    self.start_fid = start_fid
    self.node_creator = node_creator
    self.partition_creator = partition_creator

  def build(self, frames, type_dict):
    count_dict = dict()
    frames_dict = defaultdict(list)
    fid = self.start_fid
    for frame in frames:
      for id in frame:
        count = count_dict[id] if id in count_dict else 0
        count_dict[id] = count + 1
        frames_dict[id].append(fid)
      fid += 1

    partition_size = fid - self.start_fid

    top1_dict = defaultdict(int)
    roots = list()
    connected_obj_list = list()
    all_count_dicts = [count_dict]

    index_in_current_level = 0

    while len(count_dict) > 0:
      index_in_current_level += 1
      # select the one with the highest count.
      max_count_id = max(count_dict, key=lambda x : count_dict[x])
      max_count_value = count_dict.pop(max_count_id)
      # frames
      id_frames = frames_dict.pop(max_count_id)
      # TODO: create the new node
      node = self.node_creator.create(max_count_id, max_count_value, 
        id_frames, set(count_dict.keys()), frames, self.start_fid, None, 
        index_in_current_level, type_dict[max_count_id])
      connected_objs = set()

      roots.append(node)
      connected_objs.add(max_count_id)
      connected_obj_list.append(connected_objs)

      value = top1_dict[1]
      if value < max_count_value:
        top1_dict[1] = max_count_value

      # filtered_frames
      filtered_fid_frame_dict = dict()
      for i in id_frames:
        filtered_fid_frame_dict[i] = frames[i - self.start_fid]

      self.build_recursively(all_count_dicts, count_dict, filtered_fid_frame_dict, type_dict,
        set([max_count_id]), node, top1_dict, connected_objs)
    return self.partition_creator.create(roots, connected_obj_list, frames, type_dict,
      top1_dict, self.start_fid, partition_size)

  def build_recursively(self, all_count_dicts, count_dict, filtered_fid_frame_dict, type_dict,
        prefix_set, parent_node, top1_dict, connected_objs):
    filtered_count = dict()
    filtered_frame_dict = defaultdict(list)

    for fid, frame in filtered_fid_frame_dict.items():
      for id in frame:
        if id not in prefix_set and id in count_dict:
          count = filtered_count[id] if id in filtered_count else 0
          filtered_count[id] = count + 1
          filtered_frame_dict[id].append(fid)
    # fid = self.start_fid
    # for frame in frames:
    #   if prefix_set.issubset(frame):
    #     # include this frame.
    #     for id in frame:
    #       if id not in prefix_set and id in count_dict:
    #         count = filtered_count[id] if id in filtered_count else 0
    #         filtered_count[id] = count + 1
    #         filtered_frame_dict[id].append(fid)
    #   fid +=1
    
    all_count_dicts.append(filtered_count)

    index_in_current_level = 0
    while len(filtered_count) > 0:
      index_in_current_level += 1
      # get max id, value, frames
      max_count_id = max(filtered_count, key=lambda x : filtered_count[x])
      max_count_value = filtered_count.pop(max_count_id)
      id_frames = filtered_frame_dict.pop(max_count_id)

      # filter again.
      new_filtered_fid_frame_dict = dict()
      for fid, frame in filtered_fid_frame_dict.items():
        if max_count_id in frame:
          new_filtered_fid_frame_dict[fid] = frame

      node = self.node_creator.create(max_count_id, max_count_value, 
        id_frames, set(filtered_count.keys()), new_filtered_fid_frame_dict, self.start_fid, 
        parent_node, index_in_current_level, type_dict[max_count_id])
      connected_objs.add(max_count_id)
      if node is not None:
        parent_node.add_next(node)
      else:
        node = parent_node
      
      # remove ids from all count dicts
      for _count_dict in all_count_dicts:
        if _count_dict.get(max_count_id) == max_count_value:
          del _count_dict[max_count_id]
      
      # new prefix.
      new_prefix_set = set(prefix_set)
      new_prefix_set.add(max_count_id)

      level = len(new_prefix_set)
      top1_dict[level] = max(top1_dict[level], max_count_value)

      self.build_recursively(all_count_dicts, filtered_count, new_filtered_fid_frame_dict,\
        type_dict, new_prefix_set, node, top1_dict, connected_objs)

    all_count_dicts.remove(filtered_count)


if __name__ == '__main__':
  frames = [
    ["a", "c", "d", "e", "f", "g"],
    ["a", "b", "d"],
    ["a", "c"]
  ]
  type_dict = dict((x, 'person') for x in set([obj for frame in frames for obj in frame]))

  builder = PartitionIndexBuilder(0, SimpleNodeWIntervalCreator(), SimplePartitionCreator())
  partition = builder.build(frames, type_dict)
  for root in partition.roots:
    print(root.recursive_str())