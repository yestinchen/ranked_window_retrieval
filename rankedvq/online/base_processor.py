from collections import defaultdict
import logging

class PartitionGroup:
  def __init__(self) -> None:
    self.partitions = []
    self.sorted_entries_by_count = []
    self.prefix_dicts = []
    self.score = 0
    self.start = -1
    self.end = -1
  
  def select_common_objs(self):
    # get objects that appear in more than 1 partitions.
    obj_partition_num_dict = defaultdict(int)
    for p in self.partitions:
      for obj in p.objs:
        obj_partition_num_dict[obj] += 1
    common_objs = [key for key, value in obj_partition_num_dict.items() if value > 1]
    # common_objs = []
    # for p in self.partitions:
    #   for obj in p.objs:
    #     value = obj_partition_num_dict[obj]
    #     if value == 0:
    #       value = 1
    #     elif value == 1:
    #       value = 2
    #       common_objs.append(obj)
    #     else:
    #       value = 3
    return sorted(common_objs)

def generate_partition_groups(partitions, 
    partition_num_lb, partition_size, prune_partition_func, pg_size = None):
  partition_groups = []

  # default to min_partitions_in_a_window + 1
  if pg_size is None:
    pg_size = partition_num_lb + 1
    logging.info("pg_size not specified, use %s as default", pg_size)
  else:
    logging.info("pg_size specified to %s", pg_size)
  # add partition window.

  i = 0
  pruned_partition_count = 0

  # get the first partition group.
  pg1 = PartitionGroup()
  pg1.start = 0
  while i < pg_size and i < len(partitions):
    # partitions
    partition = partitions[i]
    if prune_partition_func(partition):
      pruned_partition_count += 1
    else:
      pg1.partitions.append(partition)
    pg1.end = partition.start_fid + partition.size - 1
    i += 1
  
  if len(pg1.partitions) > 0:
    partition_groups.append(pg1)
  
  last_pg = pg1
  pg_new = PartitionGroup()
  pg_new.partitions = last_pg.partitions[:]
  checkpoint_i = pg_size - partition_num_lb
  pg_new.start = checkpoint_i * partition_size

  while i < len(partitions):
    partition = partitions[i]
    if prune_partition_func(partition):
      pruned_partition_count += 1
    else:
      pg_new.partitions.append(partition)
    pg_new.end = partition.start_fid + partition.size -1

    if (i+1) % checkpoint_i == 0:
      sub_partitions = pg_new.partitions
      while len(sub_partitions) > 0 and sub_partitions[0].start_fid < pg_new.start:
        sub_partitions.pop(0)
      
      if len(sub_partitions) > 0:
        partition_groups.append(pg_new)
      
      last_pg = pg_new
      pg_new = PartitionGroup()
      pg_new.start = (i+1-partition_num_lb) * partition_size
      pg_new.end = (i+1 + 2*partition_num_lb) * (partition_size - 1)
      pg_new.partitions = sub_partitions[:]
    i += 1
  # add the last one
  if i % checkpoint_i != 0:
    sub_partitions = pg_new.partitions[:]
    while len(sub_partitions) > 0 and sub_partitions[0].start_fid < pg_new.start:
      sub_partitions.pop(0)
    if len(sub_partitions) > 0:
      partition_groups.append(pg_new)
  return partition_groups
