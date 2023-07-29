from collections import defaultdict
import math
import heapq
from rankedvq.offline.partition_builder import Partition
from rankedvq.online.base_processor import PartitionGroup, generate_partition_groups
from rankedvq.online.topk_holder import TopkBookKeeperBreakTie
from rankedvq.online.utils import count_frames_in_interval, init_window_score_arr, map_bitset, select_bitset_objs, update_topk
from rankedvq.bitarray import bitarray
import logging

class WorkingPartitionGroup:
  def __init__(self, partition_group: PartitionGroup, 
    partition_num_lb: int, partition_num_ub: int, 
    partition_size: int, w: int, obj_num: int, optimize=False) -> None:
    self.pg = partition_group
    self.partition_num_lb = partition_num_lb
    self.partition_num_ub = partition_num_ub
    self.partition_size = partition_size
    self.w = w
    self.obj_num = obj_num
    self.optimize = optimize

    # compute common_mask
    self.sorted_common_objs = partition_group.select_common_objs()
    self.common_objs = set(self.sorted_common_objs)

    self.working_partitions = [WorkingPartition(p, self.__compute_mask(p.objs), \
      self.__compute_mask_mapping(p.objs)) for p in partition_group.partitions]

    self.ordered_wps = sorted(self.working_partitions, key=lambda x: x.partition.start_fid)
    self.remaining_max = self.__estimate_max(self.working_partitions, partition_num_ub)

    self.window_score_arr = init_window_score_arr(self.pg.start, self.pg.end, w, 0)

    # store working partitions in pq
    self.wp_pq = [(-wp.remaining_max, wp) for wp in self.working_partitions if wp.remaining_max > 0]
    heapq.heapify(self.wp_pq)

    self.__cache_computed_bitset = set()
    self.min_score = 0
    self.base_idx = partition_group.start
    # if self.base_idx == 78000:
    #   print('exists 78000')
  
  def __lt__(self, other):
    return self.pg.start < other.pg.start
  
  def __compute_mask(self, objs: list):
    bitset = bitarray(len(objs))
    for i in range(len(objs)):
      bitset[i] = objs[i] in self.sorted_common_objs
    return bitset
  
  def __compute_mask_mapping(self, objs: list):
    mapping = dict()
    for i, obj in enumerate(objs):
      try:
        mapping[i] = self.sorted_common_objs.index(obj)
      except ValueError:
        pass
    return mapping

  def __estimate_max(self, working_partitions, partition_num_ub):
    # ensure ordered.
    # do we need to copy working_partitions?
    partition_estimated_max = [wp.remaining_max for wp in self.ordered_wps]

    # sliding window.
    values_per_partition_num_ub = []
    _sum = 0
    for i in range(len(partition_estimated_max)):
      _sum += partition_estimated_max[i]
      if i + 1 >= partition_num_ub:
        values_per_partition_num_ub.append(_sum)
        _sum -= partition_estimated_max[i - partition_num_ub + 1]
    # add if # partitions = partition_num_lb
    if len(partition_estimated_max) < partition_num_ub:
      values_per_partition_num_ub.append(_sum)
    return max(values_per_partition_num_ub)

  def process_until(self, stop_score, bookkeeper):
    # logging.info("working on partition group: [%s,%s], stop score: %s", self.pg.start, self.pg.end, stop_score)
    # logging.info('status: remaining_max: %s, stop_score: %s, topk_min: %s, partitions: %s', self.remaining_max,
    #   stop_score, bookkeeper.min, len(self.wp_pq))
    while self.remaining_max >= stop_score and self.remaining_max > bookkeeper.min and len(self.wp_pq) > 0:
      # get next partition
      wp:WorkingPartition = heapq.heappop(self.wp_pq)[1]
      # logging.info('working on partition: %s', wp.partition.start_fid)
      current_node = wp.next_node(self.obj_num)
      if current_node is None:
        # means we are done with this;
        break
      # store obj_set, interval
      objs_interval_dict = defaultdict(list)

      proceed = False

      if current_node.obj in self.common_objs:
        # select objs
        current_node_objs = select_bitset_objs(wp.partition.objs, current_node.payload.bitset)
        current_node_common_objs = [obj for obj in current_node_objs \
          if obj in self.common_objs]

        # check first.
        if len(current_node_common_objs) >= self.obj_num:
          # logging.debug('dealing with objset: %s', current_node_common_objs)
          proceed = True
          # retrieve from other partitions.
          # map to the global (partition group) bitset.
          mapped_bitset = map_bitset(current_node.payload.bitset, wp.mask_mapping, len(self.sorted_common_objs))
          # logging.debug('mapped bitset %s , %s', mapped_bitset, mapped_bitset.count())

          if mapped_bitset in self.__cache_computed_bitset:
            # push back
            # logging.info('skip this bitset, pushing back: %s',wp.partition.start_fid)
            heapq.heappush(self.wp_pq, (-wp.remaining_max, wp))
            continue

          result_per_partition_list = []
          # add the current bitset
          current_partition_dict = dict()
          current_partition_dict[mapped_bitset] = (current_node.payload.intervals, current_node.payload.count)
          result_per_partition_list.append(current_partition_dict)

          for _, other_wp in self.wp_pq:
            other_nodes = other_wp.get_node_with_obj(current_node.obj)
            if len(other_nodes) == 0:
              continue

            # retrieve all related nodes from other_wp
            all_nodes_retrieved = []
            for obj in current_node_common_objs:
              retrieved = other_wp.get_node_with_obj(obj)
              for _node in retrieved:
                _node_bitset = _node.payload.bitset
                if _node_bitset.count() >= self.obj_num and _node_bitset not in self.__cache_computed_bitset: 
                  # FIXME?
                  all_nodes_retrieved.append(_node)
            
            # compute bitset and for this other_wp
            other_wp_dict = dict()
            result_per_partition_list.append(other_wp_dict)
            for node in all_nodes_retrieved:
              retrieved_bitset = node.payload.bitset
              # basic pruning
              if retrieved_bitset.count() < self.obj_num:
                continue
              # prune based on the mapped result.
              retrieved_bitset_mapped = map_bitset(retrieved_bitset, other_wp.mask_mapping, 
                len(self.sorted_common_objs))
              if retrieved_bitset_mapped.count() < self.obj_num or \
                retrieved_bitset_mapped in self.__cache_computed_bitset:
                continue

              # and op
              bitset_and_result = retrieved_bitset_mapped & mapped_bitset

              # check results.
              if bitset_and_result.count() >= self.obj_num and \
                bitset_and_result not in self.__cache_computed_bitset:
                info_tuple = other_wp_dict.get(bitset_and_result)
                if info_tuple is None or info_tuple[1] < node.payload.count:
                  # put new one.
                  other_wp_dict[bitset_and_result] = (node.payload.intervals, node.payload.count)
              # Do we need the following line???
              # self.__cache_computed_bitset.add(retrieved_bitset_mapped)
          
          # begin aggregate.
          sorted_result_per_partition_list = []
          for _dict in result_per_partition_list:
            bitset_info_tuple = [(key, value[0], value[1]) for key, value in _dict.items()]
            sorted_result_per_partition_list.append(sorted(bitset_info_tuple, key=lambda x:x[2], reverse=True))

          # process partitions one by one
          for i, partition_results in enumerate(sorted_result_per_partition_list):
            for bitset_info_tuple in partition_results:
              if self.optimize and bitset_info_tuple[0].count() != self.obj_num:
                # only count bitset with the same obj_num.
                continue
              if bitset_info_tuple[0] not in self.__cache_computed_bitset:
                # process
                bitset_objs = select_bitset_objs(self.sorted_common_objs, bitset_info_tuple[0])
                bitset_obj_set = frozenset(bitset_objs)
                # if len(bitset_obj_set.intersection(set(
                #   [x.strip() for x in "person69, person82, person96, person108, person150, person99".split(',')]
                # ))) == 6:
                #   print('got ya from A', self.pg.start, bitset_info_tuple, current_node)
                objs_interval_dict[bitset_obj_set].extend(bitset_info_tuple[1])

                for j, other_partition_results in enumerate(sorted_result_per_partition_list):
                  if i == j:
                    continue
                  # test if exists the same results
                  # FIXME: could use the dict?
                  for other_bitset_info_tuple in other_partition_results:
                    _bitset_result = other_bitset_info_tuple[0] & bitset_info_tuple[0]
                    if _bitset_result.count() == bitset_info_tuple[0].count():
                      # merge intervals
                      # if len(bitset_obj_set.intersection(set(
                      #   [x.strip() for x in "person69, person82, person96, person108, person150, person99".split(',')]
                      # ))) == 6:
                      #   print('got ya from B', self.pg.start, bitset_info_tuple, current_node)
                      objs_interval_dict[bitset_obj_set].extend(other_bitset_info_tuple[1])
                      break
                # store.
                self.__cache_computed_bitset.add(bitset_info_tuple[0])

      if not proceed and current_node.payload.bitset.count() == self.obj_num:
        # print('bitset', current_node.payload.bitset, current_node.payload.bitset.count())
        # print('objs', wp.partition.objs)
        selected_objs = select_bitset_objs(wp.partition.objs, current_node.payload.bitset)
        # if len(set(selected_objs).intersection(set(
        #   [x.strip() for x in "person69, person82, person96, person108, person150, person99".split(',')]
        # ))) == 6:
        #   print('got ya from C', self.pg.start, current_node.payload.intervals, current_node)
        # print('selected', selected_objs)
        objs_interval_dict[frozenset(selected_objs)] = current_node.payload.intervals
      
      all_sets_and_intervals = [(key, value, count_frames_in_interval(value))\
         for key, value in objs_interval_dict.items()]
      all_sets_and_intervals.sort(key=lambda x: x[2], reverse=True)
      self.__update_window_score_arr(all_sets_and_intervals, bookkeeper)

      # update remaining_max
      self.remaining_max = self.__estimate_max(self.working_partitions, self.partition_num_ub)
      # push back
      # logging.info('pushing back: %s',wp.partition.start_fid)
      heapq.heappush(self.wp_pq, (-wp.remaining_max, wp))
    # update topk
    # update_topk(bookkeeper, self.window_score_arr, self.base_idx)

  def __update_window_score_arr(self, all_sets_and_intervals, bookkeeper):
    for set_and_interval in all_sets_and_intervals:
      if set_and_interval[2] < bookkeeper.min:
        break
      # sort intervals
      sorted_intervals = sorted(set_and_interval[1],key=lambda x: x[0])
      if len(sorted_intervals) > 0:
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
        last_window_end = min(interval_end + self.w -1, self.base_idx + len(self.window_score_arr) -1 + self.w - 1)
        for j in range(window_end, last_window_end+1):
          # update window score
          # window_arr_pos = j-self.base_idx-self.w + 1
          # try to add new score
          if j <= interval_end:
            window_score += expanded_interval[j - interval_start]
          # if window_score > self.window_score_arr[window_arr_pos]:
          #   self.window_score_arr[window_arr_pos] = window_score
          if window_score > bookkeeper.min:
            bookkeeper.update(j, window_score)
          expired_frame = window_start - interval_start
          if expired_frame >= 0:
            window_score -= expanded_interval[expired_frame]
          window_start += 1
    
    # for set_and_interval in all_sets_and_intervals:
    #   if set_and_interval[2] < self.min_score:
    #     break
    #   # if set_and_interval[2] == 190:
    #   #   print('what', set_and_interval)
    #   # sort intervals
    #   sorted_intervals = sorted(set_and_interval[1],key=lambda x: x[0])
    #   if len(sorted_intervals) > 0:
    #     current_min = 10000
    #     for j in range(len(self.window_score_arr)):
    #       _start = j + self.base_idx
    #       _end = _start + self.w - 1
    #       _score = 0
    #       for interval in sorted_intervals:
    #         if interval[1] < _start:
    #           continue
    #         if interval[0] > _end:
    #           break
    #         _max_start = max(_start, interval[0])
    #         _min_end = min(_end, interval[1])
    #         if _min_end >= _max_start:
    #           _score += _min_end - _max_start + 1
    #       # if _start == 4561 and _score == 203:
    #       #   print('score for', _start, _score)
    #       #   print(set_and_interval)
    #       #   print(sorted_intervals)
    #       #   print('pg',self.base_idx)
    #       #   print('partitions', self.pg.partitions[0].start_fid)
    #       if _score > self.window_score_arr[j]:
    #         self.window_score_arr[j] = _score
    #       if self.window_score_arr[j] < current_min:
    #         current_min = self.window_score_arr[j]
    #     self.min_score = current_min

class WorkingPartition:
  def __init__(self, partition: Partition, common_mask: bitarray, mask_mapping: dict) -> None:
    # get all nodes.
    self.all_nodes = partition.payload.all_sorted_nodes
    self.current_pos = -1
    self.remaining_max = 0
    if len(self.all_nodes) > 0:
      self.remaining_max = self.all_nodes[0].payload.count
    self.obj_node_dict = partition.payload.obj_node_dict
    self.common_mask = common_mask
    self.mask_mapping = mask_mapping
    self.partition = partition
  
  def next_node(self, obj_num):
    if self.remaining_max == 0:
      return None
    
    while True:
      self.__move_to_next()
      if self.current_pos >= len(self.all_nodes) or \
        self.all_nodes[self.current_pos].payload.bitset.count() >= obj_num:
        break
    
    if self.current_pos < len(self.all_nodes):
      return self.all_nodes[self.current_pos]
    return None

  def get_node_with_obj(self, obj):
    if obj in self.obj_node_dict:
      return self.obj_node_dict[obj]
    return []
  
  def __move_to_next(self):
    self.current_pos += 1
    if self.remaining_max == 0:
      return
    if self.current_pos + 1 < len(self.all_nodes):
      self.remaining_max = self.all_nodes[self.current_pos+1].payload.count
    else:
      self.remaining_max = 0
  
  def __lt__(self, other):
    return self.partition.start_fid < other.partition.start_fid

class BitsetSingleProcessor():
  
  def __init__(self, partitions, partition_size, total_frames_num, optimize):
      self.partitions = partitions
      self.partition_size = partition_size
      self.optimize = optimize
      self.total_frames_num = total_frames_num
  
  def topk(self, k, w, obj_num, pg_size):
    k = min(k, self.total_frames_num - w + 1)
    partition_num_lb = math.ceil(w/self.partition_size)
    partition_num_ub = math.ceil((w-1)/self.partition_size) + 1

    # logging.info("partition_num bounds: [%s, %s]", partition_num_lb, partition_num_ub)
    
    prune_partition_func = lambda partition: obj_num not in partition.top1_dict

    partition_groups = generate_partition_groups(self.partitions, 
      partition_num_lb, self.partition_size, prune_partition_func, pg_size)
    
    # logging.info("number of partition groups: %s", len(partition_groups))
    # map to working partition group
    working_pgs = [WorkingPartitionGroup(pg, partition_num_lb, 
      partition_num_ub, self.partition_size, w, obj_num, self.optimize) \
        for pg in partition_groups]

    # top-k cache
    bookkeeper = TopkBookKeeperBreakTie(k)

    # use priority queue
    pq = [(-wpg.remaining_max, wpg) for wpg in working_pgs]
    heapq.heapify(pq)

    while len(pq) > 0:
      wpg = heapq.heappop(pq)
      # logging.info("pick partition group: [%s, %s], score: [%s]", wpg[1].pg.start, wpg[1].pg.end, -wpg[0])
      wpg = wpg[1]
      if wpg.remaining_max > bookkeeper.min and wpg.remaining_max > 0:
        stop_score = wpg.remaining_max - 1
        if len(pq) > 0:
          # logging.info('next pg: [%s, %s]', pq[0][1].pg.start, pq[0][1].pg.end)
          stop_score = pq[0][1].remaining_max - 10
        wpg.process_until(stop_score - 1, bookkeeper)
        # add back?
        if wpg.remaining_max > 0:
          # logging.info("update remaining max: %s", wpg.remaining_max)
          heapq.heappush(pq, (-wpg.remaining_max, wpg))

    # sort 
    # working_pgs.sort(key=lambda x : x.remaining_max, reverse=True)
    # while working_pgs[0].remaining_max > bookkeeper.min \
    #   and working_pgs[0].remaining_max > 0:
    #   stop_score = working_pgs[0].remaining_max - 1
    #   if len(working_pgs) > 1:
    #     stop_score = working_pgs[1].remaining_max - 1
    #   working_pgs[0].process_until(stop_score - 1, bookkeeper)
    #   if len(working_pgs) > 1:
    #     # sort
    #     working_pgs.sort(key = lambda x: x.remaining_max, reverse=True)
    return bookkeeper.cache_list