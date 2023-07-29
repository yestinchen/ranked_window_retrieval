from collections import defaultdict
import math
import heapq
from rankedvq.online.metrics import OnlineMetrics
from rankedvq.offline.partition_builder import Partition
from rankedvq.online.base_processor import PartitionGroup, generate_partition_groups
from rankedvq.online.topk_holder import TopkBookKeeperBreakTie
from rankedvq.online.utils import count_frames_in_interval, init_window_score_arr, map_bitset, select_bitset_objs, update_topk
from rankedvq.bitarray import bitarray
from rankedvq.online.window_score_computor import update_window_score_arr_1
import logging

node_num = 0

debug_set = frozenset(['00106830', '00106824', '00106829', '00106835', '00106825', '00106827'])
class WorkingPartitionGroup:
  def __init__(self, partition_group: PartitionGroup, 
    partition_num_lb: int, partition_num_ub: int, 
    partition_size: int, w: int, query: list, labels: set, optimize=False, metrics = None) -> None:
    self.pg = partition_group
    self.partition_num_lb = partition_num_lb
    self.partition_num_ub = partition_num_ub
    self.partition_size = partition_size
    self.w = w
    self.labels = labels
    self.query = query
    self.optimize = optimize
    self.metrics = metrics

    # compute common_mask
    self.sorted_common_objs = partition_group.select_common_objs()
    self.common_objs = set(self.sorted_common_objs)
    self.common_objs_len = len(self.sorted_common_objs)

    self.working_partitions = []
    self.__cache_mapped_bitsets = dict()
    
    for p in partition_group.partitions:
      partition_mask = self.__compute_mask(p.objs)
      mapping = self.__compute_mask_mapping(p.objs)
      wp = WorkingPartition(p, partition_mask, mapping, query, labels)
      mapped_label_masks = self.__compute_mapped_label_masks(wp.label_masks, mapping)
      wp.mapped_label_masks = mapped_label_masks
      self.working_partitions.append(wp)
      self.__cache_mapped_bitsets[p.start_fid] = set()

    self.ordered_wps = sorted(self.working_partitions, key=lambda x: x.partition.start_fid)
    self.remaining_max = self.__estimate_max(partition_num_ub)

    # self.window_score_arr = init_window_score_arr(self.pg.start, self.pg.end, w, 0)

    # store working partitions in pq
    self.wp_pq = [(-wp.remaining_max, wp) for wp in self.working_partitions if wp.remaining_max > 0]
    heapq.heapify(self.wp_pq)

    self.__cache_computed_bitset = set()
    self.min_score = 0
    self.base_idx = partition_group.start

    # store the object set.
    # self.score_obj_sets = [None for _ in self.window_score_arr]
    self.window_num = self.pg.end - self.pg.start - w + 2
    if self.window_num <=0 : 
      self.window_num = 1

    # compute mapped label_masks for each partition
  
  def __lt__(self, other):
    return self.pg.start < other.pg.start
  
  def __compute_mask(self, objs: list):
    bitset = bitarray(len(objs))
    for i, obj in enumerate(objs):
      bitset[i] = obj in self.common_objs
    return bitset
  
  def __compute_mask_mapping(self, objs: list):
    mapping = dict()
    for i, obj in enumerate(objs):
      try:
        mapping[i] = self.sorted_common_objs.index(obj)
      except ValueError:
        pass
    return mapping
  
  def __compute_mapped_label_masks(self, label_masks: dict, mapping: dict):
    mapped_label_masks = dict()
    for label, mask_and_objs in label_masks.items():
      # convert 
      mapped = map_bitset(mask_and_objs, mapping, self.common_objs_len)
      mapped_label_masks[label] = mapped
    return mapped_label_masks

  def __estimate_max(self, partition_num_ub):
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
  
  def __eval_query(self, mapped_bitset, query, mapped_label_masks):
    '''
    return True if the bitset satisfies the query
    '''
    # print('bitset', mapped_bitset)
    # print('car', mapped_label_masks.get('car'))
    # print('truck', mapped_label_masks.get('truck'))
    # print('---')
    for sub_query in query:
      satisfied = False
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
    return True

  def process_until(self, stop_score, bookkeeper):
    # logging.info("working on partition group: [%s,%s], stop score: %s", self.pg.start, self.pg.end, stop_score)
    # logging.info('status: remaining_max: %s, stop_score: %s, topk_min: %s, partitions: %s', self.remaining_max,
      # stop_score, bookkeeper.min, len(self.wp_pq))
    while self.remaining_max >= stop_score and self.remaining_max > bookkeeper.min and len(self.wp_pq) > 0:
      # get next partition
      wp:WorkingPartition = heapq.heappop(self.wp_pq)[1]
      # logging.info('working on partition: %s', wp.partition.start_fid)
      current_node = wp.next_node()
      if current_node is None:
        # print('next is None')
        # print('position', wp.current_pos)
        # means we are done with this;
        break
      # print('next is not null')
      # print('node', current_node)
      # store obj_set, interval
      objs_interval_dict = defaultdict(list)
      __cached_mapped_bitset = self.__cache_mapped_bitsets[wp.partition.start_fid]

      selected_objs = select_bitset_objs(wp.partition.objs, current_node.payload.bitset)
      # if self.pg.start == 77400:
      #   print('whatp---')
      # if len(set(selected_objs).intersection(debug_set)) == len(debug_set) and self.pg.start == 77400:
      #   print('hi objs', selected_objs, current_node.payload.intervals, current_node.payload.count)
      #   print('pg', self.pg.start)

      processed = False
      # if current_node.id == 11944:
      #   print('processing node', current_node)
      # print('processing node', current_node)
      if current_node.obj in self.common_objs:
        # try to map it.
        # 1. map bitset to the global bitset & compute
        mapped_bitset = map_bitset(current_node.payload.bitset, wp.mask_mapping, self.common_objs_len)
        # skip if already proceed.
        if mapped_bitset not in __cached_mapped_bitset:
          eval_result = self.__eval_query(mapped_bitset, self.query, wp.mapped_label_masks)
          if eval_result:
            processed = True
            if self.metrics is not None:
              self.metrics.inc('node_processed')
              self.metrics.inc('node_computed_between_partitions')
            # retrieve from other partitions.
            current_node_objs = select_bitset_objs(wp.partition.objs, current_node.payload.bitset)
            # print('node, ', current_node_objs)
            current_node_common_objs = [obj for obj in current_node_objs \
              if obj in self.common_objs]

            # collect nodes from other partitions.
            result_per_partition_list = self.__collect_intermediate_results(current_node, 
              mapped_bitset, current_node_common_objs, wp)
            self.__aggregate_bitset_results_from_partitions(result_per_partition_list, objs_interval_dict)
          __cached_mapped_bitset.add(mapped_bitset)
      if not processed:
        if self.metrics is not None:
          self.metrics.inc('node_processed')
        eval_result = self.__eval_query(current_node.payload.bitset, self.query, wp.label_masks)
        selected_objs = select_bitset_objs(wp.partition.objs, current_node.payload.bitset)
        # if self.pg.start == 77400:
        #   print('whatp---')
        # if len(set(selected_objs).intersection(debug_set)) == len(debug_set):
        #   print('single partition', selected_objs, current_node.payload.intervals, current_node.payload.count)
        #   print('eval result', eval_result)
        #   print('from pg', self.pg.start)
        if eval_result:
          selected_objs = select_bitset_objs(wp.partition.objs, current_node.payload.bitset)
          objs_interval_dict[frozenset(selected_objs)] = current_node.payload.intervals
          # 2. test if the current bitset could generate a result

      all_sets_and_intervals = [(key, value, count_frames_in_interval(value))\
         for key, value in objs_interval_dict.items()]
      all_sets_and_intervals.sort(key=lambda x: x[2], reverse=True)
      self.__update_window_score_arr(all_sets_and_intervals, bookkeeper)

      # update remaining_max
      self.remaining_max = self.__estimate_max(self.partition_num_ub)
      # push back
      # logging.info('pushing back: %s',wp.partition.start_fid)
      heapq.heappush(self.wp_pq, (-wp.remaining_max, wp))
    # logging.info('updating remaining %s', self.remaining_max)
    if len(self.wp_pq) == 0:
      self.remaining_max = 0
    # update topk
    # update_topk(bookkeeper, self.window_score_arr, self.base_idx, self.score_obj_sets)

  def __collect_intermediate_results(self, current_node, mapped_bitset, current_node_common_objs, wp):
    result_per_partition_list = []
    current_partition_dict = dict()
    current_partition_dict[mapped_bitset] = (current_node.payload.intervals, current_node.payload.count)
    result_per_partition_list.append(current_partition_dict)

    for _, other_wp in self.wp_pq:
      other_nodes = other_wp.get_node_with_obj(current_node.obj)
      if len(other_nodes) == 0:
        continue

      all_nodes_retrieved = []
      for obj in current_node_common_objs:
        retrieved = other_wp.get_node_with_obj(obj)
        # eval.
        # for node in retrieved:
        #   if self.__eval_query(node.payload.bitset, self.query, other_wp.label_masks):
        #     all_nodes_retrieved.append(node)
        all_nodes_retrieved.extend(retrieved)
      
      other_wp_dict = dict()
      result_per_partition_list.append(other_wp_dict)
      for node in all_nodes_retrieved:
        retrieved_bitset = node.payload.bitset
        retrieved_bitset_mapped = map_bitset(retrieved_bitset, other_wp.mask_mapping,
          self.common_objs_len)
        # if retrieved_bitset_mapped not in self.__cache_computed_bitset:
        # evaluate query
        mapped_bitset_and = mapped_bitset & retrieved_bitset_mapped
        if mapped_bitset_and not in self.__cache_computed_bitset and \
          self.__eval_query(mapped_bitset_and, self.query, wp.mapped_label_masks):
          # add
          info_tuple = other_wp_dict.get(mapped_bitset_and)
          if info_tuple is None or info_tuple[1] < node.payload.count:
            other_wp_dict[mapped_bitset_and] = (node.payload.intervals, node.payload.count)
    return result_per_partition_list

  def __aggregate_bitset_results_from_partitions(self, result_per_partition_list, objs_interval_dict):
    # aggregate
    sorted_result_per_partition_list = []
    for _dict in result_per_partition_list:
      bitset_info_tuple = [(key, value[0], value[1]) for key, value in _dict.items()]
      sorted_result_per_partition_list.append(sorted(bitset_info_tuple, key = lambda x:x[2], reverse=True))

    # process partitions
    for i, partition_results in enumerate(sorted_result_per_partition_list):
      for bitset_info_tuple in partition_results:
        if bitset_info_tuple[0] not in self.__cache_computed_bitset:
          # process
          bitset_objs = select_bitset_objs(self.sorted_common_objs, bitset_info_tuple[0])
          bitset_obj_set = frozenset(bitset_objs)
          objs_interval_dict[bitset_obj_set].extend(bitset_info_tuple[1])

          for j, other_partition_results in enumerate(sorted_result_per_partition_list):
            if i == j:
              continue
            for other_bitset_info_tuple in other_partition_results:
              _bitset_result = other_bitset_info_tuple[0] & bitset_info_tuple[0]
              if _bitset_result.count() == bitset_info_tuple[0].count():
                # merge intervals
                # if len(bitset_obj_set.intersection(set(
                #   [x.strip() for x in "person69, person82, person96, person108, person150, person99".split(',')]
                # ))) == 6:
                #   print('got ya from B', self.pg.start, bitset_info_tuple, current_node)
                objs_interval_dict[bitset_obj_set].extend(other_bitset_info_tuple[1])
                # if len(bitset_obj_set.intersection(debug_set)) == len(debug_set):
                #   print(bitset_obj_set)
                #   print(objs_interval_dict[bitset_obj_set])
                # we only need the first one from this partition.
                break
          self.__cache_computed_bitset.add(bitset_info_tuple[0])

  # def __update_window_score_arr(self, all_sets_and_intervals, bookkeeper):
  #   for set_and_interval in all_sets_and_intervals:
  #     if set_and_interval[2] < bookkeeper.min:
  #       break
      
  #     # compute

  def __update_window_score_arr(self, all_sets_and_intervals, bookkeeper):
    for set_and_interval in all_sets_and_intervals:
      # early stop
      if set_and_interval[2] < bookkeeper.min:
        break
      # debug = False
      # if len(set_and_interval[0].intersection(debug_set)) == len(debug_set) \
      #   and len(set_and_interval[0]) == len(debug_set):
      #   print(set_and_interval)
      #   print('gp', self.base_idx, self.pg.end)
      #   print('partitions', [p.start_fid for p in self.pg.partitions])
      #   print('current node', current_node)
      #   debug = True
      # if set_and_interval[2] == 203:
      #   print('got a 203', set_and_interval)
      # sort intervals
      sorted_intervals = sorted(set_and_interval[1],key=lambda x: x[0])
      # debug = False
      # if set_and_interval[2] == 168:
      #   debug = True
      #   print('current set and interval', set_and_interval)
      if len(sorted_intervals) > 0:
        # print('current partition info: baseid', self.base_idx, '')
        # current_min = 0
        # expand interval to arr.
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
        # print('window start', window_start, window_end, 'interval_start', interval_start, interval_end)
        # if debug:
        #   print('window [{},{}], interval [{},{}]'.format(window_start, window_end, interval_start, interval_end))
        # 1. fill initial window.
        for j in range(window_start, window_end):
          if j < interval_start:
            continue
          if j <= interval_end:
            window_score += expanded_interval[j-interval_start]
        # print('init window', window_start, window_end)
        # 2. sliding window.
        # last_window_end = min(interval_end + self.w -1, self.base_idx + len(self.window_score_arr) -1 + self.w - 1)
        last_window_end = min(interval_end + self.w -1, self.base_idx + self.window_num -1 + self.w - 1)

        # print('last window end', last_window_end)
        # if debug:
        #   print('init window score: {}, for [{}, {})'.format(window_score, window_start, window_end))
        #   print('iterating windows with end frame {}, {}'.format(window_end, last_window_end+1))
        for j in range(window_end, last_window_end+1):
          # update window score
          window_arr_pos = j-self.base_idx-self.w + 1
          # print('process window end ', j, 'index', j-interval_start, 'window_arr_pos', j-self.base_idx,\
          #    'window start', window_start)
          # try to add new score
          if j <= interval_end:
            window_score += expanded_interval[j - interval_start]
          if window_score > bookkeeper.min:
            bookkeeper.update(j, window_score, set_and_interval)
          # if window_score > self.window_score_arr[window_arr_pos]:
          #   self.window_score_arr[window_arr_pos] = window_score
          #   self.score_obj_sets[window_arr_pos] = set_and_interval
          # if self.window_score_arr[window_arr_pos] < current_min:
          #   current_min = self.window_score_arr[j]
          # try to remove earliest score.
          try:
            expired_frame = window_start - interval_start
            if expired_frame >= 0:
              window_score -= expanded_interval[expired_frame]
          except IndexError as e:
            raise e
          # if debug:
          #   print('window [{}, {}] score [{}]'.format(window_start + 1, j, window_score))
          window_start += 1

        # for j in range(len(self.window_score_arr)):
        #   _start = j + self.base_idx
        #   _end = _start + self.w - 1
        #   _score = 0
        #   for interval in sorted_intervals:
        #     if interval[1] < _start:
        #       continue
        #     if interval[0] > _end:
        #       break
        #     _max_start = max(_start, interval[0])
        #     _min_end = min(_end, interval[1])
        #     if _min_end >= _max_start:
        #       _score += _min_end - _max_start + 1
        #   # if _score == 60:
        #   #   print('score', _score)
        #   #   print(set_and_interval)
        #   if _score > self.window_score_arr[j]:
        #     # self.window_score_arr[j] = _score
        #     self.score_obj_sets[j] = set_and_interval
          # if self.window_score_arr[j] < current_min:
          #   current_min = self.window_score_arr[j]
        # self.min_score = current_min
  
  def debug_summary(self):
    for wp in self.working_partitions:
      if wp.partition.start_fid == 4800:
        print('p4800: pg start:',  self.pg.start, 'pg remaining:', self.remaining_max,\
          'wp pos:', wp.current_pos, 'wp max:', wp.remaining_max)
        print('current min', self.min_score)


class WorkingPartition:
  def __init__(self, partition: Partition, common_mask: bitarray,
     mask_mapping: dict, query: list, labels: set) -> None:
    # get all nodes by labels
    label_masks = partition.payload.label_masks
    obj_node_dict = partition.payload.obj_index
    self.time_consuming_init(label_masks, labels, obj_node_dict)
    # for i, obj_node in enumerate(all_nodes):
    #   if obj_node.id == 11944:
    #     print('adding to working partition', obj_node)
    #     print('working partition', partition.start_fid, i)
    
    # sort.
    self.current_pos = -1
    self.remaining_max = 0
    if len(self.all_nodes) > 0:
      self.remaining_max = self.all_nodes[0].payload.count
    self.obj_node_dict = obj_node_dict
    self.common_mask = common_mask
    self.mask_mapping = mask_mapping
    self.partition = partition
    self.query = query
    self.label_masks = dict()
    for label, label_values in label_masks.items():
      self.label_masks[label] = label_values[0]
    self.mapped_label_masks = None

  def time_consuming_init(self, label_masks, labels, obj_node_dict):
    # retrieve nodes from the index
    all_nodes = []
    # retrieved_objs = self._extend_objs(label_masks, labels)
    # self._collect_nodes(retrieved_objs, all_nodes, obj_node_dict)
    retrieved_objs = set()
    for label, label_values in label_masks.items():
      if label in labels:
        # do we need to filter according to obj_labels?
        retrieved_objs.update(label_values[1])
    for obj in retrieved_objs:
      all_nodes.extend(obj_node_dict[obj][1])
    self._sort(all_nodes)
    global node_num
    node_num += len(all_nodes)
    self.all_nodes = all_nodes

  def _extend_objs(self, label_masks, labels):
    retrieved_objs = set()
    for label, label_values in label_masks.items():
      if label in labels:
        # do we need to filter according to obj_labels?
        retrieved_objs.update(label_values[1])
    return retrieved_objs

  def _collect_nodes(self, retrieved_objs, all_nodes, obj_node_dict):
    for obj in retrieved_objs:
      all_nodes.extend(obj_node_dict[obj][1])


  def _sort(self, all_nodes):
    all_nodes.sort(key=lambda x: x.payload.count, reverse=True)


  def __is_a_candidate_for_query(self, node):
    bitset = node.payload.bitset
    for sub_query in self.query:
      satified = False
      for labels, op, value in sub_query:
        bitset_result = bitarray(bitset)
        missing_label = False
        for label in labels:
          label_mask = self.label_masks.get(label)
          if label_mask is not None:
            bitset_result &= label_mask
          else:
            missing_label = True
        # skip to the next sub_query if satisfied
        if bitset_result.count() >= value and not missing_label:
          satified = True
          break
      if not satified:
        return False
    return True
    
  
  def next_node(self):
    if self.remaining_max == 0:
      return None
    
    while True:
      self.__move_to_next()
      if self.current_pos >= len(self.all_nodes) or \
        self.__is_a_candidate_for_query(self.all_nodes[self.current_pos]):
        break
    
    if self.current_pos < len(self.all_nodes):
      return self.all_nodes[self.current_pos]
    # set remaining to 0
    self.remaining_max = 0
    return None

  def get_node_with_obj(self, obj):
    if obj in self.obj_node_dict:
      return self.obj_node_dict[obj][1]
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

class BitsetMultiLabelProcessor():
  
  def __init__(self, partitions, partition_size, total_frames_num, optimize):
      self.partitions = partitions
      self.partition_size = partition_size
      self.optimize = optimize
      self.total_frames_num = total_frames_num
      self.metrics = OnlineMetrics()
  
  def __extract_labels_from_query(self, query):
    label_set = set()
    for sub_query in query:
      for labels, op, value in sub_query:
        label_set.update(labels)
    return label_set
  
  def topk(self, query, k, w, pg_size):
    self.metrics.reset()
    # restrict k
    k = min(k, self.total_frames_num - w + 1)
    # query.

    partition_num_lb = math.ceil(w/self.partition_size)
    partition_num_ub = math.ceil((w-1)/self.partition_size) + 1

    # logging.info("partition_num bounds: [%s, %s]", partition_num_lb, partition_num_ub)
    
    labels = self.__extract_labels_from_query(query)

    def __prune_partition(partition):
      # check if all conditions are satisfied.
      label_masks = partition.payload.label_masks
      for sub_query in query:
        satisfied = False
        for labels, op, value in sub_query:
          # all labels should be satified.
          bitset_result = None
          missing_label = False
          for label in labels:
            label_value = label_masks.get(label)
            if label_value is not None:
              if bitset_result is None:
                bitset_result = bitarray(label_value[0])
              else:
                bitset_result &= label_value[0]
            else:
              missing_label = True
          # count # of set bits
          if bitset_result is not None and not missing_label:
            satisfied |= bitset_result.count() >= value
          if satisfied:
            break
        if not satisfied:
          # prune this out.
          return True
      # take this.
      return False

    partition_groups = generate_partition_groups(self.partitions, 
      partition_num_lb, self.partition_size, __prune_partition, pg_size)
    
    # logging.info("number of partition groups: %s", len(partition_groups))
    # map to working partition group
    working_pgs = [WorkingPartitionGroup(pg, partition_num_lb, 
      partition_num_ub, self.partition_size, w, query, labels, self.optimize, self.metrics) \
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
      # logging.info('remaining max: %s, bookkeeper min: %s', wpg.remaining_max, bookkeeper.min)
      if wpg.remaining_max > bookkeeper.min and wpg.remaining_max > 0 and bookkeeper.min < w:
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
    # wpg78000 = None
    # wpg77400 = None
    # for wpg in working_pgs:
    #   if wpg.pg.start == 77400:
    #     wpg77400 = wpg
    #   if wpg.pg.start == 78000:
    #     wpg78000 = wpg
    #   wpg.debug_summary()
    # print('node num', node_num)
    return bookkeeper.cache_list