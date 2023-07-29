# print statistics.
from scripts import configs
from rankedvq.io import filter_frames_with_types, read_type_grouped_file

def get_statistics(dataset, read_type):
  frames = read_type_grouped_file(configs.dataset_grouped_template.format(video_file = dataset))
  frames = filter_frames_with_types(frames, [read_type])

  unique_ids = set()
  max_obj_per_frame = 0
  obj_per_frames = []
  for frame in frames:
    obj_num = 0
    for type, ids in frame.items():
      unique_ids.update(ids)
      obj_num += len(ids)
    if obj_num > max_obj_per_frame:
      max_obj_per_frame = obj_num
    obj_per_frames.append(obj_num)
  
  frame_num = len(frames)
  obj_num = len(unique_ids)

  return frame_num, obj_num, sum(obj_per_frames)/len(obj_per_frames), max_obj_per_frame


if __name__ == '__main__':
  print("{:<10}{:>10}{:>15}{:>15}{:>15}".format("video", "# of frames", "# of objects", "# objects/frame", "max # objects/frame"))
  for video_file, read_type in configs.video_files:
    values = get_statistics(video_file, read_type)
    print("{:<10}{:>10.0f}k{:>15.2f}k{:>15.2f}{:>15}".format(video_file, values[0]/1000, values[1]/1000, values[2], values[3] ))