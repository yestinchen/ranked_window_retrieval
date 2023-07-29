
def get_predefined_queries(obj_type):  
  query0 = [
    [(set([obj_type, 'red']), '>=', 4)]
  ]

  if obj_type == 'car':
    query1 = [
      [(set(['car']), '>=', 2)],
      [(set(['truck']), '>=', 2)]
    ]
  else:
    query1 = [
      [(set(['car']), '>=', 2)],
      [(set(['person']), '>=', 2)]
    ]

  return [
    query0,
    query1
  ]