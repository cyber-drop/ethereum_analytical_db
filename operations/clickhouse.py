from operations.indices import ClickhouseIndices

def prepare_indices():
  print("Preparing indices...")
  indices = ClickhouseIndices()
  indices.prepare_indices()