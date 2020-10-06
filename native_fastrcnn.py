from src.udfs.fastrcnn_object_detector import FastRCNNObjectDetector
from src.executor.execution_context import Context
from src.constants import NO_GPU
from src.readers.opencv_reader import OpenCVReader
import torch
import pandas as pd
import time

BATCH_SIZE = 10

def main():
    context = Context()
    detector = FastRCNNObjectDetector()
    print("Download fastrcnn")
    device = context.gpu_device()
    if device != NO_GPU:
        detector = detector.to_device(device)
        print("enable gpu")

    print("done init.")
    reader = OpenCVReader(file_url='data/ua_detrac/ua_detrac.mp4')
    batch =[]
    start_time = time.perf_counter()
    for item in reader._read(): 
        print ("Processing %sth frame, size: %d bytes" % (item['id'], item['data'].nbytes))
        batch.append(item['data'])
        if len(batch) == BATCH_SIZE:
            outcome = detector(batch)
            batch = []
    if len(batch) > 0:
        outcome = detector(batch)
    end_time = time.perf_counter()
    print(f"Cost {end_time-start_time} secs")

if __name__ == '__main__':
    main()
