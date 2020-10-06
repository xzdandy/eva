# coding=utf-8
# Copyright 2018-2020 EVA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import os
import time
import pandas as pd

from src.catalog.catalog_manager import CatalogManager
from src.models.storage.batch import Batch
from test.util import create_sample_video, perform_query
from test.util import DummyLabelDetector, DummyColorDetector
from src.cache import Cache

NUM_FRAMES = 10


class UDFCacheTest(unittest.TestCase):

    def setUp(self):
        CatalogManager().reset()
        create_sample_video(NUM_FRAMES)

        load_query = """LOAD DATA INFILE 'dummy.avi' INTO MyVideo;"""
        perform_query(load_query)

        create_udf_query = """CREATE UDF DummyLabelDetector
                  INPUT  (Frame_Array NDARRAY (3, 256, 256))
                  OUTPUT (label TEXT(10))
                  TYPE  Classification
                  IMPL  'test/util.py';
        """
        perform_query(create_udf_query)

        create_udf_query = """CREATE UDF DummyColorDetector
                  INPUT  (Frame_Array NDARRAY (3, 256, 256))
                  OUTPUT (color TEXT(10))
                  TYPE  Classification
                  IMPL  'test/util.py';
        """
        perform_query(create_udf_query)

    def tearDown(self):
        os.remove('dummy.avi')

    def perform_query_with_time(self, query):
        start_time = time.time()
        actual_batch = perform_query(query)
        end_time = time.time()
        print("%s costs %.2f seconds" % (query, (end_time - start_time)))
        return actual_batch

    # integration test
    def test_should_use_udf_cache_identical_query(self):
        Cache.drop()
        query = "SELECT id,DummyLabelDetector(data) FROM MyVideo;"
        actual_batch = self.perform_query_with_time(query)

        labels = DummyLabelDetector().labels
        expected = [{'id': i, 'label': labels[i % 3]}
                    for i in range(NUM_FRAMES)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        actual_batch.sort()
        self.assertEqual(actual_batch, expected_batch)

        actual_batch = self.perform_query_with_time(query)
        actual_batch.sort()
        self.assertEqual(actual_batch, expected_batch)

    # integration test
    def test_should_use_udf_cache_subsume_query(self):
        Cache.drop()
        query = "SELECT id,DummyLabelDetector(data) FROM MyVideo\
                 WHERE id < 5;"
        actual_batch = self.perform_query_with_time(query)

        labels = DummyLabelDetector().labels
        expected = [{'id': i, 'label': labels[i % 3]}
                    for i in range(5)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        actual_batch.sort()
        self.assertEqual(actual_batch, expected_batch)

        full_query = "SELECT id,DummyLabelDetector(data) FROM MyVideo;"
        actual_batch = self.perform_query_with_time(full_query)
        actual_batch.sort()
        expected = [{'id': i, 'label': labels[i % 3]}
                    for i in range(NUM_FRAMES)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        self.assertEqual(actual_batch, expected_batch)

    # integration test
    def test_should_use_udf_cache_merge_full_query(self):
        Cache.drop()
        query = "SELECT id,DummyLabelDetector(data) FROM MyVideo\
                 WHERE id < 5;"
        actual_batch = self.perform_query_with_time(query)

        labels = DummyLabelDetector().labels
        expected = [{'id': i, 'label': labels[i % 3]}
                    for i in range(5)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        actual_batch.sort()
        self.assertEqual(actual_batch, expected_batch)

        query = "SELECT id,DummyLabelDetector(data) FROM MyVideo\
                 WHERE id >= 5;"
        actual_batch = self.perform_query_with_time(query)
        actual_batch.sort()
        expected = [{'id': i, 'label': labels[i % 3]}
                    for i in range(5, NUM_FRAMES)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        self.assertEqual(actual_batch, expected_batch)

        full_query = "SELECT id,DummyLabelDetector(data) FROM MyVideo;"
        actual_batch = self.perform_query_with_time(full_query)
        actual_batch.sort()
        expected = [{'id': i, 'label': labels[i % 3]}
                    for i in range(NUM_FRAMES)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        self.assertEqual(actual_batch, expected_batch)

    def color_helper(self, i):
        labels = DummyColorDetector().labels
        if i < 3:
            label = labels[0]
        elif i < 6:
            label = labels[1]
        else:
            label = labels[2]
        return label

    # integration test
    def test_should_use_udf_cache_udf_predicate_query(self):
        Cache.drop()
        query = "SELECT id,DummyColorDetector(data) FROM MyVideo\
                 WHERE DummyLabelDetector(data).label = 'person';"
        actual_batch = self.perform_query_with_time(query)
        actual_batch.sort()
        expected = [{'id': i, 'color': self.color_helper(i)}
                    for i in range(NUM_FRAMES) if i % 3 == 1]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        self.assertEqual(actual_batch, expected_batch)


        query = "SELECT id,DummyColorDetector(data) FROM MyVideo\
                 WHERE DummyLabelDetector(data).label = 'bicycle';"
        actual_batch = self.perform_query_with_time(query)
        actual_batch.sort()
        expected = [{'id': i, 'color': self.color_helper(i)}
                    for i in range(NUM_FRAMES) if i % 3 == 2]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        self.assertEqual(actual_batch, expected_batch)

        query = "SELECT id,DummyColorDetector(data) FROM MyVideo;"
        actual_batch = self.perform_query_with_time(query)
        actual_batch.sort()
        expected = [{'id': i, 'color': self.color_helper(i)}
                    for i in range(NUM_FRAMES)]
        expected_batch = Batch(frames=pd.DataFrame(expected))
        self.assertEqual(actual_batch, expected_batch)
