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
from typing import Callable
from src.cache.abstract_cache import AbstractCache
from src.models.storage.batch import Batch

class NoCache(AbstractCache):

    """
    No cache is enabled. Always execute the UDF.
    """
    def __init__(self):
        pass

    def execute(self, func: Callable, args: Batch) -> Batch:
        return Batch(func(args.frames))

    def drop(self):
        pass
