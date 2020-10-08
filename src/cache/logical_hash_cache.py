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
import pandas as pd
import numpy as np
import hashlib

from typing import Callable
from src.cache.abstract_cache import AbstractCache
from src.utils.logging_manager import LoggingLevel
from src.utils.logging_manager import LoggingManager
from src.models.storage.batch import Batch

class LogicalHashCache(AbstractCache):

    """
    In memory cache based on Dictionary. For testing purpose only.
    """
    def __init__(self):
        self.rowcache = {}


    def execute(self, func: Callable, args: Batch) -> pd.DataFrame:
        """
            Notice: args can be a dataframe with mutiple rows.
            Run with row by row?
        """
        try:
            fname = func.__name__
        except:
            LoggingManager().log("Unable to acquire function name.",
                                 LoggingLevel.WARNING)
            return func(args.frames)
        bash_hash_key = '%s.%s' % (fname, args.index_column.name)
        print('bash_hash_key: %s' % bash_hash_key)
        outcome = []
        for i in range(len(args.frames)):
            key = '%s.%s' % (bash_hash_key, args.index_column[i])
            if key in self.rowcache:
                print('hit: %s' % key)
                outcome.append(self.rowcache[key])
            else:
                print('miss: %s' % key)
                retval = func(args.frames.iloc[[i]])
                self.rowcache[key] = retval
                outcome.append(retval)
        return pd.concat(outcome, ignore_index=True)


    def drop(self):
        self.rowcache = {}
