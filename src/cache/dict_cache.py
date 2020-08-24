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

class DictCache(AbstractCache):

    """
    In memory cache based on Dictionary. For testing purpose only.
    """
    def __init__(self):
        self.batchcache = {}
        self.rowcache = {}

    def _hash_args(self, args):
        # No good hash function for panda DataFrame?
        # We should use table id as unique key
        # Drop index
        argshash = hashlib.md5(args.to_csv(index=False).encode('utf-8')).hexdigest()

    def _add(self, cache, fname, argshash, input, output):
        if fname not in cache:
            cache[fname] = {}
        if argshash not in cache[fname]:
            cache[fname][argshash] = []
        cache[fname][argshash].append((input, output))

    def _check(self, cache, fname, argshash, args):
        if fname in cache and argshash in cache[fname]:
            for input, output in cache[fname][argshash]:
                if args.equals(input):
                    return (True, output)
        return (False, None)

    def execute(self, func: Callable, args: pd.DataFrame) -> pd.DataFrame:
        """
            Notice: args can be a dataframe with mutiple rows.
            Run with row by row?
        """
        try:
            fname = func.__name__
        except:
            LoggingManager().log("Unable to acquire function name.",
                                 LoggingLevel.WARNING)
            return func(args)

        # Make sure the index column is consistent
        args.reset_index(drop=True, inplace=True)
        argshash = self._hash_args(args)

        # Check which cache should be used
        if len(args.index) > 1:
            cache = self.batchcache
        else:
            cache = self.rowcache

        # Hit
        hit, output = self._check(cache, fname, argshash, args)
        if hit:
            LoggingManager().log("Hit: %s, %s" % (fname, args),
                                 LoggingLevel.INFO)
            return output

        if len(args.index) > 1:
            # No batch hit
            retval = pd.DataFrame()
            for i in range(len(args.index)):
                # df.iloc[[i]] gives back a dataframe
                retval = retval.append(self.execute(func, args.iloc[[i]]), ignore_index=True)
        else:
            # No row hit
            LoggingManager().log("Miss: %s, %s" % (fname, args),
                                 LoggingLevel.INFO)
            retval = func(args)

        self._add(cache, fname, argshash, args, retval)
        return retval

    def drop(self):
        self.batchcache = {}
        self.rowcache = {}
