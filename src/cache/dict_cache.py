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
        self.cache = {}

    def _hash_args(self, args):
        # No good hash function for panda DataFrame?
        # We should use table id as unique key
        argshash = hashlib.md5(args.to_csv().encode('utf-8')).hexdigest()

    def put(self, func: Callable, args: pd.DataFrame, retval: pd.DataFrame) -> bool:
        fname = func.__name__
        if fname not in self.cache:
            self.cache[fname] = {}
        # pandas.util.hash_pandas_object does not work on internal numpy array.
        argshash = self._hash_args(args)
        if argshash not in self.cache[fname]:
            self.cache[fname][argshash] = []

        for input, output in self.cache[fname][argshash]:
            if args.equals(input):
                if retval.equals(output):
                    LoggingManager().log("Exact funtion call has been cached.\n\
                                         Function: %s\n\
                                         Args: %s" % (fname, args),
                                         LoggingLevel.WARNING)
                    return True
                else:
                    LoggingManager().log("Exact funtion call has been cached\
                                         (but different output).\n\
                                         Function: %s\n\
                                         Args: %s\n\
                                         Old: %s\n\
                                         New: %s"
                                         % (fname, args, output, retval),
                                         LoggingLevel.WARNING)
                    return False

        self.cache[fname][argshash].append((args, retval))
        return True


    def get(self, func: Callable, args: pd.DataFrame) -> (bool, pd.DataFrame):
        fname = func.__name__
        if fname not in self.cache:
            return (False, None)
        argshash = self._hash_args(args)
        if argshash not in self.cache[fname]:
            return (False, None)

        for input, output in self.cache[fname][argshash]:
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
            LoggingManager.log("Unable to acquire function name.")
            return func(args)

        argshash = self._hash_args(args)
        if fname in self.cache and argshash in self.cache[fname]:
            for input, output in self.cache[fname][argshash]:
                if args.equals(input):
                    return output

        # No hit
        retval = func(args)
        if fname not in self.cache:
            self.cache[fname] = {}
        if argshash not in self.cache[fname]:
            self.cache[fname][argshash] = []
        self.cache[fname][argshash].append((args, retval))
        return retval
