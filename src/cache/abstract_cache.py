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

from typing import Callable
from abc import ABCMeta, abstractmethod


class AbstractCache(metaclass=ABCMeta):

    @abstractmethod
    def put(self, func: Callable, args: pd.DataFrame, retval: pd.DataFrame) -> bool:
        """
            Update the cache with this function call
        """

    @abstractmethod
    def get(self, func: Callable, args: pd.DataFrame) -> (bool, pd.DataFrame):
        """
            Look for the function in the cache.
            Returns a boolean indicating whether the miss or hit
            and the output of the function if hit.
        """
