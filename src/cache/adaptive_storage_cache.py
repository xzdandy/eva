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
import time

from typing import Callable, List
from src.cache.abstract_cache import AbstractCache
from src.models.storage.batch import Batch
from src.catalog.models.df_metadata import DataFrameMetadata
from src.catalog.catalog_manager import CatalogManager
from src.parser.create_statement import ColumnDefinition, \
    ColConstraintInfo
from src.utils.generic_utils import generate_file_path
from src.catalog.column_type import ColumnType
from src.storage import StorageEngine
from src.parser.types import ParserColumnDataType

from src.utils.logging_manager import LoggingLevel
from src.utils.logging_manager import LoggingManager


class AdaptiveStorageCache(AbstractCache):

    """
    In memory cache based on Dictionary. For testing purpose only.
    """

    def __init__(self):
        self.cache = {}

    def execute(self, func: Callable, args: Batch) -> Batch:
        """
            Notice: args can be a dataframe with mutiple rows.
            Run with row by row?
        """
        try:
            fname = func.__name__
        except:
            LoggingManager().log("Unable to acquire function name.",
                                 LoggingLevel.WARNING)
            return Batch(func(args.frames))

        cache, table_meta = self._load_cache_table(fname)
        outcome = []
        for i in range(len(args.frames)):
            key = '%s.%s' % (args.index_column.name, args.index_column[i])
            if key in cache:
                print('hit: %s' % key)
                outcome.append(self._get_cache(cache, key))
            else:
                print('miss: %s' % key)
                retval = func(args.frames.iloc[[i]])
                self._write_cache(cache, table_meta, key, retval)
                outcome.append(retval)
        return Batch(pd.concat(outcome, ignore_index=True))

    def drop(self):
        self.cache = {}
        LoggingManager().log('Dropping for the persisent stored cache results is '
                             'not implemented yet.', LoggingLevel.ERROR)

    def _load_cache_table(self, fname: str):
        """
        Load cache table from the StorageEngine if existing
        or create an new one.
        """
        start_time = time.perf_counter()
        table_name = self._cache_table_name(fname)
        table_meta = self._bind_cache_table(table_name)
        if table_meta is None:
            table_meta = self._create_cache_metadata(table_name)
            self.cache[table_name] = {}
        elif table_name not in self.cache:
            self.cache[table_name] = {}
            for batch in StorageEngine.read(table_meta):
                df = batch.frames
                for key, value in zip(df['key'], df['value']):
                    key = key.decode()
                    value = value.decode()
                    self.cache[table_name][key] = value
        end_time = time.perf_counter()

        print('Load cache table for %s costs: %.4f' %
              (table_name, end_time-start_time))
        print(self.cache[table_name])
        return self.cache[table_name], table_meta

    def _write_cache(self, cache: dict, table_meta: DataFrameMetadata,
                     key: str, value: pd.DataFrame):
        try:
            json_str = value.to_json()
        except:
            LoggingManager().log('The output: %s is not JSON serializable'
                                 % value, LoggingLevel.WARNING)
            return
        df = pd.DataFrame([{'key': key, 'value': json_str}])
        StorageEngine.write(table_meta, Batch(df))
        cache[key] = json_str

    def _get_cache(self, cache: dict, key: str) -> pd.DataFrame:
        return pd.read_json(cache[key])

    def _cache_table_name(self, func_name: str) -> str:
        return 'CACHE_%s' % func_name

    def _bind_cache_table(self, table_name: str) -> DataFrameMetadata:
        catalog = CatalogManager()
        return catalog.get_dataset_metadata(None, table_name)

    def _create_cache_metadata(self, table_name: str) -> DataFrameMetadata:
        """
        Here we build a key-value store in StorageEngine
        """
        catalog = CatalogManager()
        columns = [ColumnDefinition('key', ParserColumnDataType.TEXT, [],
                                    ColConstraintInfo(unique=True))]
        columns.append(ColumnDefinition('value', ParserColumnDataType.TEXT, []))
        col_metadata = create_column_metadata(columns)
        uri = str(generate_file_path(table_name))
        metadata = catalog.create_metadata(table_name, uri, col_metadata,
                                           identifier_column='key')
        return metadata


def xform_parser_column_type_to_catalog_type(
        col_type: ParserColumnDataType) -> ColumnType:
    """translate parser defined column type to the catalog type

    Arguments:
        col_type {ParserColumnDataType} -- input parser column type

    Returns:
        ColumnType -- catalog column type
    """
    if col_type == ParserColumnDataType.BOOLEAN:
        return ColumnType.BOOLEAN
    elif col_type == ParserColumnDataType.FLOAT:
        return ColumnType.FLOAT
    elif col_type == ParserColumnDataType.INTEGER:
        return ColumnType.INTEGER
    elif col_type == ParserColumnDataType.TEXT:
        return ColumnType.TEXT
    elif col_type == ParserColumnDataType.NDARRAY:
        return ColumnType.NDARRAY


def create_column_metadata(col_list: List[ColumnDefinition]):
    """Create column metadata for the input parsed column list. This function
    will not commit the provided column into catalog table.
    Will only return in memory list of ColumnDataframe objects

    Arguments:
        col_list {List[ColumnDefinition]} -- parsed col list to be created
    """
    if isinstance(col_list, ColumnDefinition):
        col_list = [col_list]

    result_list = []
    for col in col_list:
        if col is None:
            LoggingManager().log(
                "Empty column while creating column metadata",
                LoggingLevel.ERROR)
            result_list.append(col)
        col_type = xform_parser_column_type_to_catalog_type(col.type)
        result_list.append(
            CatalogManager().create_column_metadata(
                col.name, col_type, col.dimension))

    return result_list
