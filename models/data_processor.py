import logging
from typing import Any, List, Tuple, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from models.pipeline import PipelineConfig

logger = logging.getLogger('data processor')


class BaseDataProcessor:
    def __init__(self, config: PipelineConfig, **params: Any):
        self.spark = SparkSession.builder \
            .appName("DataProcessorApp") \
            .config("spark.driver.extraClassPath", "lib/postgresql-42.7.3.jar") \
            .getOrCreate()

        logger.debug(f"Spark session created.")

        self.params = params
        self.config = config

    def extract(self) -> Union[DataFrame, List[DataFrame]]:
        pass

    def transform(self, df: Union[DataFrame, List[DataFrame]]) -> Union[DataFrame, List[DataFrame]]:
        pass

    def load(self, df: Union[DataFrame, List[DataFrame]]) -> None:
        pass

    def join(self, dfs: Union[DataFrame, List[DataFrame]], joiner: Optional[Tuple] = None) -> DataFrame:
        pass

    def run(self) -> None:
        dfs = self.extract()

        if not dfs:
            print("No data extracted. Exiting the run method.")
            return

        if isinstance(dfs, list) and self.config.joiners:
            joiner_before_transform = self.config.joiners.get("join_before_transform")
            if joiner_before_transform:
                df = self.join(dfs, joiner_before_transform)
            else:
                df = dfs if isinstance(dfs, DataFrame) else dfs[0]
        else:
            df = dfs if isinstance(dfs, DataFrame) else dfs[0]

        if "transform" in self.config.config:
            df = self.transform(df)

        if isinstance(df, list) and self.config.joiners:
            joiner_before_load = self.config.joiners.get("join_before_load")
            if joiner_before_load:
                df = self.join(df, joiner_before_load)

        self.load(df)


class DataProcessor(BaseDataProcessor):
    def processor_exception(self, message: str):
        raise Exception(message)

    def extract(self) -> Union[DataFrame, List[DataFrame]]:
        dfs = []
        for func, url, table_name, params in self.config.extractors:
            df = func(self.spark, url, table_name, **params)
            dfs.append(df)

        logger.debug(f"Extractor created.")
        return dfs[0] if len(dfs) == 1 else dfs

    def transform(self, df: Union[DataFrame, List[DataFrame]]) -> Union[DataFrame, List[DataFrame]]:
        if not self.config.transformers:
            return df if isinstance(df, list) else [df]

        dfs = df if isinstance(df, list) else [df]
        for func, params in self.config.transformers:
            params = {**params, **self.params}
            dfs = [func(d, **params) for d in dfs]

        logger.debug(f"Transformer created.")
        return dfs if len(dfs) > 1 else dfs[0]

    def load(self, df: Union[DataFrame, List[DataFrame]]) -> None:
        if self.config.settings.get("use_cache_in_loaders") is True:
            if isinstance(df, DataFrame):
                df.cache()
            else:
                for d in df:
                    d.cache()

        for func, url, table_name, params in self.config.loaders:
            for d in (df if isinstance(df, list) else [df]):
                func(d, url, table_name, **params)

        logger.debug(f"Loader created.")

    def join(self, dfs: Union[DataFrame, List[DataFrame]], joiner: Optional[Tuple] = None) -> DataFrame:
        if not dfs:
            raise self.processor_exception("No data to join")

        if isinstance(dfs, DataFrame):
            return dfs

        if joiner is None:
            raise self.processor_exception("Got multiple dataframes, but no joiner provided")

        func, params = joiner
        params = {**params, **self.params}

        logger.debug(f"Joiner created.")
        return func(dfs, **params)
