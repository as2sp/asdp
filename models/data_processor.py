from logging_config import data_processor_logger
from typing import Any, List, Tuple, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from models.pipeline import PipelineConfig


class BaseDataProcessor:
    def __init__(self, config: PipelineConfig, **params: Any):
        self.spark = SparkSession.builder \
            .appName("DataProcessorApp") \
            .config("spark.driver.extraClassPath", "lib/postgresql-42.7.3.jar") \
            .getOrCreate()
        data_processor_logger.debug(f"Spark session created.")
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
        data_processor_logger.debug("Starting extract phase")
        dfs = self.extract()

        if not dfs:
            data_processor_logger.info("No data extracted. Exiting the run method")
            return

        if isinstance(dfs, list) and self.config.joiners:
            joiner_before_transform = self.config.joiners.get("join_before_transform")
            if joiner_before_transform:
                data_processor_logger.debug("Joining dataframes before transform phase")
                df = self.join(dfs, joiner_before_transform)
            else:
                df = dfs if isinstance(dfs, DataFrame) else dfs[0]
        else:
            df = dfs if isinstance(dfs, DataFrame) else dfs[0]
        data_processor_logger.debug("Starting transform phase")
        if "transform" in self.config.config:
            df = self.transform(df)

        if isinstance(df, list) and self.config.joiners:
            joiner_before_load = self.config.joiners.get("join_before_load")
            if joiner_before_load:
                data_processor_logger.debug("Joining dataframes before load phase")
                df = self.join(df, joiner_before_load)
        data_processor_logger.debug("Starting load phase")
        self.load(df)
        data_processor_logger.debug("Pipeline execution finished")


class DataProcessor(BaseDataProcessor):
    def processor_exception(self, message: str):
        raise Exception(message)

    def extract(self) -> Union[DataFrame, List[DataFrame]]:
        data_processor_logger.info("Extracting data")
        dfs = []
        for func, url, table_name, params in self.config.extractors:
            df = func(self.spark, url, table_name, **params)
            dfs.append(df)
        data_processor_logger.info("Data extraction completed")
        return dfs[0] if len(dfs) == 1 else dfs

    def transform(self, df: Union[DataFrame, List[DataFrame]]) -> Union[DataFrame, List[DataFrame]]:
        data_processor_logger.info("Transforming data")
        if not self.config.transformers:
            return df if isinstance(df, list) else [df]

        dfs = df if isinstance(df, list) else [df]
        for func, params in self.config.transformers:
            params = {**params, **self.params}
            dfs = [func(d, **params) for d in dfs]
        data_processor_logger.info("Data transformation completed")
        return dfs if len(dfs) > 1 else dfs[0]

    def load(self, df: Union[DataFrame, List[DataFrame]]) -> None:
        data_processor_logger.info("Loading data")
        if isinstance(df, DataFrame):
            df.cache()
        else:
            for d in df:
                d.cache()

        for func, url, table_name, params in self.config.loaders:
            for d in (df if isinstance(df, list) else [df]):
                func(d, url, table_name, **params)
        data_processor_logger.info("Data loading completed")

    def join(self, dfs: Union[DataFrame, List[DataFrame]], joiner: Optional[Tuple] = None) -> DataFrame:
        data_processor_logger.info("Joining data")
        if not dfs:
            raise self.processor_exception("No data to join")

        if isinstance(dfs, DataFrame):
            return dfs

        if joiner is None:
            raise self.processor_exception("Got multiple dataframes, but no joiner provided")

        func, params = joiner
        params = {**params, **self.params}

        data_processor_logger.info("Data join completed")
        return func(dfs, **params)
