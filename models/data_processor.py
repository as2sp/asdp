from logging_config import data_processor_logger
from typing import Any, List, Tuple, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from models.pipeline import PipelineConfig


class BaseDataProcessor:
    """
    Base class for data processing, containing the main methods for executing ETL processes.
    """
    def __init__(self, config: PipelineConfig, **params: Any):
        """
        Initialize the base data processor.

        :param config: Pipeline configuration.
        :param params: Additional parameters for data processing.
        """
        self.spark = SparkSession.builder \
            .appName("DataProcessorApp") \
            .config("spark.driver.extraClassPath", "db_drivers/postgresql-42.7.3.jar") \
            .getOrCreate()
        data_processor_logger.debug(f"Spark session created.")
        self.params = params
        self.config = config

    def extract(self) -> Union[DataFrame, List[DataFrame]]:
        """
        Extract data method to be implemented by subclasses.

        :return: Extracted data from source as a DataFrame or a list of DataFrames.
        """
        pass

    def transform(self, df: Union[DataFrame, List[DataFrame]]) -> Union[DataFrame, List[DataFrame]]:
        """
        Transform data method to be implemented by subclasses.

        :param df: Input DataFrame or list of DataFrames to transform.
        :return: Transformed data as a DataFrame or a list of DataFrames.
        """
        pass

    def load(self, df: Union[DataFrame, List[DataFrame]]) -> None:
        """
        Load data method to be implemented by subclasses.

        :param df: DataFrame or list of DataFrames to load to destination.
        """
        pass

    def join(self, dfs: Union[DataFrame, List[DataFrame]], joiner: Optional[Tuple] = None) -> DataFrame:
        """
        Join multiple DataFrames method to be implemented by subclasses.

        :param dfs: DataFrame or list of DataFrames to join.
        :param joiner: Optional tuple specifying the join function and its parameters.
        :return: Joined DataFrame.
        """
        pass


class DataProcessor(BaseDataProcessor):
    """
    Data processor class extending the base data processor with specific ETL process implementations.
    """
    def processor_exception(self, message: str):
        """
        Raise an exception with the given message.

        :param message: Message for the exception.
        """
        raise Exception(message)

    def run(self) -> None:
        """
        Run the data processing pipeline.
        """
        data_processor_logger.debug("Starting pipeline execution")
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

    def extract(self) -> Union[DataFrame, List[DataFrame]]:
        """
        Extract data from the sources specified in the pipeline configuration.

        :return: Extracted data as a DataFrame or a list of DataFrames.
        """
        data_processor_logger.info("Extracting data")
        dfs = []
        for extractor in self.config.extractors:
            func, params = extractor
            df = func(self.spark, **params)
            dfs.append(df)
        data_processor_logger.info("Data extraction completed")
        return dfs[0] if len(dfs) == 1 else dfs

    def transform(self, df: Union[DataFrame, List[DataFrame]]) -> Union[DataFrame, List[DataFrame]]:
        """
        Transform the extracted data using the transformers specified in the pipeline configuration.

        :param df: DataFrame or list of DataFrames to transform.
        :return: Transformed data as a DataFrame or a list of DataFrames.
        """
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
        """
        Load the transformed data into the specified destinations.

        :param df: DataFrame or list of DataFrames to load.
        """
        data_processor_logger.info("Loading data")
        if isinstance(df, DataFrame):
            df.cache()
        else:
            for d in df:
                d.cache()

        for loader in self.config.loaders:
            func, params = loader
            for d in (df if isinstance(df, list) else [df]):
                func(d, **params)
        data_processor_logger.info("Data loading completed")

    def join(self, dfs: Union[DataFrame, List[DataFrame]], joiner: Optional[Tuple] = None) -> DataFrame:
        """
        Join multiple DataFrames using the specified joiner function.

        :param dfs: DataFrame or list of DataFrames to join.
        :param joiner: Optional tuple specifying the join function and its parameters.
        :return: Joined DataFrame.
        """
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
