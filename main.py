from logging_config import setup_logging, main_logger
from models.pipeline import PipelineConfig
from models.data_processor import DataProcessor

setup_logging()


def run_pipeline(config_path: str):
    """
    Run the data processing pipeline with the given configuration file.

    This function initializes the pipeline configuration, creates a DataProcessor instance,
    and runs the data processing pipeline.

    Args:
        config_path (str): Path to the pipeline configuration file.
    """
    main_logger.info(f"Starting pipeline with config: {config_path}")
    config = PipelineConfig(config_path)
    data_processor = DataProcessor(config)
    data_processor.run()
    main_logger.info(f"Finished pipeline with config: {config_path}")


if __name__ == "__main__":
    main_logger.info("App started")
    config_paths = [
        "pipelines/test.yaml",
        "pipelines/test2.yaml"
    ]

    for config_path in config_paths:
        run_pipeline(config_path)
