from models.pipeline import PipelineConfig
from models.data_processor import DataProcessor


def run_pipeline(config_path: str):
    config = PipelineConfig(config_path)
    data_processor = DataProcessor(config)
    data_processor.run()


if __name__ == "__main__":
    config_paths = ["pipelines/test.yaml",
                    "pipelines/test2.yaml"]

    for config_path in config_paths:
        run_pipeline(config_path)
