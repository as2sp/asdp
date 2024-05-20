import logging
from models.pipeline import PipelineConfig
from models.data_processor import DataProcessor

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.FileHandler("asdp_app.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger('main')


def run_pipeline(config_path: str):
    config = PipelineConfig(config_path)
    data_processor = DataProcessor(config)
    data_processor.run()


if __name__ == "__main__":
    logger.info(f"App started")
    config_paths = [
        "pipelines/test.yaml",
        "pipelines/test2.yaml"
        ]

    for config_path in config_paths:
        run_pipeline(config_path)
