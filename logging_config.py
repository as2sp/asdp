import logging

# Please adjust the logging level here to exclude debugging of all Python packages.
# Then you will observe only debug events related to the application's set loggers.
logging_level = 'INFO'

main_logger = logging.getLogger('main')
pipeline_builder_logger = logging.getLogger('pipeline builder')
data_processor_logger = logging.getLogger('data processor')
extractor_logger = logging.getLogger('extractor')
transformer_logger = logging.getLogger('transformer')
loader_logger = logging.getLogger('loader')


def setup_logging():
    """
    Set up logging configuration for the application.

    This function configures logging to output logs to both a file and the console.
    The logging level and format are set according to the application's requirements.
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[
                            logging.FileHandler("asdp_app.log"),
                            logging.StreamHandler()
                        ])

    level = getattr(logging, logging_level)

    main_logger.setLevel(level)
    pipeline_builder_logger.setLevel(level)
    data_processor_logger.setLevel(level)
    extractor_logger.setLevel(level)
    transformer_logger.setLevel(level)
    loader_logger.setLevel(level)
