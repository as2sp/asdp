from logging_config import pipeline_builder_logger
import yaml
from typing import List, Tuple, Dict
from jinja2 import Template
import functions.extract as extract_functions
import functions.transform as transform_functions
import functions.load as load_functions
import functions.join as join_functions

# You can add new YAML config here
config_files = ['config/db_connections.yaml',
                'config/db_schemas.yaml']


class PipelineConfig:
    """
    Class to load and manage pipeline configuration, including extracting, transforming, loading, and joining functions.
    """
    def __init__(self, config_path: str):
        """
        Initialize the PipelineConfig.

        :param config_path: Path to the main configuration file.
        """
        self.config_files = config_files
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.extractors = self._load_functions("extract")
        self.transformers = self._load_functions("transform")
        self.loaders = self._load_functions("load")
        self.joiners = self._load_functions("join")

    def _load_config(self, config_path: str) -> Dict:
        """
        Load the main and additional configuration files.

        :param config_path: Path to the main configuration file.
        :return: Merged configuration dictionary.
        """
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        additional_configs = {}
        for file in self.config_files:
            with open(file, "r") as f:
                additional_configs.update(yaml.safe_load(f))

        config = self._render_config(config, additional_configs)
        pipeline_builder_logger.debug('Config files: %s', config_files)
        pipeline_builder_logger.debug(f"Config loaded")
        return config

    def _render_config(self, config: Dict, additional_configs: Dict) -> Dict:
        """
        Render the configuration using Jinja2 templates.

        :param config: Main configuration dictionary.
        :param additional_configs: Additional configurations for rendering.
        :return: Rendered configuration dictionary.
        """
        config_str = yaml.dump(config)
        template = Template(config_str)
        rendered_config_str = template.render(additional_configs)
        return yaml.safe_load(rendered_config_str)

    def _load_functions(self, key: str) -> List[Tuple]:
        """
        Load ETL functions based on the configuration section.

        :param key: Configuration section key ('extract', 'transform', 'load', 'join').
        :return: List of tuples with functions and their parameters.
        """
        functions = []
        if key in self.config:
            for item in self.config[key]:
                func_name = item["operation"].lower()
                params = item.get("parameters", {})
                func = self._get_function_by_name(func_name, key)
                if func is None:
                    raise ValueError(f"Function {func_name} not found in {key} section")
                functions.append((func, params))
        pipeline_builder_logger.debug(f"ETL functions defined")
        return functions

    def _get_function_by_name(self, name: str, section: str):
        """
        Retrieve a function by its name from the appropriate module.

        :param name: Name of the function.
        :param section: Section of the configuration ('extract', 'transform', 'load', 'join').
        :return: Function object.
        """
        module = None
        if section == "extract":
            module = extract_functions
        elif section == "transform":
            module = transform_functions
        elif section == "load":
            module = load_functions
        elif section == "join":
            module = join_functions

        if module is None:
            raise ValueError(f"Invalid section: {section}")

        func = getattr(module, name, None)
        return func
