import yaml
from typing import List, Tuple, Dict
from jinja2 import Template
import functions.extract as extract_functions
import functions.transform as transform_functions
import functions.load as load_functions
import functions.join as join_functions


# You can add new config YAML here
config_files = ['config/db_connections.yaml',
                'config/db_schemas.yaml']


class PipelineConfig:
    def __init__(self, config_path: str):
        self.config_files = config_files
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.settings = self.config.get("settings", {})
        self.extractors = self._load_functions("extract")
        self.transformers = self._load_functions("transform")
        self.loaders = self._load_functions("load")
        self.joiners = self._load_functions("join")

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        additional_configs = {}
        for file in self.config_files:
            with open(file, "r") as f:
                additional_configs.update(yaml.safe_load(f))

        config = self._render_config(config, additional_configs)
        return config

    def _render_config(self, config: Dict, additional_configs: Dict) -> Dict:
        config_str = yaml.dump(config)
        template = Template(config_str)
        rendered_config_str = template.render(additional_configs)
        return yaml.safe_load(rendered_config_str)

    def _load_functions(self, key: str) -> List[Tuple]:
        functions = []
        if key in self.config:
            for item in self.config[key]:
                func_name = item["operation"]
                params = item.get("params", {})
                url = item.get("url")
                table_name = item.get("table_name")
                func = self._get_function_by_name(func_name, key)
                if func is None:
                    raise ValueError(f"Function {func_name} not found in {key} section")
                if key in ["extract", "load"]:
                    functions.append((func, url, table_name, params))
                else:
                    functions.append((func, params))
        return functions

    def _get_function_by_name(self, name: str, section: str):
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
