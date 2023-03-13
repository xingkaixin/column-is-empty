from pathlib import Path

import yaml
from pydantic import BaseModel


class APIConfig(BaseModel):
    base_url: str


class Config:
    def __init__(self, api_config: APIConfig):
        self.api = api_config

    @classmethod
    def parse_file(cls, file_path: str):
        with open(file_path, "r", encoding="utf-8") as f:
            config_yaml = yaml.safe_load(f)
        api_config = APIConfig.parse_obj(config_yaml["api"])
        return cls(api_config)


config_file = Path("config.yaml")
config = Config.parse_file(config_file)
api_config = config.api
