import yaml
from pathlib import Path

_CONFIG_PATH = Path(__file__).parent / "settings.yaml"

def load_config() -> dict:
    with open(_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)
