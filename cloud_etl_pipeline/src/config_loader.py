import yaml
import os

def load_config(path="config.yaml"):
    base_path = os.path.dirname(os.path.dirname(__file__))
    full_path = os.path.join(base_path, path)

    with open(full_path, "r") as f:
        return yaml.safe_load(f)