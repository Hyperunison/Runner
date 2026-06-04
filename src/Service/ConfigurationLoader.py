import os
import yaml
import re
import argparse

env_var_pattern = re.compile(r'ENV\("([^"]+)"\s*(?:,\s*"([^"]+)")?\)')
arg_var_pattern = re.compile(r'ARG\("([^"]+)"\s*(?:,\s*"([^"]+)")?\)')

cli_args = {}
parser = argparse.ArgumentParser(description="Update YAML config with CLI args.")
args, unknown = parser.parse_known_args()
for arg in unknown:
    if arg.startswith("--"):
        key, value = arg[2:].split("=")
        cli_args[key] = value

def env_constructor(loader, node):
    value = loader.construct_scalar(node)
    match = env_var_pattern.match(value)
    if match:
        env_var_name = match.group(1)
        default = match.group(2) if match.group(2) else ''
        return os.getenv(env_var_name, default)
    return value


def arg_constructor(loader, node):
    value = loader.construct_scalar(node)
    match = arg_var_pattern.match(value)
    if match:
        arg_var_name = match.group(1)
        default = match.group(2) if match.group(2) else ''
        result = cli_args.get(arg_var_name, default)
        return result
    return value


# Adding new constructors to the yaml loader
yaml.add_implicit_resolver("!env", env_var_pattern)
yaml.add_constructor("!env", env_constructor)

yaml.add_implicit_resolver("!arg", arg_var_pattern)
yaml.add_constructor("!arg", arg_constructor)


def load_yaml_with_custom_vars(filepath):
    with open(filepath, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class ConfigurationLoader:
    config: any

    def __init__(self, filename: str):
        config_dir = os.path.dirname(os.path.abspath(filename))
        defaults_path = os.path.join(config_dir, 'config.default.yaml')

        if os.path.exists(defaults_path):
            defaults = load_yaml_with_custom_vars(defaults_path)
        else:
            defaults = {}

        self.config = deep_merge(defaults, load_yaml_with_custom_vars(filename))

    def get_config(self):
        return self.config
