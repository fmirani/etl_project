import os
import yaml


def loadconfig() -> dict:
    '''
    Function to load the configuration from the
    filename in the function argument
    '''
    filename = "config.yaml"
    if not os.path.exists(filename):
        raise ValueError("Config file 'config.yaml' not found. ")

    with open(filename, "r") as f:
        config = yaml.safe_load(f)
    return config
