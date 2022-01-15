import os
import yaml


def loadconfig() -> dict:
    '''
    Function to load the configuration from the
    filename in the function argument
    '''
    path = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(path, "config/config.yaml")

    if not os.path.exists(filename):
        raise OSError("Config file 'config.yaml' not found.")

    with open(filename, "r") as f:
        config = yaml.safe_load(f)

    return(config)
