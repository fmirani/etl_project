import yaml


def loadconfig(filename: str) -> dict:
    '''
    Function to load the configuration from the
    filename in the function argument
    '''
    with open(filename, "r") as f:
        config = yaml.safe_load(f)
    return config
