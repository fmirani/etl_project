import yaml


def loadconfig(filename: str) -> dict:
    with open(filename, "r") as f:
        config = yaml.safe_load(f)
    return config


def updateconfig(update: dict, filename: str) -> None:
    with open(filename, "w") as f:
        yaml.dump(update, f)


if __name__ == "__main__":
    filename = "config.yaml"
    config = loadconfig(filename)
