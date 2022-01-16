import os
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.config import loadconfig
from etl.logger import get_logger

logger = get_logger("main")


def set_environment():
    '''
    Function to set up environment
    '''
    conf = loadconfig()

    # User data path
    data_path = os.path.expanduser(conf["global"]["data_path"])
    # Path to source files
    curr_path = os.path.dirname(os.path.abspath(__file__))

    # If user data path does not exist, create the structure
    if not os.path.exists(data_path):
        logger.info("etl/ datapath missing, setting up new directory")
        os.mkdir(data_path)
        os.mkdir(os.path.join(data_path, "config/"))
        os.mkdir(os.path.join(data_path, "data/"))
        os.mkdir(os.path.join(data_path, "data/logs/"))
        logger.info("User datapath structure created")
    else:
        logger.info("User datapath already exists")

    # If the config file is not in the usr data path, create a symlink
    if not os.path.exists(os.path.join(data_path, "config/config.yaml")):
        logger.info("Configuration file missing, setting one up")

        # Creating symbolic link
        os.symlink(os.path.join(curr_path, "config/config.yaml"),
                   os.path.join(data_path, "config/config.yaml"))
        logger.info("Configuration file link setup success")
    else:
        logger.info("Configuration file already exists")

    # If the log file is not in the usr data path, create a symlink
    if not os.path.exists(os.path.join(data_path, "data/logs/etl.log")):
        logger.info("Log file missing, setting one up")

        # Creating symbolic link
        os.symlink(os.path.join(curr_path, "data/logs/etl.log"),
                   os.path.join(data_path, "data/logs/etl.log"))
        logger.info("Log file link setup success")
    else:
        logger.info("Log file already exists")


def full_run():
    '''
    Function to execute the full ETL cycle
    '''

    logger.info("Going to check (and set) the environment first")
    set_environment()

    conf = loadconfig()

    data_path = os.path.expanduser(conf["global"]["data_path"])

    logger.info("Start of program.")

    services = ["youtube", "netflix"]
    files = [os.path.join(data_path, conf["youtube"]["history_file"]),
             os.path.join(data_path, conf["netflix"]["history_file"])]

    for i, service in enumerate(services):

        logger.info(f"Going to extract {service} data")
        s, f = extract_data(service, files[i])

        if len(s) < 1 or len(f) < 1:
            logger.error(f"Could not locate file for {service}")
            logger.error(
                f"Please copy the data file for {service} and confirm")
            logger.error("its name is in the ")
            logger.error(f"{data_path}config/config.yaml file")
            continue

        logger.info(f"Going to transform {service} data")
        data = transform_data(s, f)

        if data.shape[0] < 1:
            logger.error(f"Incompatible file format or empty data file.")
            continue

        logger.info(f"Going to load {service} data")
        added = load_data(data)

        logger.info(f"{added} items updated for '{service}' in the table")


if __name__ == "__main__":
    full_run()
