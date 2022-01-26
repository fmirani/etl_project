import os
from typing import Tuple
from etl.logger import get_logger


logger = get_logger("extract")


def extract_data(service: str, data_file: str) -> Tuple[str, str]:
    """
    Function to orchestrate the operation
    1. fetch data from latest files
    2. update the database
    3. return number of items added
    """
    logger.info(f"Extracting data for {service} from {data_file}..")

    if not os.path.exists(data_file):
        logger.error(f"'{service}' data file not found")
        return ("", "")

    if service not in ["youtube", "netflix"]:
        logger.error(
            f"Incorrect service name. Expecting 'youtube' or 'netflix', provided {service}"
        )
        return ("", "")

    return (service, data_file)
