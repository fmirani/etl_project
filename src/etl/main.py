import os
from typing import Any, Tuple
from dotenv import load_dotenv

load_dotenv()

PATH = os.path.dirname(os.path.abspath(__file__))


class ETL:

    db_name: str = os.path.join(PATH, "data/database.db")

    service: str
    filename: str

    simulation: bool = False
    simul_days: int = 0

    conn: object
    cur: object

    def set_service(self, service: str) -> None:
        ETL.service = service

    def get_service(self) -> str:
        return ETL.service

    def set_filename(self, filename: str) -> None:
        ETL.filename = filename

    def get_filename(self) -> str:
        return ETL.filename

    def set_api(self, api_key: str) -> None:
        os.environ["API_KEY"] = api_key

    def get_api(self) -> str:
        return str(os.getenv("API_KEY"))

    def get_db(self) -> str:
        return ETL.db_name

    def set_simulation(self, simu: bool = True, days: int = 45) -> None:
        ETL.simulation = simu
        ETL.simul_days = days

    def get_sim_status(self) -> bool:
        return ETL.simulation

    def get_simul_days(self) -> int:
        return ETL.simul_days

    def set_connection(self, conn: object, cur: object) -> None:
        ETL.conn = conn
        ETL.cur = cur

    def get_connection(self) -> Tuple[Any, Any]:
        return (ETL.conn, ETL.cur)
