import os


PATH = os.path.dirname(os.path.abspath(__file__))
# os.environ["API_KEY"] = "API_KEY_GOES_INSIDE_THESE_QUOTES"


class ETL:
    api_key: str = ""
    db_name: str = os.path.join(PATH, "data/database.db")

    service: str = ""
    filename: str = ""

    simulation: bool = False
    simul_days: int = 0

    def set_service(self, service: str) -> None:
        ETL.service = service

    def get_service(self) -> str:
        return(ETL.service)

    def set_filename(self, filename: str) -> None:
        ETL.filename = filename

    def get_filename(self) -> str:
        return(ETL.filename)

    def set_api(self, api_key: str) -> None:
        ETL.api_key = api_key

    def get_api(self) -> str:
        return(ETL.api_key)

    def get_db(self) -> str:
        return(ETL.db_name)

    def set_simulation(self, simu: bool=True, days: int=45) -> None:
        ETL.simulation = simu
        ETL.simul_days = days

    def get_sim_status(self) -> bool:
        return(ETL.simulation)

    def get_simul_days(self) -> int:
        return(ETL.simul_days)
