from etl import ETL


def new_main():

    instance2 = ETL()

    instance2.set_service("netflix")
    instance2.set_filename("netflix.csv")

    print(instance2.get_api())
    print(instance2.get_service())
    print(instance2.get_filename())
