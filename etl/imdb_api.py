from imdb import IMDb
from etl.logger import get_logger

logger = get_logger("imdb")


def get_genre(name: str) -> str:
    '''
    Function to get genres for Movies/TV Series
    using IMDbPy library
    '''
    genres = ""

    try:  # to create an instance of the IMDb class
        logger.debug("Creating a new IMDB instance")
        ia = IMDb()
        logger.debug("A new IMDB instance created")
    except Exception as err:
        logger.debug(f"IMDb instance failed: {err}")
        return(genres)

    # Search the title
    logger.debug(f"Searching name: {name}")
    movies = ia.search_movie(name)
    logger.debug("Search ended")
    # Title not found, return empty string
    if len(movies) < 1:
        logger.warning(f"Name: {name} didn't return any results")
        return(genres)

    # Get the ID of the first result
    id = movies[0].movieID

    # Get the IMDb.movie object from the ID
    logger.debug("Getting Title using ID")
    movie = ia.get_movie(id)
    logger.debug(f'Title found: {movie}')

    # Get the genres of the title
    if "genres" in movie.data:
        genres = ", ".join(movie.data["genres"])

    return(genres)
