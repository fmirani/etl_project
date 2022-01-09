from imdb import IMDb
from logger import get_logger

logger = get_logger("imdb")


def get_genre(name: str) -> str:
    '''
    Function to get genres for Movies/TV Series
    using IMDbPy library
    '''
    genres = ""

    try:  # to create an instance of the IMDb class
        ia = IMDb()
    except Exception as err:
        logger.error(f"IMDb instance failed: {err}")
        return(genres)

    # Search the title
    logger.info(f"Searching name: {name}")
    movies = ia.search_movie(name)

    # Title not found, return empty string
    if len(movies) < 1:
        logger.error(f"Name: {name} didn't return any results")
        return(genres)

    # Get the ID of the first result
    id = movies[0].movieID

    # Get the IMDb.movie object from the ID
    movie = ia.get_movie(id)
    logger.info(f'Title: {movie}')

    # Get the genres of the title
    if "genres" in movie.data:
        genres = ", ".join(movie.data["genres"])

    return(genres)
