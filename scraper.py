import datetime
import re
import requests
from bs4 import BeautifulSoup

from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, Integer
from sqlalchemy.sql import func

"""
NEXT:
 > create metadata table:
   has counts of movies per year
   when last scraped (?) (not sure if need)
 > get movie counts
 > get movies by year:
    only look at movies (by ID) that we do not have
    (cannot update movies, so only need to worry about new movies)
"""

def database_setup(db_name="bechdel"):
    """
    Later: save movie data to movies table (?)
    TODO: how to set utc timezone default for date_created/date_modified?
    """
    engine = create_engine("postgresql+psycopg2://jacinda@localhost:5432/bechdel")
    meta = MetaData(engine)
    table = Table("movie_year_counts", meta,
                   Column('id', Integer, primary_key=True),
                   Column('year', Integer, nullable=False),
                   Column('count', Integer, nullable=False),
                   Column('date_created', DateTime(timezone=True), server_default=func.now()),
                   Column('date_modified', DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
                  )
    meta.create_all()


def process_movies(all_movies):
    processed_movies = []
    for movie in all_movies:
        new_movie = {
            "bechdel_id": "",
            "bechdel_url": "",
            "title": "",
            "imdb_url": "",
            "pass": "",
        }
        all_urls = movie.find_all('a')

        for url in all_urls:
            url_href = url.get('href')

            if re.search('imdb', url_href):
                new_movie["imdb_url"] = url_href

                if re.search('/nopass.png', url.find('img').get('src')):
                    new_movie["pass"] = False
                else:
                    new_movie["pass"] = True
            elif re.search('view', url_href):
                new_movie["bechdel_url"] = "https://bechdeltest.com" + url_href

                if url.text:
                    new_movie["title"] = url.text
                if url.get("id"):
                    new_movie["bechdel_id"] = url.get("id").split("-")[1]
        processed_movies.append(new_movie)
    return processed_movies


def scrape(bechdel_test_url):
    html = requests.get(bechdel_test_url).text
    soup = BeautifulSoup(html, 'html.parser')

    all_movies = soup.find_all('div', attrs='movie')
    return process_movies(all_movies)


homepage = "https://bechdeltest.com/"
all_movies_url = "https://bechdeltest.com/?list=all"
page1 = "https://bechdeltest.com/?page=1"
page42 = "https://bechdeltest.com/?page=42"

# print(scrape("https://bechdeltest.com/?page=19"))
database_setup()
