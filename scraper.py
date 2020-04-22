import datetime
import logging
import os
import re
import requests
from bs4 import BeautifulSoup,SoupStrainer

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
DB_URI = "postgresql+psycopg2://jacinda@localhost:5432/bechdel"
MOVIE_COUNTS_TABLE_NAME = "movie_year_counts"

LOGGER = logging.getLogger("bechdel_scraping")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def database_setup(db_uri, db_name="bechdel"):
    """
    Later: save movie data to movies table (?)
    TODO: how to set utc timezone default for date_created/date_modified?
    """
    engine = create_engine(db_uri)
    meta = MetaData(engine)
    table = Table(MOVIE_COUNTS_TABLE_NAME, meta,
                   Column('id', Integer, primary_key=True),
                   Column('year', Integer, nullable=False),
                   Column('count', Integer, nullable=False),
                   Column('date_created', DateTime(timezone=True), server_default=func.now()),
                   Column('date_modified', DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
                  )
    meta.create_all()
    LOGGER.info(f"Created table: {MOVIE_COUNTS_TABLE_NAME}")


def find_movie_counts(bechdel_url="https://bechdeltest.com/?list=all"):
    """
    Scrape https://bechdeltest.com/?list=all
     > retreive counts for # movies per year
     > handle edge cases:
         if year doesn't exist
         if movie count doesn't exist

    For future: consider loading counts per page
     > about 8,000 movies total
       per movie: 1 div + 2 <a> tags
     > years span 1888 - 2020 (132 years)
    """
    movies_per_year = []
    html = requests.get(bechdel_url).text
    soup = BeautifulSoup(html, "html.parser", parse_only=SoupStrainer("h3"))
    LOGGER.info("Parsed h3 elements for movie counts per year")

    for year_count in soup.find_all(True, recursive=False):
        if year_count.find("a") and year_count.find("a").get("id"):
            id_text = year_count.find("a").get("id").split("-")
            if len(id_text) == 2 and id_text[1].isdigit():
                year = int(year_count.find("a").get("id").split("-")[1])

                if year_count.find("span") and year_count.find("span").text:
                    num_movies = int(year_count.find("span").text.split(" ")[0][1:])
                else:
                    num_movies = 0

                movies_per_year.append((year, num_movies))
            else:
                maybe_year = year_count.find("a").get("id")
                LOGGER.warning(f"Could not find movie count for year {maybe_year}, html: {id_text}")
        else:
            LOGGER.warning(f"Could not find year in <a> tag: {year_count}")

    LOGGER.info("Completed scraping movie counts per year.")
    return movies_per_year


def save_movie_counts(db_uri, table, year_counts):
    """
    Given a dictonary: { 2020: 1234 }
      > save this to MOVIE_COUNTS_TABLE_NAME
    """
    LOGGER.info(f"Grabbed year_counts from bechdel website, from: {min(year_counts)}, to: {max(year_counts)}")
    engine = create_engine(db_uri)

    updated_rows = 0
    inserted_rows = 0
    no_change_rows = 0
    for scraped_year,scraped_count in year_counts:
        # if year already exists, update the value
        does_year_exist = f"""
        select count from {table} where year = {scraped_year}
        """
        count_row = engine.execute(does_year_exist).fetchall()

        if count_row:
            count_in_db = count_row[0][0]
            if count_in_db != scraped_count:
                update_count = f"""
                UPDATE {table} SET count = {scraped_count}
                WHERE year = {scraped_year};
                """
                engine.execute(update_count)
                LOGGER.info(f"Updated count for year: {scraped_year}, new count: {scraped_count}")
                updated_rows += 1
            else:
                no_change_rows += 1

        else:
            insert = f"""
            INSERT INTO {table}
            (year, count)
            VALUES (
            {scraped_year}, {scraped_count}
            )
            """
            engine.execute(insert)
            inserted_rows += 1

    LOGGER.info(f"Inserted {inserted_rows}, Updated {updated_rows}, No change {no_change_rows}")


def scrape_and_save_movie_counts():
    database_setup(DB_URI)
    year_counts = find_movie_counts()
    save_movie_counts(DB_URI, MOVIE_COUNTS_TABLE_NAME, year_counts)

def years_to_scrape(bechdel_movie_counts, db_uri):
    """
    Query movie_year_counts and compare with bechdel website
    Input: current counts per year from Bechdel website

    Return: list of years where counts < bechdel website
      > therefore, for those years, there are new movie entries
    """

    # Query my table:
    #   > only need year + count
    #   > want it to be dictionary[year] = count
    # Iterate over bechdel_movie_counts
    #   > if bechdel_count == my_counts[year], then continue
    #   > else: res.append(year)
    pass


def insert_movies(movies):
    """
    FIRST: see how long it takes to insert 1 row
    at a time for 8k records
    (use psycopg NOT sqlalchemy)

    Instead of a dictionary, change process_movies
      > to create a CSV file if > 10,000 records
      > use postgres COPY to bulk insert
      > delete CSV file
    """
    # start with batch of 1000 for inserts
    pass


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


def scrape(bechdel_url):
    """
    Parse the list of all movies

    Parsing operations:
     > Parse only <div> tags
     > Filter down to elements with "movie" attr
     > recursive=False: only look at the first
       set of <div> tags found - don't continue searching
       the tree for children <div> tags
       (none exist since movie <div> tags do not have nested
        <div> tags, but cuts down on the tree traversal)
    """
    html = requests.get(bechdel_url).text
    movie_soup = BeautifulSoup(html, "html.parser", parse_only=SoupStrainer("div"))
    all_movies = movie_soup.find_all(attrs="movie", recursive=False)
    LOGGER.info("Parsed and filtered down to div tags with attr movie.")

    return process_movies(all_movies)


homepage = "https://bechdeltest.com/"
all_movies_url = "https://bechdeltest.com/?list=all"
page1 = "https://bechdeltest.com/?page=1"
page42 = "https://bechdeltest.com/?page=42"

# scrape_and_save_movie_counts()
