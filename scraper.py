import datetime
import logging
import os
import re
import requests
from bs4 import BeautifulSoup,SoupStrainer

from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, Integer
from sqlalchemy.sql import func


DB_URI = "postgresql+psycopg2://jacinda@localhost:5432/bechdel"
MOVIE_COUNTS_TABLE_NAME = "movie_year_counts"

LOGGER = logging.getLogger("bechdel_scraping")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def database_setup(db_uri, table_name=MOVIE_COUNTS_TABLE_NAME):
    """
    TODO: how to set utc timezone default for date_created/date_modified?
    """
    engine = create_engine(db_uri)
    meta = MetaData(engine)
    table = Table(table_name, meta,
                   Column('id', Integer, primary_key=True),
                   Column('year', Integer, nullable=False),
                   Column('count', Integer, nullable=False),
                   Column('date_created', DateTime(timezone=True), server_default=func.now()),
                   Column('date_modified', DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
                  )
    meta.create_all()
    LOGGER.info(f"Created table: {table_name}")


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


def _create_engine():
    return create_engine(DB_URI)


def _execute_sql(engine, sql):
    return engine.execute(sql)


def find_year_counts_in_db(engine):
    does_year_exist = f"""
    select count from {table} where year = {scraped_year}
    """
    return _execute_sql(engine, does_year_exist).fetchall()


def save_movie_counts(table, year_counts):
    """
    Given a dictonary: { 2020: 1234 }
      > save this to MOVIE_COUNTS_TABLE_NAME
      > insert: when year doesn't exist
      > update: when count in db is different from scraped_count
      > no action: year and count are the same as scraped

    Output: list of years
      > where the count is different in the db
        than the scraped_count
      > or the year is new, and didn't exist in the db
    """
    LOGGER.info(f"Grabbed year_counts from bechdel website, from: {min(year_counts)}, to: {max(year_counts)}")
    engine = _create_engine()

    updated_rows = 0
    inserted_rows = 0
    no_change_rows = 0

    different_counts_or_new_years = []
    for scraped_year,scraped_count in year_counts:
        counts_row = find_year_counts_in_db(engine)
        if counts_row:
            count_in_db = counts_row[0][0]

            # Update
            if count_in_db != scraped_count:
                print(f"count_in_db: {count_in_db}, scraped_count: {scraped_count}")
                update_count = f"""
                UPDATE {table} SET count = {scraped_count}
                WHERE year = {scraped_year};
                """
                _execute_sql(engine, update_count)
                LOGGER.info(f"Updated count for year: {scraped_year}, new count: {scraped_count}")

                updated_rows += 1
                different_counts_or_new_years.append(scraped_year)

            # No change
            else:
                no_change_rows += 1

        else: # Insert
            insert = f"""
            INSERT INTO {table}
            (year, count)
            VALUES (
            {scraped_year}, {scraped_count}
            )
            """
            _execute_sql(engine, insert)

            inserted_rows += 1
            different_counts_or_new_years.append(scraped_year)

    LOGGER.info(f"Inserted {inserted_rows}, Updated {updated_rows}, No change {no_change_rows}")
    return different_counts_or_new_years


def scrape_and_save_movie_counts():
    database_setup(DB_URI)
    year_counts = find_movie_counts()
    years_to_scrape = save_movie_counts(DB_URI, MOVIE_COUNTS_TABLE_NAME, year_counts)
    return years_to_scrape


def process_movies(all_movies):
    """
    Input: all_movies is a list of <div> tags
    Output: return list of dictionaries with movie data
    """
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


def scrape_movies_by_year():
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
    years_to_scrape = scrape_and_save_movie_counts()
    if years_to_scrape == []: LOGGER.info("No movies to scrape, all up-to-date.")

    new_movies = []
    for year in years_to_scrape:
        bechdel_url = f"http://bechdeltest.com/year/{year}"
        html = requests.get(bechdel_url).text
        movie_soup = BeautifulSoup(html, "html.parser", parse_only=SoupStrainer("div", attrs="movie"))
        all_movies = movie_soup.find_all("div", recursive=False)
        new_movies += process_movies(all_movies)
    return new_movies


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


homepage = "https://bechdeltest.com/"
all_movies_url = "https://bechdeltest.com/?list=all"
page1 = "https://bechdeltest.com/?page=1"
page42 = "https://bechdeltest.com/?page=42"

# print(scrape_movies_by_year())
