from bs4 import BeautifulSoup
import re
import pytest
import requests

from bechdel_test.scraper import process_movies,find_movie_counts,save_movie_counts,MOVIE_COUNTS_TABLE_NAME
import bechdel_test.scraper
from bechdel_test.tests.helpers import mocked_sqlalchemy_engine, mocked_engine_execute
import requests_mock
import sqlalchemy
from unittest import mock

PASSED_MOVIE_HTML = '<div class="movie"><a href="http://us.imdb.com/title/tt3907584/"><img alt="[[3]]" src="/static/pass.png" title="[There are two or more women in this movie and they talk to each other about something other than a man]"/></a><a href="/view/9036/all_the_bright_places/" id="movie-9036">All the Bright Places</a> <a href="/view/9036/all_the_bright_places/" onclick="showComments("9036"); return false;"><img alt="[1 comment(s) available]" id="comment-img-9036" src="/static/comments.png" style="height: 10px; width: 10px;" title="1 comment"/></a> </div>'
NOT_PASSED_MOVIE_HTML = '<div class="movie"><a href="http://us.imdb.com/title/tt7458762/"><img alt="[[0]]" src="/static/nopass.png" title="[Fewer than two women in this movie]"/></a> <a href="/view/8655/le_chant_du_loup/" id="movie-8655">Le chant du loup</a> <a href="/view/8655/le_chant_du_loup/" onclick="showComments("8655"); return false;"><img alt="[2 comment(s) available]" id="comment-img-8655" src="/static/comments.png" style="height: 10px; width: 10px;" title="2 comments"/></a> </div>'


@pytest.mark.parametrize("response, expected", [
    pytest.param(
        '<h3><a id="year-2020"></a>2020 <span style="font-size: 10pt; color: gray; font-weight: normal;">(18 movies)</span></h3>',
        [(2020, 18)],
        id="happy_path"
    ),
    pytest.param(
        '<h3><a id="year-1820"></a>2020 <span style="font-size: 10pt; color: gray; font-weight: normal;"></span></h3>',
        [(1820, 0)],
        id="no_movies"
    ),
    pytest.param(
        '<h3><a id="year-2020"></a>2020(18 movies)</h3>',
        [(2020, 0)],
        id="no_span_for_movies"
    ),
    pytest.param(
        '<h3><a id="not-ayear"></a>2020 <span style="font-size: 10pt; color: gray; font-weight: normal;">(27 movies)</span></h3>',
        [],
        id="year_is_str_in_id"
    ),
    pytest.param(
        '<h3><a id="hello"></a>2020 <span style="font-size: 10pt; color: gray; font-weight: normal;">(27 movies)</span></h3>',
        [],
        id="no_year_in_id"
    ),
    pytest.param(
        '<h3><a></a>2020 <span style="font-size: 10pt; color: gray; font-weight: normal;">(27 movies)</span></h3>',
        [],
        id="no_id"
    )
])
def test_find_movie_counts(response, expected):
    with requests_mock.Mocker() as m:
        m.get("https://bechdeltest.com/?list=all", text=response)

        movies_per_year = find_movie_counts()
        assert movies_per_year == expected


@pytest.mark.parametrize("old_movie_count,cur_year,new_movie_counts,exp_metacommand,exp_sql1,exp_sql2", [
    pytest.param(
        [[0]],
        2020,
        [(2020, 123)],
        "update",
        "set count = 123",
        "where year = 2020",
        id="update_new_count"
    ),
    pytest.param(
        [],
        1998,
        [(1998, 567)],
        "insert",
        "(year, count)",
        "( 1998, 567 )",
        id="insert_new_count"
    ),
])
def test_save_movie_counts( \
    mocked_sqlalchemy_engine,mocked_engine_execute,mocker, \
    old_movie_count,cur_year,new_movie_counts, \
    exp_metacommand,exp_sql1,exp_sql2 \
):
    """
    Tests save_movie_counts
      > mocks SqlAlchemy engine
      > mocks engine.execute

    SQL checks:
      > insert happens for a year I do not have
      > update happens for a count different from newly
        scraped count, for a given year
    """
    mocker.patch('bechdel_test.scraper.find_year_counts_in_db', return_value=old_movie_count)
    save_movie_counts(MOVIE_COUNTS_TABLE_NAME, new_movie_counts)

    # extracting input arguments
    arguments,_ = mocked_engine_execute.call_args_list[0]
    mocked_engine,sql_input = arguments
    sql_input = " ".join([x.rstrip() for x in sql_input.split(" ") if x]).strip().lower()

    # assert an update was done
    assert re.match(exp_metacommand, sql_input)
    assert exp_sql1 in sql_input
    assert exp_sql2 in sql_input

    # assert a call to the database was made
    mocked_engine_execute.assert_called()


def test_save_movie_counts_no_change(mocked_sqlalchemy_engine, mocked_engine_execute, mocker):
    """
    Tests save_movie_counts
      > mocks SqlAlchemy engine
      > mocks engine.execute

    SQL checks:
      > nothing happens if year + count are the same as newly
        scraped year + count
    """

    # mock find_year_counts_in_db
    old_movie_and_new_movie_count = 123
    cur_year = 2020
    mocker.patch('bechdel_test.scraper.find_year_counts_in_db', return_value=[[old_movie_and_new_movie_count]])

    new_movie_counts = [(cur_year, old_movie_and_new_movie_count)]
    save_movie_counts(MOVIE_COUNTS_TABLE_NAME, new_movie_counts)

    # assert a call to the database was NOT made
    mocked_engine_execute.assert_not_called()


@pytest.mark.parametrize("movie_html,expected_keys,expected_pass_value",
    [
        pytest.param(
            PASSED_MOVIE_HTML,
            ['bechdel_id', 'bechdel_url', 'imdb_url', 'pass', 'title'],
            True,
            id="passed_movie"
        ),
        pytest.param(
            NOT_PASSED_MOVIE_HTML,
            ['bechdel_id', 'bechdel_url', 'imdb_url', 'pass', 'title'],
            False,
            id="not_passed_movie"
        ),
])
def test_process_movies(movie_html, expected_keys, expected_pass_value):
    soup = BeautifulSoup(movie_html, 'html.parser')
    processed_movies = process_movies([soup])

    processed_movie = processed_movies[0]

    assert sorted(list(processed_movie.keys())) == expected_keys
    for val in list(processed_movie.values()):
        assert val is not None
    assert int(processed_movie["bechdel_id"])
    assert processed_movie["pass"] == expected_pass_value
    assert re.search('bechdeltest.com', processed_movie["bechdel_url"])
    assert re.search('imdb.com', processed_movie["imdb_url"])


def test_scrape_movies_by_year():
    """
    random thoughts:
      > stub: requests.get(bechdel url per year)
      > return: ^ 2 years of data,
        assert new_movies contains both years
      >
    """
    pass
