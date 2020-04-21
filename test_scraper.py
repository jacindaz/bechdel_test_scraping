from bs4 import BeautifulSoup
import re
from scraper import process_movies

import pytest

PASSED_MOVIE_HTML = '<div class="movie"><a href="http://us.imdb.com/title/tt3907584/"><img alt="[[3]]" src="/static/pass.png" title="[There are two or more women in this movie and they talk to each other about something other than a man]"/></a><a href="/view/9036/all_the_bright_places/" id="movie-9036">All the Bright Places</a> <a href="/view/9036/all_the_bright_places/" onclick="showComments("9036"); return false;"><img alt="[1 comment(s) available]" id="comment-img-9036" src="/static/comments.png" style="height: 10px; width: 10px;" title="1 comment"/></a> </div>'
NOT_PASSED_MOVIE_HTML = '<div class="movie"><a href="http://us.imdb.com/title/tt7458762/"><img alt="[[0]]" src="/static/nopass.png" title="[Fewer than two women in this movie]"/></a> <a href="/view/8655/le_chant_du_loup/" id="movie-8655">Le chant du loup</a> <a href="/view/8655/le_chant_du_loup/" onclick="showComments("8655"); return false;"><img alt="[2 comment(s) available]" id="comment-img-8655" src="/static/comments.png" style="height: 10px; width: 10px;" title="2 comments"/></a> </div>'


@pytest.mark.parametrize("movie_html,expected_keys",
    [
        pytest.param(
            PASSED_MOVIE_HTML,
            ['bechdel_id', 'bechdel_url', 'imdb_url', 'pass', 'title'],
            id="passed_movie"
        ),
        pytest.param(
            NOT_PASSED_MOVIE_HTML,
            ['bechdel_id', 'bechdel_url', 'imdb_url', 'pass', 'title'],
            id="not_passed_movie"
        ),

])
def test_data_elements_populated(movie_html, expected_keys):
    soup = BeautifulSoup(movie_html, 'html.parser')
    processed_movies = process_movies([soup])

    processed_movie = processed_movies[0]

    assert sorted(list(processed_movie.keys())) == expected_keys
    for val in list(processed_movie.values()):
        assert val is not None
    assert int(processed_movie["bechdel_id"])
    assert type(processed_movie["pass"]) == bool
    assert re.search('.com', processed_movie["bechdel_url"])
    assert re.search('.com', processed_movie["imdb_url"])


@pytest.mark.parametrize("movie_html,expected",
    [
        pytest.param(
            PASSED_MOVIE_HTML,
            True,
            id="passed_movie"
        ),
        pytest.param(
            NOT_PASSED_MOVIE_HTML,
            False,
            id="not_passed_movie"
        ),

])
def test_passed_movie(movie_html, expected):
    soup = BeautifulSoup(movie_html, 'html.parser')
    processed_movies = process_movies([soup])

    processed_movie = processed_movies[0]
    assert processed_movie["pass"] == expected
