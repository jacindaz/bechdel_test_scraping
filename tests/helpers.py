import bechdel_test.scraper as scraper
import pytest
import sqlalchemy


@pytest.fixture()
def sqlalchemy_engine():
    return scraper._create_engine()


@pytest.fixture()
def mocked_sqlalchemy_engine(mocker):
    return mocker.patch('bechdel_test.scraper._create_engine', return_value=mocker.Mock())


@pytest.fixture()
def mocked_engine_execute(mocker):
    return mocker.patch('bechdel_test.scraper._execute')
