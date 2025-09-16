from src.movie_report_pipeline import make_movie_row

def test_make_movie_row_happy_path():
    """Tests transformation with a complete, valid API response."""
    api_data = {
        "id": 550,
        "title": "Fight Club",
        "vote_average": 8.4,
        "genres": [{"id": 18, "name": "Drama"}, {"id": 53, "name": "Thriller"}],
    }
    movie_row = make_movie_row(550, api_data)
    assert movie_row.id == 550
    assert movie_row.title == "Fight Club"
    assert movie_row.vote_average == 8.4
    assert movie_row.genres == ["Drama", "Thriller"]
    assert not movie_row.is_action

def test_make_movie_row_handles_action_genre():
    """Ensures the 'is_action' flag is correctly set."""
    api_data = {
        "id": 299536,
        "title": "Avengers: Infinity War",
        "vote_average": 8.2,
        "genres": [{"id": 12, "name": "Adventure"}, {"id": 28, "name": "Action"}],
    }
    movie_row = make_movie_row(299536, api_data)
    assert movie_row.is_action

def test_make_movie_row_handles_missing_data():
    """Tests robustness when the API response is missing fields."""
    api_data = {"id": 999, "title": "Movie with Missing Info"}
    movie_row = make_movie_row(999, api_data)
    assert movie_row.id == 999
    assert movie_row.title == "Movie with Missing Info"
    assert movie_row.vote_average is None
    assert movie_row.genres == []
    assert not movie_row.is_action

def test_make_movie_row_handles_not_found():
    """Tests the case where a movie ID returns no data from the API."""
    movie_row = make_movie_row(404, None)
    assert movie_row.id == 404
    assert movie_row.title == ""
    assert movie_row.vote_average is None
    assert movie_row.genres == []
    assert not movie_row.is_action


def test_make_movie_row_spiderman_real_example():
    """Uses the real Spider-Man: No Way Home payload fields to verify extraction."""
    api_data = {
        "id": 634649,
        "title": "Spider-Man: No Way Home",
        "vote_average": 7.94,
        "genres": [
            {"id": 28, "name": "Action"},
            {"id": 12, "name": "Adventure"},
            {"id": 878, "name": "Science Fiction"},
        ],
    }

    movie_row = make_movie_row(634649, api_data)

    assert movie_row.id == 634649
    assert movie_row.title == "Spider-Man: No Way Home"
    assert movie_row.vote_average == 7.94
    # Genres should be alphabetically sorted by implementation
    assert movie_row.genres == ["Action", "Adventure", "Science Fiction"]
    assert movie_row.is_action is True