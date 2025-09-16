import requests
from src.tmdb_client import TMDBClient

def test_fetch_movie_details_success(mocker):
    """Tests a successful API call."""
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 634649, "title": "Spider-Man: No Way Home"}
    get_mock = mocker.patch("requests.Session.get", return_value=mock_response)

    client = TMDBClient(api_key="fake_key")
    data = client.fetch_movie_details(634649)

    assert data is not None
    assert data["title"] == "Spider-Man: No Way Home"
    # Assert request details
    assert get_mock.call_count == 1
    args, kwargs = get_mock.call_args
    assert args[0] == "https://api.themoviedb.org/3/movie/634649"
    assert kwargs["params"] == {"api_key": "fake_key", "language": "en-US"}
    assert kwargs["timeout"] == 10.0

def test_fetch_movie_details_not_found(mocker):
    """Tests the case where a movie is not found (404)."""
    mock_response = mocker.Mock()
    mock_response.status_code = 404
    get_mock = mocker.patch("requests.Session.get", return_value=mock_response)

    client = TMDBClient(api_key="fake_key")
    data = client.fetch_movie_details(9999)

    assert data is None
    assert get_mock.call_count == 1
    args, kwargs = get_mock.call_args
    assert args[0] == "https://api.themoviedb.org/3/movie/9999"
    assert kwargs["params"] == {"api_key": "fake_key", "language": "en-US"}
    assert kwargs["timeout"] == 10.0

def test_fetch_movie_details_retry_on_server_error(mocker):
    """Tests that the client retries on a 500 server error."""
    mock_success = mocker.Mock()
    mock_success.status_code = 200
    mock_success.json.return_value = {"id": 550, "title": "Fight Club"}

    mock_failure = mocker.Mock()
    mock_failure.status_code = 500

    # Simulate one failure, then a success
    get_mock = mocker.patch("requests.Session.get", side_effect=[mock_failure, mock_success])
    mocker.patch("time.sleep")  # Avoid actual sleeping during tests

    client = TMDBClient(api_key="fake_key", max_retry_attempts=1)
    data = client.fetch_movie_details(550)

    assert data is not None
    assert data["title"] == "Fight Club"
    assert get_mock.call_count == 2
    # Check last call details
    args, kwargs = get_mock.call_args
    assert args[0] == "https://api.themoviedb.org/3/movie/550"
    assert kwargs["params"] == {"api_key": "fake_key", "language": "en-US"}
    assert kwargs["timeout"] == 10.0


def test_fetch_movie_details_exhausts_retries(mocker):
    mock_failure = mocker.Mock()
    mock_failure.status_code = 500
    get_mock = mocker.patch("requests.Session.get", return_value=mock_failure)
    mocker.patch("time.sleep")

    client = TMDBClient(api_key="fake_key", max_retry_attempts=2)
    data = client.fetch_movie_details(550)

    assert data is None
    # 1 initial attempt + 2 retries
    assert get_mock.call_count == 3
    args, kwargs = get_mock.call_args
    assert args[0] == "https://api.themoviedb.org/3/movie/550"
    assert kwargs["params"] == {"api_key": "fake_key", "language": "en-US"}
    assert kwargs["timeout"] == 10.0


def test_fetch_movie_details_handles_network_exception_then_success(mocker):
    mock_success = mocker.Mock()
    mock_success.status_code = 200
    mock_success.json.return_value = {"id": 550, "title": "Fight Club"}
    get_mock = mocker.patch("requests.Session.get", side_effect=[requests.RequestException("boom"), mock_success])
    mocker.patch("time.sleep")

    client = TMDBClient(api_key="fake_key")
    data = client.fetch_movie_details(550)

    assert data is not None
    assert data["title"] == "Fight Club"
    assert get_mock.call_count == 2
    args, kwargs = get_mock.call_args
    assert args[0] == "https://api.themoviedb.org/3/movie/550"
    assert kwargs["params"] == {"api_key": "fake_key", "language": "en-US"}
    assert kwargs["timeout"] == 10.0