import time
import random
import requests
import logging
from typing import Any, Dict, Optional

class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(
        self,
        api_key: str,
        language: str = "en-US",
        request_timeout_seconds: float = 10.0,
        max_retry_attempts: int = 3,
        initial_backoff_seconds: float = 1.0,
    ):
        self.api_key = api_key
        self.language = language
        self.request_timeout = request_timeout_seconds
        self.max_retry_attempts = max_retry_attempts
        self.initial_backoff_seconds = initial_backoff_seconds
        self.http_session = requests.Session()

    def fetch_movie_details(self, movie_id: int) -> Optional[Dict[str, Any]]:
        api_endpoint_url = f"{self.BASE_URL}/movie/{movie_id}"
        request_params = {"api_key": self.api_key, "language": self.language}

        for current_attempt in range(self.max_retry_attempts + 1):
            try:
                api_response = self.http_session.get(
                    api_endpoint_url, 
                    params=request_params, 
                    timeout=self.request_timeout
                )

                if api_response.status_code == 200:
                    return api_response.json()

                if api_response.status_code == 404:
                    logging.warning(f"Movie ID {movie_id} not found (404).")
                    return None

                if api_response.status_code in {429, 500, 502, 503, 504}:
                    logging.warning(
                        f"Received status {api_response.status_code} for movie {movie_id}. "
                        f"Retrying (attempt {current_attempt + 1}/{self.max_retry_attempts + 1})..."
                    )
                    self._wait_with_exponential_backoff(current_attempt)
                    continue

                logging.error(
                    f"Failed to fetch movie {movie_id} with unrecoverable status: {api_response.status_code}"
                )
                return None

            except requests.RequestException as network_error:
                logging.error(f"Network error for movie {movie_id}: {network_error}")
                if current_attempt == self.max_retry_attempts:
                    break
                self._wait_with_exponential_backoff(current_attempt)

        logging.error(f"All retries failed for movie ID: {movie_id}")
        return None

    def _wait_with_exponential_backoff(self, attempt_number: int):
        exponential_delay = self.initial_backoff_seconds * (2 ** attempt_number)
        jitter_delay = random.uniform(0, 0.5)
        total_wait_time = exponential_delay + jitter_delay
        time.sleep(total_wait_time)