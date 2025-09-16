import os
import sys
import logging
import csv
import time
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.report_io import read_movie_ids_with_skips, write_excel, write_failed_ids
from src.tmdb_client import TMDBClient
from src.schemas import MovieRow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


class Config:
    def __init__(self):
        load_dotenv()
        self.api_key = os.environ.get("TMDB_API_KEY")
        if not self.api_key:
            logging.error("TMDB_API_KEY not set in environment.")
            raise ValueError("Missing TMDB_API_KEY")

        self.input_csv = os.path.join(os.getcwd(), "movies.csv")
        # Dead letter queue for failed movie IDs
        self.failed_ids_csv = os.path.join(os.getcwd(), "dead_letter_queue.csv")   
        self.output_excel = os.path.join(os.getcwd(), "movie_data.xlsx")




def make_movie_row(movie_id: int, api_data: Optional[Dict[str, Any]]) -> MovieRow:
    if not api_data:
        return MovieRow(id=movie_id, title="", vote_average=None, genres=[], is_action=False)

    title = str(api_data.get("title", ""))

    vote_average = api_data.get("vote_average")
    try:
        vote_num = float(vote_average) if vote_average is not None else None
    except (ValueError, TypeError):
        vote_num = None

    raw_genres = api_data.get("genres", [])
    genre_names = [
        str(g["name"]).strip()
        for g in raw_genres
        if isinstance(g, dict) and g.get("name")
    ]

    # alphabetical order
    genre_names.sort()

    is_action = "Action" in genre_names

    return MovieRow(
        id=movie_id,
        title=title,
        vote_average=vote_num,
        genres=genre_names,
        is_action=is_action,
    )

def get_movie_data(client: TMDBClient, movie_id: int) -> Tuple[Optional[MovieRow], int]:
    api_data = client.fetch_movie_details(movie_id)
    if not api_data:
        logging.warning(f"No data found for movie ID: {movie_id}. Marking as failed.")
        return None, movie_id

    movie_row = make_movie_row(movie_id, api_data)
    return movie_row, movie_id

def main() -> None:
    try:
        config = Config()
        client = TMDBClient(api_key=config.api_key)
    except ValueError:
        sys.exit(1)

    logging.info(f"Reading movie IDs from {config.input_csv}")
    movie_ids, skipped_rows = read_movie_ids_with_skips(config.input_csv)
    logging.info(f"Found {len(movie_ids)} unique movie IDs to process. Skipped {len(skipped_rows)} rows during parsing.")
    if not movie_ids:
        logging.error("No valid movie IDs found. Exiting.")
        return

    movie_rows: List[MovieRow] = []
    failed_ids: List[int] = []
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_id = {executor.submit(get_movie_data, client, mid): mid for mid in movie_ids}
        
        for future in tqdm(as_completed(future_to_id), total=len(movie_ids), desc="Fetching movie data"):
            movie_row, movie_id = future.result()
            if movie_row:
                movie_rows.append(movie_row)
            else:
                failed_ids.append(movie_id)

    logging.info(f"Writing {len(movie_rows)} rows to {config.output_excel}")
    if movie_rows:
      movie_rows.sort(key=lambda row: row.id)
      write_excel(movie_rows, config.output_excel)

    total_failed_like = len(failed_ids) + len(skipped_rows)
    if total_failed_like:
        logging.warning(
            f"Writing {total_failed_like} failed/skipped rows (including {len(failed_ids)} API failures and {len(skipped_rows)} input skips) to {config.failed_ids_csv}"
        )
        write_failed_ids(failed_ids, skipped_rows, config.failed_ids_csv)

    elapsed_seconds = max(time.perf_counter() - start_time, 0.0)
    processed_count = len(movie_rows)
    failed_count = len(failed_ids)
    skipped_count = len(skipped_rows)
    total_attempted = len(movie_ids)
    req_per_sec = (total_attempted / elapsed_seconds) if elapsed_seconds > 0 else 0.0
    logging.info(
        f"Processing complete. processed={processed_count}, failed={failed_count}, skipped={skipped_count}, "
        f"elapsed={elapsed_seconds:.2f}s, rate={req_per_sec:.2f} req/s"
    )


if __name__ == "__main__":
    main()