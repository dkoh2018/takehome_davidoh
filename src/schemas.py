from dataclasses import dataclass
from typing import List, Optional

@dataclass
class MovieRow:
    id: int
    title: str
    vote_average: Optional[float]
    genres: List[str]
    is_action: bool = False