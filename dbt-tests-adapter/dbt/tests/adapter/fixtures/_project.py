from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Project:
    project_name: str
    project_root: str
    profile_name: Optional[str]
    model_paths: List[str]
    macro_paths: List[str]
    seed_paths: List[str]
    test_paths: List[str]
