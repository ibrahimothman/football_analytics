from src.build_gold import validate_gold
import pandas as pd
import pytest

def test_completed_passes_less_than_attempted():
    df = pd.DataFrame(
        [
            {
                "goals": 2,
                "shots": 10,
                "passes_attempted": 500,
                "passes_completed": 600,
                "progressive_passes": 40,
                "pass_completion_pct": 86.0,
                "xg": 1.7,
            },
            {
                "goals": 1,
                "shots": 7,
                "passes_attempted": 350,
                "passes_completed": 280,
                "progressive_passes": 25,
                "pass_completion_pct": 80.0,
                "xg": 0.9,
            },
        ]
    )

    with pytest.raises(ValueError):
        validate_gold(df)