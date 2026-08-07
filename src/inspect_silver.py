import pandas as pd

df = pd.read_parquet(
    "data/silver/match_id=3749153/events_78ef32fcafb9.parquet"
)

passes = df[
    df["event_type"] == "Pass"
]

print(
    passes[
        [
            "player_name",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "outcome",
        ]
    ].head(20)
)