import pandas as pd

df = pd.read_parquet(
    "data/silver/match_id=3749153/events_78ef32fcafb9.parquet"
)

# passes = df[
#     # order the shots by shot_xg descending
#     (df["event_type"] == "Shot")
# ].sort_values(
#     by="shot_xg",
#     ascending=False
# )

# print(
#     passes[
#         [
#             "team_name",
#             "player_name",
#             "start_x",
#             "start_y",
#             "end_x",
#             "end_y",
#             "outcome",
#             "shot_xg",
#         ]
#     ].head(20)
# )

# progressive = df[
#     df["is_progressive_pass"]
# ]

# print(
#     progressive[
#         [
#             "minute",
#             "team_name",
#             "player_name",
#             "start_x",
#             "start_y",
#             "end_x",
#             "end_y",
#             "progress_toward_goal_m",
#             "progress_ratio",
#         ]
#     ]
#     .head(30)
#     .to_string(index=False)
# )


# print(
#     df[
#         df["is_progressive_pass"]
#     ]
#     .groupby("team_name")
#     .size()
# )

# print(
#     df[
#         df["is_progressive_pass"]
#     ]
#     .groupby("player_name")
#     .size()
#     .sort_values(ascending=False)
#     .head(10)
# )

# # inspecting start_x for shots to validate the attcking direction

# shot_summary = (
#     df[df["is_shot"]]
#     .groupby(
#         ["team_name", "period"]
#     )
#     .agg(
#         shots=("event_id", "count"),
#         avg_shot_x=("start_x", "mean"),
#         min_shot_x=("start_x", "min"),
#         max_shot_x=("start_x", "max"),
#     )
# )

# print(shot_summary)

# most threatening actions
moves = df[
    df["is_successful_move"]
]

print(
    moves[
        [
            "minute",
            "team_name",
            "player_name",
            "event_type",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "xt_start",
            "xt_end",
            "xt_added",
        ]
    ]
    .sort_values(
        "xt_added",
        ascending=False,
    )
    .head(20)
    .to_string(index=False)
)


print(
    moves
    .groupby("player_name")["xt_added"]
    .sum()
    .sort_values(ascending=False)
    .head(15)
)