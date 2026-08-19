import pandas as pd

TEAM_COLORS = ("C0", "C1")

def ordered_teams(team_names) -> list[str]:
    unique = {n for n in team_names if pd.notna(n)}
    return sorted(unique)

def team_color_map(teams: list[str]) -> dict[str, str]:
    return {team: TEAM_COLORS[i] for i, team in enumerate(teams)}