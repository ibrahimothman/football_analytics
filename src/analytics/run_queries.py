from src.analytics.queries import (
    team_season_summary,
    match_by_match_performance,
    team_home_away_summary,
)


def main():
    team = "Arsenal"
    season = "2003/2004"

    print("\n=== TEAM SEASON SUMMARY ===")
    print(
        team_season_summary(
            team,
            season,
        ).to_string(index=False)
    )

    print("\n=== MATCH BY MATCH ===")
    print(
        match_by_match_performance(
            team,
            season,
        ).to_string(index=False)
    )

    print("\n=== HOME VS AWAY ===")
    print(
        team_home_away_summary(
            team,
            season,
        ).to_string(index=False)
    )


if __name__ == "__main__":
    main()