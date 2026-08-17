-- CI fixture: one 90-minute match, two teams, mixed event types.
-- Sized so dbt tests can assert grain, invariants, and silver↔gold reconciliation
-- without loading a full StatsBomb file.

CREATE SCHEMA IF NOT EXISTS serving;

DROP TABLE IF EXISTS serving.src_silver_events;
DROP TABLE IF EXISTS serving.dim_match;

CREATE TABLE serving.dim_match (
    match_id bigint PRIMARY KEY,
    match_date date NOT NULL,
    kickoff_time time,
    competition_id integer,
    competition_name text,
    season_id integer,
    season_name text,
    match_week integer,
    home_team_id bigint NOT NULL,
    home_team_name text NOT NULL,
    away_team_id bigint NOT NULL,
    away_team_name text NOT NULL,
    stadium text,
    referee text
);

CREATE TABLE serving.src_silver_events (
    match_id bigint NOT NULL,
    event_id text NOT NULL,
    event_index integer NOT NULL,
    period integer NOT NULL,
    minute integer NOT NULL,
    second integer NOT NULL,
    event_type text NOT NULL,
    team_id bigint,
    team_name text,
    player_id bigint,
    player_name text,
    possession_id integer,
    start_x numeric,
    start_y numeric,
    end_x numeric,
    end_y numeric,
    outcome text,
    shot_xg numeric,
    is_pass boolean NOT NULL,
    is_carry boolean NOT NULL,
    is_shot boolean NOT NULL,
    is_completed_pass boolean NOT NULL,
    is_progressive_pass boolean NOT NULL,
    progress_ratio numeric,
    progress_toward_goal_m numeric,
    is_successful_move boolean NOT NULL,
    xt_start numeric,
    xt_end numeric,
    xt_added numeric,
    xt_model_version text,
    source_version integer,
    file_hash text
);

INSERT INTO serving.dim_match (
    match_id,
    match_date,
    kickoff_time,
    competition_id,
    competition_name,
    season_id,
    season_name,
    match_week,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    stadium,
    referee
)
VALUES (
    1001,
    DATE '2024-04-23',
    TIME '20:00:00',
    2,
    'Premier League',
    281,
    '2023/2024',
    34,
    1,
    'Arsenal',
    2,
    'Chelsea',
    'Emirates Stadium',
    'Michael Oliver'
);

-- Match 1001: Arsenal 1-0 Chelsea.
-- Arsenal: 2 shots / 1 goal, 3 attempted passes / 2 completed / 1 progressive.
-- Chelsea: 1 shot / 0 goals, 3 attempted passes / 2 completed / 0 progressive.
-- Successful moves include both +xt and -xt so interval net_xt = positive_xt + negative_xt.
INSERT INTO serving.src_silver_events (
    match_id, event_id, event_index, period, minute, second,
    event_type, team_id, team_name, player_id, player_name, possession_id,
    start_x, start_y, end_x, end_y, outcome, shot_xg,
    is_pass, is_carry, is_shot, is_completed_pass, is_progressive_pass,
    progress_ratio, progress_toward_goal_m, is_successful_move,
    xt_start, xt_end, xt_added, xt_model_version, source_version, file_hash
)
VALUES
    -- Arsenal, first half
    (1001, 'e1', 1, 1, 3, 12,
     'Pass', 1, 'Arsenal', 101, 'Odegaard', 1,
     40.0, 40.0, 48.0, 42.0, NULL, NULL,
     TRUE, FALSE, FALSE, TRUE, FALSE,
     0.08, 8.0, TRUE,
     0.02, 0.03, 0.01, '1.0', 1, 'ci-fixture'),

    (1001, 'e2', 2, 1, 8, 40,
     'Pass', 1, 'Arsenal', 101, 'Odegaard', 1,
     50.0, 30.0, 85.0, 38.0, NULL, NULL,
     TRUE, FALSE, FALSE, TRUE, TRUE,
     0.35, 35.0, TRUE,
     0.04, 0.12, 0.08, '1.0', 1, 'ci-fixture'),

    (1001, 'e3', 3, 1, 10, 5,
     'Pass', 1, 'Arsenal', 102, 'Saka', 1,
     70.0, 20.0, 78.0, 8.0, 'Incomplete', NULL,
     TRUE, FALSE, FALSE, FALSE, FALSE,
     0.04, 4.0, FALSE,
     NULL, NULL, NULL, '1.0', 1, 'ci-fixture'),

    (1001, 'e4', 4, 1, 22, 18,
     'Carry', 1, 'Arsenal', 102, 'Saka', 2,
     60.0, 25.0, 72.0, 28.0, NULL, NULL,
     FALSE, TRUE, FALSE, FALSE, FALSE,
     0.12, 12.0, TRUE,
     0.06, 0.10, 0.04, '1.0', 1, 'ci-fixture'),

    (1001, 'e5', 5, 1, 22, 41,
     'Carry', 1, 'Arsenal', 102, 'Saka', 2,
     72.0, 28.0, 64.0, 30.0, NULL, NULL,
     FALSE, TRUE, FALSE, FALSE, FALSE,
     -0.08, -8.0, TRUE,
     0.10, 0.08, -0.02, '1.0', 1, 'ci-fixture'),

    (1001, 'e6', 6, 1, 31, 9,
     'Shot', 1, 'Arsenal', 103, 'Havertz', 2,
     108.0, 40.0, 120.0, 40.0, 'Saved', 0.12,
     FALSE, FALSE, TRUE, FALSE, FALSE,
     NULL, NULL, FALSE,
     NULL, NULL, NULL, '1.0', 1, 'ci-fixture'),

    -- Arsenal stoppage time (period 1, minute >= 45)
    (1001, 'e7', 7, 1, 45, 22,
     'Pass', 1, 'Arsenal', 101, 'Odegaard', 3,
     35.0, 50.0, 42.0, 48.0, NULL, NULL,
     TRUE, FALSE, FALSE, TRUE, FALSE,
     0.07, 7.0, TRUE,
     0.02, 0.03, 0.01, '1.0', 1, 'ci-fixture'),

    -- Arsenal, second half: the goal
    (1001, 'e8', 8, 2, 67, 3,
     'Shot', 1, 'Arsenal', 102, 'Saka', 4,
     112.0, 36.0, 120.0, 40.0, 'Goal', 0.35,
     FALSE, FALSE, TRUE, FALSE, FALSE,
     NULL, NULL, FALSE,
     NULL, NULL, NULL, '1.0', 1, 'ci-fixture'),

    -- Chelsea
    (1001, 'e9', 9, 1, 5, 50,
     'Pass', 2, 'Chelsea', 201, 'Palmer', 5,
     30.0, 40.0, 44.0, 38.0, NULL, NULL,
     TRUE, FALSE, FALSE, TRUE, FALSE,
     0.10, 14.0, TRUE,
     0.02, 0.04, 0.02, '1.0', 1, 'ci-fixture'),

    (1001, 'e10', 10, 1, 18, 11,
     'Pass', 2, 'Chelsea', 202, 'Gallagher', 5,
     55.0, 60.0, 62.0, 72.0, 'Out', NULL,
     TRUE, FALSE, FALSE, FALSE, FALSE,
     0.05, 5.0, FALSE,
     NULL, NULL, NULL, '1.0', 1, 'ci-fixture'),

    (1001, 'e11', 11, 2, 50, 28,
     'Carry', 2, 'Chelsea', 201, 'Palmer', 6,
     80.0, 40.0, 74.0, 42.0, NULL, NULL,
     FALSE, TRUE, FALSE, FALSE, FALSE,
     -0.06, -6.0, TRUE,
     0.09, 0.08, -0.01, '1.0', 1, 'ci-fixture'),

    (1001, 'e12', 12, 2, 88, 14,
     'Shot', 2, 'Chelsea', 203, 'Jackson', 6,
     105.0, 44.0, 120.0, 38.0, 'Off T', 0.08,
     FALSE, FALSE, TRUE, FALSE, FALSE,
     NULL, NULL, FALSE,
     NULL, NULL, NULL, '1.0', 1, 'ci-fixture'),

    -- Chelsea stoppage time (period 2, minute >= 90)
    (1001, 'e13', 13, 2, 91, 6,
     'Pass', 2, 'Chelsea', 201, 'Palmer', 7,
     40.0, 20.0, 52.0, 24.0, NULL, NULL,
     TRUE, FALSE, FALSE, TRUE, FALSE,
     0.10, 12.0, TRUE,
     0.03, 0.04, 0.01, '1.0', 1, 'ci-fixture');
