-- ============================================================
-- Football Analytics - Snowflake Database Setup
-- ============================================================
-- Run this script in Snowflake to set up the data warehouse
-- ============================================================

-- ============================================================
-- 1. CREATE DATABASE & SCHEMA
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS FOOTBALL_ANALYTICS;

-- Use database
USE DATABASE FOOTBALL_ANALYTICS;

-- Create schema for Manchester United analytics
CREATE SCHEMA IF NOT EXISTS MANUTD;

-- Use schema
USE SCHEMA MANUTD;

-- ============================================================
-- 2. CREATE WAREHOUSE
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WITH 
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300          -- Suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE          -- Resume automatically when queries arrive
    INITIALLY_SUSPENDED = TRUE; -- Start suspended to save costs

-- Use warehouse
USE WAREHOUSE ANALYTICS_WH;

-- ============================================================
-- 3. CREATE TABLES
-- ============================================================

-- Dimension: Competitions
CREATE OR REPLACE TABLE DIM_COMPETITIONS (
    competition_id      INTEGER         PRIMARY KEY,
    competition_name    VARCHAR(200),
    season_id           INTEGER,
    season_name         VARCHAR(50),
    country             VARCHAR(100),
    gender              VARCHAR(20),
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Matches
CREATE OR REPLACE TABLE FACT_MATCHES (
    match_id            INTEGER         PRIMARY KEY,
    match_date          DATE,
    kick_off            VARCHAR(20),
    match_week          INTEGER,
    match_status        VARCHAR(50),
    competition_id      INTEGER,
    competition_name    VARCHAR(200),
    season_id           INTEGER,
    season_name         VARCHAR(50),
    home_team_id        INTEGER,
    home_team_name      VARCHAR(200),
    away_team_id        INTEGER,
    away_team_name      VARCHAR(200),
    home_score          INTEGER,
    away_score          INTEGER,
    stadium_name        VARCHAR(200),
    stadium_country     VARCHAR(100),
    winner              VARCHAR(10),        -- HOME, AWAY, DRAW
    total_goals         INTEGER,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Events (main events table)
CREATE OR REPLACE TABLE FACT_EVENTS (
    event_id            VARCHAR(100)    PRIMARY KEY,
    match_id            INTEGER,
    event_index         INTEGER,
    period              INTEGER,
    timestamp           VARCHAR(20),
    minute              INTEGER,
    second              INTEGER,
    event_type_id       INTEGER,
    event_type          VARCHAR(100),
    possession          INTEGER,
    possession_team_id  INTEGER,
    possession_team_name VARCHAR(200),
    play_pattern        VARCHAR(100),
    team_id             INTEGER,
    team_name           VARCHAR(200),
    player_id           INTEGER,
    player_name         VARCHAR(200),
    position            VARCHAR(100),
    location_x          FLOAT,
    location_y          FLOAT,
    duration            FLOAT,
    under_pressure      BOOLEAN,
    off_camera          BOOLEAN,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Passes
CREATE OR REPLACE TABLE FACT_PASSES (
    event_id            VARCHAR(100)    PRIMARY KEY,
    match_id            INTEGER,
    player_name         VARCHAR(200),
    team_name           VARCHAR(200),
    minute              INTEGER,
    pass_start_x        FLOAT,
    pass_start_y        FLOAT,
    pass_end_x          FLOAT,
    pass_end_y          FLOAT,
    pass_length         FLOAT,
    pass_angle          FLOAT,
    pass_height         VARCHAR(50),
    pass_body_part      VARCHAR(50),
    pass_outcome        VARCHAR(100),       -- NULL = successful
    pass_recipient      VARCHAR(200),
    under_pressure      BOOLEAN,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Shots with xG
CREATE OR REPLACE TABLE FACT_SHOTS (
    event_id            VARCHAR(100)    PRIMARY KEY,
    match_id            INTEGER,
    player_name         VARCHAR(200),
    team_name           VARCHAR(200),
    minute              INTEGER,
    shot_x              FLOAT,
    shot_y              FLOAT,
    xg                  FLOAT,              -- Expected Goals
    shot_outcome        VARCHAR(100),       -- Goal, Saved, Off T, etc.
    shot_type           VARCHAR(100),       -- Open Play, Free Kick, etc.
    shot_body_part      VARCHAR(50),
    shot_technique      VARCHAR(100),
    under_pressure      BOOLEAN,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Manchester United specific matches
CREATE OR REPLACE TABLE MANUTD_MATCHES (
    match_id            INTEGER         PRIMARY KEY,
    match_date          DATE,
    competition_name    VARCHAR(200),
    season_name         VARCHAR(50),
    home_team_name      VARCHAR(200),
    away_team_name      VARCHAR(200),
    home_score          INTEGER,
    away_score          INTEGER,
    winner              VARCHAR(10),
    total_goals         INTEGER,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 4. CREATE VIEWS
-- ============================================================

-- View: Team Statistics
CREATE OR REPLACE VIEW V_TEAM_STATS AS
SELECT 
    team_name,
    COUNT(DISTINCT match_id) as matches_played,
    SUM(CASE WHEN event_type = 'Shot' THEN 1 ELSE 0 END) as total_shots,
    SUM(CASE WHEN event_type = 'Pass' THEN 1 ELSE 0 END) as total_passes,
    COUNT(*) as total_events
FROM FACT_EVENTS
GROUP BY team_name;

-- View: Player xG Leaders
CREATE OR REPLACE VIEW V_XG_LEADERS AS
SELECT 
    player_name,
    team_name,
    COUNT(*) as total_shots,
    ROUND(SUM(xg), 2) as total_xg,
    ROUND(AVG(xg), 3) as avg_xg_per_shot,
    SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) as goals,
    ROUND(SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as conversion_rate
FROM FACT_SHOTS
GROUP BY player_name, team_name
HAVING COUNT(*) >= 5
ORDER BY total_xg DESC;

-- View: Pass Completion by Team
CREATE OR REPLACE VIEW V_PASS_COMPLETION AS
SELECT 
    team_name,
    COUNT(*) as total_passes,
    SUM(CASE WHEN pass_outcome IS NULL THEN 1 ELSE 0 END) as completed_passes,
    ROUND(SUM(CASE WHEN pass_outcome IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as completion_rate,
    ROUND(AVG(pass_length), 1) as avg_pass_length
FROM FACT_PASSES
GROUP BY team_name
ORDER BY completion_rate DESC;

-- ============================================================
-- 5. GRANT PERMISSIONS (adjust role names as needed)
-- ============================================================

-- Create role for analytics users
CREATE ROLE IF NOT EXISTS FOOTBALL_ANALYST;

-- Grant permissions
GRANT USAGE ON DATABASE FOOTBALL_ANALYTICS TO ROLE FOOTBALL_ANALYST;
GRANT USAGE ON SCHEMA MANUTD TO ROLE FOOTBALL_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA MANUTD TO ROLE FOOTBALL_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA MANUTD TO ROLE FOOTBALL_ANALYST;

-- ============================================================
-- 6. VERIFY SETUP
-- ============================================================

-- Show tables
SHOW TABLES IN SCHEMA MANUTD;

-- Show views
SHOW VIEWS IN SCHEMA MANUTD;

SELECT 'Setup complete!' as status;
