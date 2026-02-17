# export_for_ml.py
import pandas as pd
import sqlite3

conn = sqlite3.connect('scrapers/data/stats.db')

df = pd.read_sql_query("""
    SELECT
        p.name as player_name,
        l.name as league,
        s.year as season,
        t.name as team,
        ps.*,
        ws.tackles,
        ws.tackles_won,
        ws.interceptions,
        ws.clearances,
        ws.blocks,
        ws.aerial_duels,
        ws.aerial_duels_won,
        ws.fouls_committed,
        ws.fouls_won,
        ws.dribbled_past,
        ws.recoveries,
        ws.dispossessed,
        ws.errors_leading_to_shot,
        ws.tackles_per_90,
        ws.interceptions_per_90,
        ws.clearances_per_90,
        ws.aerial_win_pct
    FROM player_season_stats ps
    JOIN players p ON ps.player_id = p.id
    JOIN teams t ON ps.team_id = t.id
    JOIN seasons s ON ps.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    LEFT JOIN whoscored_players wp
        ON wp.understat_player_id = ps.player_id
    LEFT JOIN whoscored_player_season_stats ws
        ON ws.player_id = wp.id
        AND ws.season_id = ps.season_id
""", conn)

# Drop duplicates from transfers (keep row with more defensive data)
df = df.sort_values('tackles', ascending=False).drop_duplicates(
    subset=['player_id', 'season_id'], keep='first'
)

df.to_csv('fpti_model/fpti_player_data.csv', index=False)