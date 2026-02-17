from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class League(Base):
    __tablename__ = 'leagues'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)  # e.g., "EPL", "La_liga", "Bundesliga"
    display_name = Column(String)  # e.g., "English Premier League"

    teams = relationship("Team", back_populates="league")
    seasons = relationship("Season", back_populates="league")


class Season(Base):
    __tablename__ = 'seasons'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)  # e.g., 2024 for 2024/25 season
    league_id = Column(Integer, ForeignKey('leagues.id'))

    league = relationship("League", back_populates="seasons")
    team_stats = relationship("TeamSeasonStats", back_populates="season")
    player_stats = relationship("PlayerSeasonStats", back_populates="season")
    matches = relationship("Match", back_populates="season")


class Team(Base):
    __tablename__ = 'teams'

    id = Column(Integer, primary_key=True)
    understat_id = Column(Integer, unique=True)  # Understat's internal team ID
    name = Column(String)
    league_id = Column(Integer, ForeignKey('leagues.id'))

    league = relationship("League", back_populates="teams")
    season_stats = relationship("TeamSeasonStats", back_populates="team")
    player_seasons = relationship("PlayerSeasonStats", back_populates="team")
    home_matches = relationship("Match", foreign_keys="Match.home_team_id", back_populates="home_team")
    away_matches = relationship("Match", foreign_keys="Match.away_team_id", back_populates="away_team")


class TeamSeasonStats(Base):
    """Aggregated team stats per season from Understat"""
    __tablename__ = 'team_season_stats'

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))

    # Basic stats
    matches_played = Column(Integer)
    wins = Column(Integer)
    draws = Column(Integer)
    losses = Column(Integer)
    goals = Column(Integer)
    goals_against = Column(Integer)
    points = Column(Integer)

    # Expected goals
    xg = Column(Float)
    xg_against = Column(Float)
    npxg = Column(Float)  # Non-penalty xG
    npxg_against = Column(Float)
    xg_diff = Column(Float)
    npxg_diff = Column(Float)

    # Pressing stats
    ppda = Column(Float)  # Passes allowed per defensive action
    oppda = Column(Float)  # Opponent PPDA

    # Deep completions
    deep = Column(Integer)  # Passes completed within 20 yards of goal
    deep_allowed = Column(Integer)

    # Expected points
    xpts = Column(Float)

    team = relationship("Team", back_populates="season_stats")
    season = relationship("Season", back_populates="team_stats")


class Player(Base):
    __tablename__ = 'players'

    id = Column(Integer, primary_key=True)
    understat_id = Column(Integer, unique=True)  # Understat's internal player ID
    name = Column(String)

    season_stats = relationship("PlayerSeasonStats", back_populates="player")
    shots = relationship("Shot", back_populates="player")


class PlayerSeasonStats(Base):
    """Player stats per season from Understat"""
    __tablename__ = 'player_season_stats'

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))

    # Basic stats
    games = Column(Integer)
    minutes = Column(Integer)
    goals = Column(Integer)
    assists = Column(Integer)
    shots = Column(Integer)
    key_passes = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    position = Column(String)  # Primary position played

    # Expected goals / assists
    xg = Column(Float)
    xa = Column(Float)
    npg = Column(Integer)  # Non-penalty goals
    npxg = Column(Float)  # Non-penalty xG
    xg_chain = Column(Float)  # xG chain (involved in attack)
    xg_buildup = Column(Float)  # xG buildup (involved but not shooter/assister)

    player = relationship("Player", back_populates="season_stats")
    team = relationship("Team", back_populates="player_seasons")
    season = relationship("Season", back_populates="player_stats")


class Match(Base):
    """Match-level data with xG"""
    __tablename__ = 'matches'

    id = Column(Integer, primary_key=True)
    understat_id = Column(Integer, unique=True)
    season_id = Column(Integer, ForeignKey('seasons.id'))
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))

    # Score
    home_goals = Column(Integer)
    away_goals = Column(Integer)

    # Expected goals
    home_xg = Column(Float)
    away_xg = Column(Float)

    # Match info
    date = Column(String)  # ISO date string
    is_result = Column(Boolean)  # True if match has been played

    season = relationship("Season", back_populates="matches")
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_matches")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_matches")
    shots = relationship("Shot", back_populates="match")


class Shot(Base):
    """Individual shot data with xG values"""
    __tablename__ = 'shots'

    id = Column(Integer, primary_key=True)
    understat_id = Column(Integer, unique=True)
    match_id = Column(Integer, ForeignKey('matches.id'))
    player_id = Column(Integer, ForeignKey('players.id'))

    # Shot details
    minute = Column(Integer)
    x = Column(Float)  # X coordinate (0-1)
    y = Column(Float)  # Y coordinate (0-1)
    xg = Column(Float)

    # Outcome
    result = Column(String)  # Goal, SavedShot, MissedShots, BlockedShot, ShotOnPost
    situation = Column(String)  # OpenPlay, FromCorner, SetPiece, DirectFreekick, Penalty
    shot_type = Column(String)  # RightFoot, LeftFoot, Head

    # Additional context
    last_action = Column(String)  # What happened before the shot
    is_home = Column(Boolean)

    match = relationship("Match", back_populates="shots")
    player = relationship("Player", back_populates="shots")


# ============================================================================
# WhoScored Models - Defensive Statistics
# ============================================================================

class WhoScoredPlayer(Base):
    """Player record from WhoScored (separate IDs from Understat)"""
    __tablename__ = 'whoscored_players'

    id = Column(Integer, primary_key=True)
    whoscored_id = Column(Integer, unique=True)
    name = Column(String)
    # Link to Understat player if matched
    understat_player_id = Column(Integer, ForeignKey('players.id'), nullable=True)

    understat_player = relationship("Player", backref="whoscored_player")
    defensive_stats = relationship("WhoScoredPlayerSeasonStats", back_populates="player")


class WhoScoredPlayerSeasonStats(Base):
    """Player defensive stats per season from WhoScored"""
    __tablename__ = 'whoscored_player_season_stats'

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('whoscored_players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))

    # Basic info
    games = Column(Integer)
    minutes = Column(Integer)
    position = Column(String)

    # Defensive stats
    tackles = Column(Integer)
    tackles_won = Column(Integer)
    interceptions = Column(Integer)
    clearances = Column(Integer)
    blocks = Column(Integer)

    # Aerial stats
    aerial_duels = Column(Integer)
    aerial_duels_won = Column(Integer)

    # Other defensive metrics
    fouls_committed = Column(Integer)
    fouls_won = Column(Integer)
    dribbled_past = Column(Integer)  # Times dribbled past (bad)

    # Ball recovery
    recoveries = Column(Integer)

    # Passing under pressure / defensive contribution
    dispossessed = Column(Integer)
    errors_leading_to_shot = Column(Integer)

    # Per 90 stats (calculated)
    tackles_per_90 = Column(Float)
    interceptions_per_90 = Column(Float)
    clearances_per_90 = Column(Float)
    aerial_win_pct = Column(Float)

    player = relationship("WhoScoredPlayer", back_populates="defensive_stats")
    team = relationship("Team")
    season = relationship("Season")


def init_db(db_path='data/understat.db'):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return engine, Session()
