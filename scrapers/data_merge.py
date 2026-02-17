"""
Utility functions for merging Understat and WhoScored data.

Handles name matching between the two sources and provides
combined query functions.
"""

from difflib import SequenceMatcher
from typing import Optional, List, Tuple
from sqlalchemy import func
from database import (
    init_db, League, Season, Team, Player, PlayerSeasonStats,
    WhoScoredPlayer, WhoScoredPlayerSeasonStats
)


# Common name variations between Understat and WhoScored
TEAM_NAME_MAP = {
    # EPL
    "Manchester United": ["Man United", "Man Utd"],
    "Manchester City": ["Man City"],
    "Tottenham": ["Tottenham Hotspur", "Spurs"],
    "Newcastle United": ["Newcastle"],
    "Wolverhampton Wanderers": ["Wolves"],
    "Brighton": ["Brighton & Hove Albion", "Brighton and Hove Albion"],
    "Nottingham Forest": ["Nott'm Forest"],
    "West Ham": ["West Ham United"],
    "Sheffield United": ["Sheffield Utd"],

    # La Liga
    "Atletico Madrid": ["Atlético Madrid", "Atletico"],
    "Real Betis": ["Betis"],
    "Athletic Club": ["Athletic Bilbao"],
    "Rayo Vallecano": ["Rayo"],

    # Bundesliga
    "Bayern Munich": ["Bayern München", "FC Bayern"],
    "Borussia Dortmund": ["Dortmund", "BVB"],
    "Bayer Leverkusen": ["Leverkusen"],
    "RB Leipzig": ["Leipzig", "RasenBallsport Leipzig"],
    "Borussia M.Gladbach": ["Borussia Monchengladbach", "Gladbach", "Mönchengladbach"],
    "Eintracht Frankfurt": ["Frankfurt"],

    # Serie A
    "AC Milan": ["Milan"],
    "Inter": ["Inter Milan", "Internazionale"],
    "Napoli": ["SSC Napoli"],
    "AS Roma": ["Roma"],

    # Ligue 1
    "Paris Saint Germain": ["Paris Saint-Germain", "PSG"],
    "Olympique Marseille": ["Marseille", "OM"],
    "Olympique Lyonnais": ["Lyon", "OL"],
    "AS Monaco": ["Monaco"],
}


def normalize_name(name: str) -> str:
    """Normalize a name for comparison"""
    if not name:
        return ""
    # Remove accents and lowercase
    import unicodedata
    name = unicodedata.normalize('NFKD', name)
    name = name.encode('ASCII', 'ignore').decode('ASCII')
    return name.lower().strip()


def similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity between two names"""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    return SequenceMatcher(None, n1, n2).ratio()


def find_matching_team(team_name: str, session, threshold: float = 0.8) -> Optional[Team]:
    """
    Find a matching team in the database.
    First tries exact match, then known aliases, then fuzzy match.
    """
    # Exact match
    team = session.query(Team).filter(func.lower(Team.name) == team_name.lower()).first()
    if team:
        return team

    # Check known aliases
    for canonical, aliases in TEAM_NAME_MAP.items():
        all_names = [canonical] + aliases
        if any(normalize_name(n) == normalize_name(team_name) for n in all_names):
            # Find team with any of these names
            for name in all_names:
                team = session.query(Team).filter(func.lower(Team.name) == name.lower()).first()
                if team:
                    return team

    # Fuzzy match
    all_teams = session.query(Team).all()
    best_match = None
    best_score = 0

    for team in all_teams:
        score = similarity_score(team_name, team.name)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = team

    return best_match


def find_matching_player(player_name: str, session, threshold: float = 0.85) -> Optional[Player]:
    """
    Find a matching Understat player for a given name.
    Uses fuzzy matching on names.
    """
    # Exact match
    player = session.query(Player).filter(func.lower(Player.name) == player_name.lower()).first()
    if player:
        return player

    # Fuzzy match
    all_players = session.query(Player).all()
    best_match = None
    best_score = 0

    for player in all_players:
        score = similarity_score(player_name, player.name)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = player

    return best_match


def link_whoscored_to_understat_players(session, threshold: float = 0.85) -> int:
    """
    Attempt to link WhoScored players to their Understat counterparts.
    Returns the number of players linked.
    """
    unlinked = session.query(WhoScoredPlayer).filter(
        WhoScoredPlayer.understat_player_id.is_(None)
    ).all()

    linked_count = 0
    for ws_player in unlinked:
        understat_player = find_matching_player(ws_player.name, session, threshold)
        if understat_player:
            ws_player.understat_player_id = understat_player.id
            linked_count += 1

    session.commit()
    print(f"Linked {linked_count} of {len(unlinked)} unlinked WhoScored players")
    return linked_count


def get_combined_player_stats(session, player_name: str = None, season_year: int = None,
                               league_code: str = None) -> List[dict]:
    """
    Get combined offensive (Understat) and defensive (WhoScored) stats for players.

    Args:
        session: Database session
        player_name: Optional filter by player name
        season_year: Optional filter by season year
        league_code: Optional filter by league code

    Returns:
        List of dicts with combined player statistics
    """
    results = []

    # Build query for Understat stats
    query = session.query(
        Player.name.label('player_name'),
        Team.name.label('team_name'),
        League.name.label('league'),
        Season.year.label('season'),
        PlayerSeasonStats
    ).join(
        PlayerSeasonStats, Player.id == PlayerSeasonStats.player_id
    ).join(
        Team, PlayerSeasonStats.team_id == Team.id
    ).join(
        Season, PlayerSeasonStats.season_id == Season.id
    ).join(
        League, Season.league_id == League.id
    )

    if season_year:
        query = query.filter(Season.year == season_year)
    if league_code:
        query = query.filter(League.name == league_code)
    if player_name:
        query = query.filter(Player.name.ilike(f"%{player_name}%"))

    for row in query.all():
        player_data = {
            'player_name': row.player_name,
            'team': row.team_name,
            'league': row.league,
            'season': f"{row.season}/{row.season + 1}",
            # Understat stats (offensive)
            'games': row.PlayerSeasonStats.games,
            'minutes': row.PlayerSeasonStats.minutes,
            'goals': row.PlayerSeasonStats.goals,
            'assists': row.PlayerSeasonStats.assists,
            'xg': row.PlayerSeasonStats.xg,
            'xa': row.PlayerSeasonStats.xa,
            'npxg': row.PlayerSeasonStats.npxg,
            'shots': row.PlayerSeasonStats.shots,
            'key_passes': row.PlayerSeasonStats.key_passes,
            # Defensive stats (to be filled from WhoScored)
            'tackles': None,
            'interceptions': None,
            'clearances': None,
            'blocks': None,
            'aerial_duels_won': None,
            'aerial_win_pct': None,
        }

        # Try to find matching WhoScored stats
        ws_player = session.query(WhoScoredPlayer).filter(
            WhoScoredPlayer.name.ilike(f"%{row.player_name}%")
        ).first()

        if not ws_player:
            # Try through linked Understat player
            understat_player = session.query(Player).filter_by(name=row.player_name).first()
            if understat_player:
                ws_player = session.query(WhoScoredPlayer).filter_by(
                    understat_player_id=understat_player.id
                ).first()

        if ws_player:
            ws_stats = session.query(WhoScoredPlayerSeasonStats).filter_by(
                player_id=ws_player.id,
                season_id=row.PlayerSeasonStats.season_id
            ).first()

            if ws_stats:
                player_data.update({
                    'tackles': ws_stats.tackles,
                    'tackles_per_90': ws_stats.tackles_per_90,
                    'interceptions': ws_stats.interceptions,
                    'interceptions_per_90': ws_stats.interceptions_per_90,
                    'clearances': ws_stats.clearances,
                    'clearances_per_90': ws_stats.clearances_per_90,
                    'blocks': ws_stats.blocks,
                    'aerial_duels_won': ws_stats.aerial_duels_won,
                    'aerial_duels': ws_stats.aerial_duels,
                    'aerial_win_pct': ws_stats.aerial_win_pct,
                    'recoveries': ws_stats.recoveries,
                })

        results.append(player_data)

    return results


def export_combined_stats_csv(session, output_path: str, season_year: int = None,
                               league_code: str = None):
    """Export combined stats to CSV file"""
    import csv

    stats = get_combined_player_stats(session, season_year=season_year, league_code=league_code)

    if not stats:
        print("No stats to export")
        return

    fieldnames = list(stats[0].keys())

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stats)

    print(f"Exported {len(stats)} player records to {output_path}")


def main():
    """Example usage"""
    _, session = init_db()

    # Link WhoScored players to Understat
    print("Linking WhoScored players to Understat...")
    link_whoscored_to_understat_players(session)

    # Export combined stats
    print("\nExporting combined stats...")
    export_combined_stats_csv(session, 'data/combined_stats_2024.csv', season_year=2024)


if __name__ == "__main__":
    main()
