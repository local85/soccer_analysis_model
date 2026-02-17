"""
Understat.com Web Scraper

Scrapes xG data from understat.com including:
- League standings with xG stats
- Player stats per season
- Match data with xG
- Individual shot data

Uses Playwright for JavaScript rendering.
"""

import json
import time
from typing import Optional
from playwright.sync_api import sync_playwright, Browser, Page
from database import (
    init_db, League, Season, Team, TeamSeasonStats,
    Player, PlayerSeasonStats, Match, Shot
)

BASE_URL = "https://understat.com"

LEAGUES = {
    "EPL": "English Premier League",
    "La_liga": "La Liga",
    "Bundesliga": "Bundesliga",
    "Serie_A": "Serie A",
    "Ligue_1": "Ligue 1",
    "RFPL": "Russian Premier League"
}


class UnderstatScraper:
    def __init__(self, db_path: str = "data/stats.db"):
        self.engine, self.session = init_db(db_path)
        self.request_delay = 1.5  # Be respectful to the server
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._playwright = None

    def _start_browser(self):
        """Start Playwright browser if not running"""
        if self.browser is None:
            self._playwright = sync_playwright().start()
            self.browser = self._playwright.chromium.launch(headless=True)
            self.page = self.browser.new_page()

    def _stop_browser(self):
        """Stop Playwright browser"""
        if self.browser:
            self.browser.close()
            self._playwright.stop()
            self.browser = None
            self.page = None

    def _fetch_page_data(self, url: str, data_vars: list[str]) -> dict:
        """
        Fetch a page and extract JavaScript data variables.

        Args:
            url: Page URL to fetch
            data_vars: List of JS variable names to extract (e.g., ['teamsData', 'playersData'])

        Returns:
            Dict mapping variable names to their parsed JSON values
        """
        self._start_browser()
        time.sleep(self.request_delay)

        self.page.goto(url, wait_until="networkidle")

        results = {}
        for var_name in data_vars:
            try:
                # Execute JS to get the variable value
                data = self.page.evaluate(f"""
                    () => {{
                        if (typeof {var_name} !== 'undefined') {{
                            return {var_name};
                        }}
                        return null;
                    }}
                """)
                if data is not None:
                    results[var_name] = data
            except Exception as e:
                print(f"  Warning: Could not extract {var_name}: {e}")

        return results

    def _get_or_create_league(self, league_code: str) -> League:
        """Get or create a league by its code"""
        league = self.session.query(League).filter_by(name=league_code).first()
        if not league:
            league = League(
                name=league_code,
                display_name=LEAGUES.get(league_code, league_code)
            )
            self.session.add(league)
            self.session.commit()
        return league

    def _get_or_create_season(self, year: int, league: League) -> Season:
        """Get or create a season"""
        season = self.session.query(Season).filter_by(
            year=year, league_id=league.id
        ).first()
        if not season:
            season = Season(year=year, league_id=league.id)
            self.session.add(season)
            self.session.commit()
        return season

    def _get_or_create_team(self, understat_id: int, name: str, league: League) -> Team:
        """Get or create a team by Understat ID"""
        team = self.session.query(Team).filter_by(understat_id=understat_id).first()
        if not team:
            team = Team(
                understat_id=understat_id,
                name=name,
                league_id=league.id
            )
            self.session.add(team)
            self.session.commit()
        return team

    def _get_or_create_player(self, understat_id: int, name: str) -> Player:
        """Get or create a player by Understat ID"""
        player = self.session.query(Player).filter_by(understat_id=understat_id).first()
        if not player:
            player = Player(understat_id=understat_id, name=name)
            self.session.add(player)
            self.session.commit()
        return player

    def scrape_league_season(self, league_code: str, year: int):
        """
        Scrape all data for a league season.

        Args:
            league_code: One of EPL, La_liga, Bundesliga, Serie_A, Ligue_1, RFPL
            year: The starting year of the season (e.g., 2024 for 2024/25)
        """
        print(f"Scraping {league_code} {year}/{year+1}...")

        url = f"{BASE_URL}/league/{league_code}/{year}"

        # Fetch page and extract data
        data = self._fetch_page_data(url, ["teamsData", "playersData", "datesData"])

        league = self._get_or_create_league(league_code)
        season = self._get_or_create_season(year, league)

        teams_data = data.get("teamsData")
        players_data = data.get("playersData")
        dates_data = data.get("datesData")

        if teams_data:
            self._process_teams_data(teams_data, league, season)
        else:
            print(f"  No teams data found")

        if players_data:
            self._process_players_data(players_data, league, season)
        else:
            print(f"  No players data found")

        if dates_data:
            self._process_matches_data(dates_data, league, season)
        else:
            print(f"  No matches data found")

        self.session.commit()
        print(f"Completed {league_code} {year}/{year+1}")

    def _process_teams_data(self, teams_data: dict, league: League, season: Season):
        """Process team statistics from league page"""
        for team_id_str, data in teams_data.items():
            understat_id = int(team_id_str)
            team_name = data.get("title", "Unknown")

            team = self._get_or_create_team(understat_id, team_name, league)

            # Check if stats already exist for this team/season
            existing = self.session.query(TeamSeasonStats).filter_by(
                team_id=team.id, season_id=season.id
            ).first()

            if existing:
                team_stats = existing
            else:
                team_stats = TeamSeasonStats(team_id=team.id, season_id=season.id)
                self.session.add(team_stats)

            # Parse history to get aggregated stats
            history = data.get("history", [])
            if history:
                team_stats.matches_played = len(history)
                team_stats.wins = sum(1 for m in history if m.get("wins", 0) == 1)
                team_stats.draws = sum(1 for m in history if m.get("draws", 0) == 1)
                team_stats.losses = sum(1 for m in history if m.get("loses", 0) == 1)
                team_stats.goals = sum(int(m.get("scored", 0)) for m in history)
                team_stats.goals_against = sum(int(m.get("missed", 0)) for m in history)
                team_stats.points = sum(int(m.get("pts", 0)) for m in history)
                team_stats.xg = sum(float(m.get("xG", 0)) for m in history)
                team_stats.xg_against = sum(float(m.get("xGA", 0)) for m in history)
                team_stats.npxg = sum(float(m.get("npxG", 0)) for m in history)
                team_stats.npxg_against = sum(float(m.get("npxGA", 0)) for m in history)
                team_stats.deep = sum(int(m.get("deep", 0)) for m in history)
                team_stats.deep_allowed = sum(int(m.get("deep_allowed", 0)) for m in history)
                team_stats.xpts = sum(float(m.get("xpts", 0)) for m in history)

                # Average PPDA over the season
                ppda_values = [float(m.get("ppda", {}).get("att", 0)) / max(float(m.get("ppda", {}).get("def", 1)), 1)
                               for m in history if m.get("ppda")]
                team_stats.ppda = sum(ppda_values) / len(ppda_values) if ppda_values else None

            team_stats.xg_diff = (team_stats.xg or 0) - (team_stats.xg_against or 0)
            team_stats.npxg_diff = (team_stats.npxg or 0) - (team_stats.npxg_against or 0)

        print(f"  Processed {len(teams_data)} teams")

    def _process_players_data(self, players_data: list, league: League, season: Season):
        """Process player statistics from league page"""
        for p in players_data:
            understat_id = int(p.get("id", 0))
            player_name = p.get("player_name", "Unknown")

            player = self._get_or_create_player(understat_id, player_name)

            # Get or create team - handle comma-separated team names (players who moved mid-season)
            team_title = p.get("team_title", "Unknown")
            # Take the first team if multiple are listed
            team_name = team_title.split(",")[0].strip() if team_title else "Unknown"

            # Try to find existing team
            team = self.session.query(Team).filter_by(name=team_name).first()
            if not team:
                # Use hash of team name as placeholder ID (negative to avoid collision with real IDs)
                placeholder_id = -abs(hash(team_name)) % 1000000
                # Check if this placeholder ID already exists
                existing = self.session.query(Team).filter_by(understat_id=placeholder_id).first()
                if existing:
                    team = existing
                else:
                    team = Team(
                        understat_id=placeholder_id,
                        name=team_name,
                        league_id=league.id
                    )
                    self.session.add(team)
                    self.session.commit()

            # Check if stats already exist
            existing = self.session.query(PlayerSeasonStats).filter_by(
                player_id=player.id, season_id=season.id, team_id=team.id
            ).first()

            if existing:
                stats = existing
            else:
                stats = PlayerSeasonStats(
                    player_id=player.id,
                    team_id=team.id,
                    season_id=season.id
                )
                self.session.add(stats)

            # Update stats
            stats.games = int(p.get("games", 0))
            stats.minutes = int(p.get("time", 0))
            stats.goals = int(p.get("goals", 0))
            stats.assists = int(p.get("assists", 0))
            stats.shots = int(p.get("shots", 0))
            stats.key_passes = int(p.get("key_passes", 0))
            stats.yellow_cards = int(p.get("yellow_cards", 0))
            stats.red_cards = int(p.get("red_cards", 0))
            stats.position = p.get("position", "")
            stats.xg = float(p.get("xG", 0))
            stats.xa = float(p.get("xA", 0))
            stats.npg = int(p.get("npg", 0))
            stats.npxg = float(p.get("npxG", 0))
            stats.xg_chain = float(p.get("xGChain", 0))
            stats.xg_buildup = float(p.get("xGBuildup", 0))

        print(f"  Processed {len(players_data)} players")

    def _process_matches_data(self, dates_data: list, league: League, season: Season):
        """Process match data from league page"""
        match_count = 0
        for match_data in dates_data:
            understat_id = int(match_data.get("id", 0))

            # Skip if already exists
            existing = self.session.query(Match).filter_by(understat_id=understat_id).first()
            if existing:
                continue

            # Get teams
            home_team_name = match_data.get("h", {}).get("title", "Unknown")
            away_team_name = match_data.get("a", {}).get("title", "Unknown")

            home_team = self.session.query(Team).filter_by(name=home_team_name).first()
            away_team = self.session.query(Team).filter_by(name=away_team_name).first()

            if not home_team or not away_team:
                continue

            is_result = match_data.get("isResult", False)

            match = Match(
                understat_id=understat_id,
                season_id=season.id,
                home_team_id=home_team.id,
                away_team_id=away_team.id,
                home_goals=int(match_data.get("h", {}).get("goals", 0)) if is_result else None,
                away_goals=int(match_data.get("a", {}).get("goals", 0)) if is_result else None,
                home_xg=float(match_data.get("xG", {}).get("h", 0)) if is_result else None,
                away_xg=float(match_data.get("xG", {}).get("a", 0)) if is_result else None,
                date=match_data.get("datetime", ""),
                is_result=is_result
            )
            self.session.add(match)
            match_count += 1

        print(f"  Processed {match_count} new matches")

    def scrape_match_shots(self, match_understat_id: int):
        """Scrape individual shot data for a specific match"""
        url = f"{BASE_URL}/match/{match_understat_id}"
        data = self._fetch_page_data(url, ["shotsData"])

        shots_data = data.get("shotsData")
        if not shots_data:
            print(f"No shot data found for match {match_understat_id}")
            return

        match = self.session.query(Match).filter_by(understat_id=match_understat_id).first()
        if not match:
            print(f"Match {match_understat_id} not found in database")
            return

        shot_count = 0
        for side in ["h", "a"]:  # home and away
            is_home = side == "h"
            for shot in shots_data.get(side, []):
                shot_id = int(shot.get("id", 0))

                # Skip if already exists
                if self.session.query(Shot).filter_by(understat_id=shot_id).first():
                    continue

                player_id = int(shot.get("player_id", 0))
                player_name = shot.get("player", "Unknown")
                player = self._get_or_create_player(player_id, player_name)

                new_shot = Shot(
                    understat_id=shot_id,
                    match_id=match.id,
                    player_id=player.id,
                    minute=int(shot.get("minute", 0)),
                    x=float(shot.get("X", 0)),
                    y=float(shot.get("Y", 0)),
                    xg=float(shot.get("xG", 0)),
                    result=shot.get("result", ""),
                    situation=shot.get("situation", ""),
                    shot_type=shot.get("shotType", ""),
                    last_action=shot.get("lastAction", ""),
                    is_home=is_home
                )
                self.session.add(new_shot)
                shot_count += 1

        self.session.commit()
        print(f"  Added {shot_count} shots for match {match_understat_id}")

    def scrape_player_history(self, player_understat_id: int):
        """Scrape a player's full career history from their player page"""
        url = f"{BASE_URL}/player/{player_understat_id}"
        data = self._fetch_page_data(url, ["groupsData", "shotsData"])

        grouped_data = data.get("groupsData")
        shots_data = data.get("shotsData")

        if grouped_data:
            print(f"Found career data for player {player_understat_id}")

        if shots_data:
            print(f"Found {len(shots_data)} career shots for player {player_understat_id}")

        return grouped_data, shots_data

    def scrape_all_leagues(self, year: int):
        """Scrape all available leagues for a given season"""
        for league_code in LEAGUES.keys():
            try:
                self.scrape_league_season(league_code, year)
            except Exception as e:
                print(f"Error scraping {league_code}: {e}")

    def scrape_all_match_shots(self, league_code: str, year: int):
        """Scrape shot data for all matches in a league season"""
        league = self.session.query(League).filter_by(name=league_code).first()
        if not league:
            print(f"League {league_code} not found")
            return

        season = self.session.query(Season).filter_by(year=year, league_id=league.id).first()
        if not season:
            print(f"Season {year} not found for {league_code}")
            return

        matches = self.session.query(Match).filter_by(
            season_id=season.id, is_result=True
        ).all()

        print(f"Scraping shots for {len(matches)} matches...")
        for match in matches:
            self.scrape_match_shots(match.understat_id)

    def close(self):
        """Clean up resources"""
        self._stop_browser()


def main():
    scraper = UnderstatScraper()

    try:
        # Top 5 leagues (excluding RFPL)
        top_5_leagues = ["EPL", "La_liga", "Serie_A", "Bundesliga", "Ligue_1"]

        # Seasons from 2015/16 to 2025/26
        seasons = range(2015, 2026)

        total = len(top_5_leagues) * len(seasons)
        count = 0

        for year in seasons:
            for league_code in top_5_leagues:
                count += 1
                print(f"\n[{count}/{total}] ", end="")
                try:
                    scraper.scrape_league_season(league_code, year)
                except Exception as e:
                    print(f"Error scraping {league_code} {year}: {e}")
                    scraper.session.rollback()  # Reset session after error
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
