"""
WhoScored.com Web Scraper

Scrapes defensive player statistics from WhoScored including:
- Tackles, interceptions, clearances, blocks
- Aerial duels won
- Recoveries, fouls, etc.

Uses Playwright for JavaScript rendering.
Note: WhoScored has anti-bot protection - may require adjustments.
"""

import asyncio
import re
from typing import Optional
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeout
from database import (
    init_db, League, Season, Team, Player,
    WhoScoredPlayer, WhoScoredPlayerSeasonStats
)

BASE_URL = "https://www.whoscored.com"

# WhoScored league URL mappings
# Uses lowercase URL format: /regions/{id}/tournaments/{id}/
WHOSCORED_LEAGUES = {
    "EPL": {
        "region": 252,  # England
        "tournament": 2,  # Premier League
    },
    "La_liga": {
        "region": 206,  # Spain
        "tournament": 4,  # La Liga
    },
    "Bundesliga": {
        "region": 81,  # Germany
        "tournament": 3,  # Bundesliga
    },
    "Serie_A": {
        "region": 108,  # Italy
        "tournament": 5,  # Serie A
    },
    "Ligue_1": {
        "region": 74,  # France
        "tournament": 22,  # Ligue 1
    }
}


class WhoScoredScraper:
    def __init__(self, db_path: str = "data/understat.db"):
        self.engine, self.session = init_db(db_path)
        self.request_delay = 3.0  # WhoScored is stricter - longer delay
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._playwright = None

    async def _start_browser(self):
        """Start Playwright browser with stealth settings"""
        if self.browser is None:
            self._playwright = await async_playwright().start()
            self.browser = await self._playwright.chromium.launch(
                headless=False,  # Set to True for production
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                ]
            )
            context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            self.page = await context.new_page()

            # Mask webdriver detection
            await self.page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

    async def _stop_browser(self):
        """Stop Playwright browser"""
        if self.browser:
            await self.browser.close()
            await self._playwright.stop()
            self.browser = None
            self.page = None

    def _safe_int(self, value, default=0):
        """Safely convert to int"""
        if value is None or value == '-' or value == '':
            return default
        try:
            return int(float(str(value).replace(',', '')))
        except (ValueError, TypeError):
            return default

    def _safe_float(self, value, default=0.0):
        """Safely convert to float"""
        if value is None or value == '-' or value == '':
            return default
        try:
            return float(str(value).replace(',', '').replace('%', ''))
        except (ValueError, TypeError):
            return default

    def _get_or_create_whoscored_player(self, whoscored_id: int, name: str) -> WhoScoredPlayer:
        """Get or create a WhoScored player record"""
        player = self.session.query(WhoScoredPlayer).filter_by(whoscored_id=whoscored_id).first()
        if not player:
            # Try to find matching Understat player by name
            understat_player = self.session.query(Player).filter(
                Player.name.ilike(f"%{name}%")
            ).first()

            player = WhoScoredPlayer(
                whoscored_id=whoscored_id,
                name=name,
                understat_player_id=understat_player.id if understat_player else None
            )
            self.session.add(player)
            self.session.commit()
        return player

    def _get_or_create_team(self, name: str, league: League) -> Team:
        """Get or create a team by name"""
        # Handle None or empty name
        if not name:
            name = 'Unknown'

        # Try exact match first
        team = self.session.query(Team).filter_by(name=name).first()
        if team:
            return team

        # Try case-insensitive match
        team = self.session.query(Team).filter(
            Team.name.ilike(name)
        ).first()
        if team:
            return team

        # Create new team with placeholder ID
        placeholder_id = -abs(hash(f"ws_{name}")) % 1000000
        existing = self.session.query(Team).filter_by(understat_id=placeholder_id).first()
        if existing:
            return existing

        team = Team(
            understat_id=placeholder_id,
            name=name,
            league_id=league.id
        )
        self.session.add(team)
        self.session.commit()
        return team

    def _get_season(self, year: int, league: League) -> Optional[Season]:
        """Get existing season or create if not exists"""
        season = self.session.query(Season).filter_by(
            year=year, league_id=league.id
        ).first()
        if not season:
            season = Season(year=year, league_id=league.id)
            self.session.add(season)
            self.session.commit()
        return season

    def _get_league(self, league_code: str) -> Optional[League]:
        """Get league by code"""
        return self.session.query(League).filter_by(name=league_code).first()

    async def _discover_season_urls(self, league_code: str) -> dict:
        """
        Discover all available season URLs for a league from the tournament page.
        Returns a dict mapping year -> playerstatistics URL.
        """
        league_info = WHOSCORED_LEAGUES[league_code]
        url = f"{BASE_URL}/regions/{league_info['region']}/tournaments/{league_info['tournament']}"

        print(f"  Discovering seasons from {url}")
        await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Handle cookie consent
        try:
            await self.page.click('button[id*="accept"], button[class*="accept"], #onetrust-accept-btn-handler', timeout=3000)
            await asyncio.sleep(1)
        except:
            pass

        # Extract all season links with their season/stage IDs and year labels
        season_data = await self.page.evaluate("""
            () => {
                const seasons = [];
                const html = document.documentElement.innerHTML;

                // Method 1: Look for links with season/stage URLs and year text
                const links = document.querySelectorAll('a[href]');
                for (const link of links) {
                    const href = link.href;
                    const match = href.match(/seasons\\/(\\d+)\\/stages\\/(\\d+)/i);
                    if (match) {
                        const text = link.textContent.trim();
                        // Extract year from text like "2017/2018" or from URL slug
                        const yearMatch = text.match(/(20\\d{2})\\/(20\\d{2})/) ||
                                          href.match(/(20\\d{2})-(20\\d{2})/);
                        if (yearMatch) {
                            seasons.push({
                                year: parseInt(yearMatch[1]),
                                seasonId: match[1],
                                stageId: match[2],
                                text: text,
                                href: href
                            });
                        }
                    }
                }

                // Method 2: Look for select dropdown options
                const selects = document.querySelectorAll('select');
                for (const sel of selects) {
                    const options = sel.querySelectorAll('option');
                    for (const opt of options) {
                        const text = opt.textContent.trim();
                        const yearMatch = text.match(/(20\\d{2})\\/(20\\d{2})/);
                        const val = opt.value;
                        // Value might be a URL or contain season/stage IDs
                        const idMatch = val.match(/seasons\\/(\\d+)\\/stages\\/(\\d+)/i) ||
                                        val.match(/(\\d+)/);
                        if (yearMatch && idMatch) {
                            seasons.push({
                                year: parseInt(yearMatch[1]),
                                seasonId: idMatch[1],
                                stageId: idMatch[2] || null,
                                text: text,
                                href: val.startsWith('http') ? val : null,
                                selectValue: val
                            });
                        }
                    }
                }

                // Method 3: Parse season data from page scripts
                const scripts = document.querySelectorAll('script');
                for (const script of scripts) {
                    const src = script.textContent;
                    // Look for arrays of season objects
                    const seasonArrayMatch = src.match(/allSeasons[^=]*=\\s*(\\[.*?\\])/s);
                    if (seasonArrayMatch) {
                        try {
                            const arr = JSON.parse(seasonArrayMatch[1]);
                            for (const item of arr) {
                                if (item.id && item.name) {
                                    const ym = item.name.match(/(20\\d{2})\\/(20\\d{2})/);
                                    if (ym) {
                                        seasons.push({
                                            year: parseInt(ym[1]),
                                            seasonId: String(item.id),
                                            stageId: null,
                                            text: item.name
                                        });
                                    }
                                }
                            }
                        } catch(e) {}
                    }
                }

                // Deduplicate by year
                const byYear = {};
                for (const s of seasons) {
                    if (!byYear[s.year] || s.stageId) {
                        byYear[s.year] = s;
                    }
                }
                return byYear;
            }
        """)

        # Build URL mapping
        url_map = {}
        for year_str, data in season_data.items():
            year = int(year_str)
            if data.get('href') and 'playerstatistics' in data['href'].lower():
                url_map[year] = data['href']
            elif data.get('seasonId') and data.get('stageId'):
                url_map[year] = f"{BASE_URL}/regions/{league_info['region']}/tournaments/{league_info['tournament']}/seasons/{data['seasonId']}/stages/{data['stageId']}/playerstatistics"
            elif data.get('seasonId'):
                # We have season ID but no stage ID - will need to discover it
                url_map[year] = f"{BASE_URL}/regions/{league_info['region']}/tournaments/{league_info['tournament']}/seasons/{data['seasonId']}"

        print(f"  Discovered {len(url_map)} seasons: {sorted(url_map.keys())}")
        return url_map

    async def _navigate_to_season_stats(self, league_code: str, year: int) -> bool:
        """
        Navigate directly to the player statistics page for a specific league season.
        Returns True if successfully navigated, False otherwise.
        """
        league_info = WHOSCORED_LEAGUES[league_code]

        # First, go to the current season's player stats page to discover season URLs
        url = f"{BASE_URL}/regions/{league_info['region']}/tournaments/{league_info['tournament']}"
        print(f"  Navigating to {url}")
        await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Handle cookie consent
        try:
            await self.page.click('button[id*="accept"], button[class*="accept"], #onetrust-accept-btn-handler', timeout=3000)
            await asyncio.sleep(1)
        except:
            pass

        # Find the player statistics link for the current season
        stats_url = await self.page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href]');
                for (const link of links) {
                    if (link.href.toLowerCase().includes('playerstatistics')) {
                        return link.href;
                    }
                }
                return null;
            }
        """)

        if not stats_url:
            # Try to construct from page data
            ids = await self.page.evaluate("""
                () => {
                    const html = document.documentElement.innerHTML;
                    const seasonMatch = html.match(/seasons\\/(\\d+)/i);
                    const stageMatch = html.match(/stages\\/(\\d+)/i);
                    return {
                        season: seasonMatch ? seasonMatch[1] : null,
                        stage: stageMatch ? stageMatch[1] : null
                    };
                }
            """)
            if ids['season'] and ids['stage']:
                stats_url = f"{BASE_URL}/regions/{league_info['region']}/tournaments/{league_info['tournament']}/seasons/{ids['season']}/stages/{ids['stage']}/playerstatistics"

        if not stats_url:
            print("  Could not find player statistics URL")
            return False

        # Navigate to player stats page
        print(f"  Found player statistics URL: {stats_url}")
        await self.page.goto(stats_url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Check if this is already the right season (URL slug contains year)
        current_url = self.page.url
        target_slug = f"{year}-{year+1}"
        if target_slug in current_url:
            print(f"  Already on correct season {year}/{year+1}")
            return True

        # Need to switch season - use the season dropdown on the player stats page
        print(f"  Need to switch to season {year}/{year+1}, current URL: {current_url}")

        # Try to use WhoScored's season selector on the stats page
        switched = await self.page.evaluate(f"""
            () => {{
                // Look for all select elements and find the season dropdown
                const selects = document.querySelectorAll('select');
                for (const sel of selects) {{
                    const options = sel.querySelectorAll('option');
                    for (const opt of options) {{
                        const text = opt.textContent.trim();
                        if (text.includes('{year}/{year+1}') || text.includes('{year}-{year+1}')) {{
                            sel.value = opt.value;
                            sel.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            return {{method: 'select', text: text, value: opt.value}};
                        }}
                    }}
                }}

                // Try clicking a season link directly
                const links = document.querySelectorAll('a[href]');
                for (const link of links) {{
                    const text = link.textContent.trim();
                    const href = link.href;
                    if ((text.includes('{year}/{year+1}') || href.includes('{year}-{year+1}')) &&
                        href.includes('playerstatistics')) {{
                        return {{method: 'link', href: href}};
                    }}
                }}

                // Look for season/stage IDs in page for this year
                const html = document.documentElement.innerHTML;
                const pattern = /seasons\\/(\\d+)\\/stages\\/(\\d+)[^"']*{year}-{year+1}/gi;
                const match = pattern.exec(html);
                if (match) {{
                    return {{method: 'url', seasonId: match[1], stageId: match[2]}};
                }}

                // Broader search - look for the year in URLs
                const pattern2 = new RegExp('seasons/(\\\\d+)/stages/(\\\\d+)[^"]*?{year}[^"]*?{year+1}', 'i');
                const match2 = pattern2.exec(html);
                if (match2) {{
                    return {{method: 'url', seasonId: match2[1], stageId: match2[2]}};
                }}

                return null;
            }}
        """)

        if switched:
            print(f"  Season switch result: {switched}")

            if switched['method'] == 'select':
                # Wait for page to reload after dropdown change
                await asyncio.sleep(5)
                # Verify we're on playerstatistics
                new_url = self.page.url
                if 'playerstatistics' not in new_url.lower():
                    # The dropdown may have navigated away - go back to stats
                    print(f"  Dropdown navigated to: {new_url}, need to find stats page")
                    stats_link = await self.page.evaluate("""
                        () => {
                            const links = document.querySelectorAll('a[href]');
                            for (const link of links) {
                                if (link.href.toLowerCase().includes('playerstatistics')) {
                                    return link.href;
                                }
                            }
                            return null;
                        }
                    """)
                    if stats_link:
                        await self.page.goto(stats_link, wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(3)
                return True

            elif switched['method'] == 'link':
                await self.page.goto(switched['href'], wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(3)
                return True

            elif switched['method'] == 'url':
                target_url = f"{BASE_URL}/regions/{league_info['region']}/tournaments/{league_info['tournament']}/seasons/{switched['seasonId']}/stages/{switched['stageId']}/playerstatistics"
                print(f"  Navigating to: {target_url}")
                await self.page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(3)
                return True

        print(f"  Could not switch to season {year}/{year+1}")
        return False

    async def scrape_player_defensive_stats(self, league_code: str, year: int):
        """
        Scrape player defensive statistics for a league season.

        Args:
            league_code: One of EPL, La_liga, Bundesliga, Serie_A, Ligue_1
            year: The starting year of the season (e.g., 2024 for 2024/25)
        """
        if league_code not in WHOSCORED_LEAGUES:
            print(f"League {league_code} not supported for WhoScored")
            return

        league_info = WHOSCORED_LEAGUES[league_code]
        print(f"Scraping WhoScored defensive stats for {league_code} {year}/{year+1}...")

        await self._start_browser()
        await asyncio.sleep(self.request_delay)

        # Get or create league/season in database
        league = self._get_league(league_code)
        if not league:
            league = League(name=league_code, display_name=league_code)
            self.session.add(league)
            self.session.commit()

        season = self._get_season(year, league)

        try:
            # Navigate to the correct season's player statistics page
            navigated = await self._navigate_to_season_stats(league_code, year)
            if not navigated:
                print(f"  SKIPPING: Could not navigate to {league_code} {year}/{year+1} stats")
                return

            print(f"  Player stats URL: {self.page.url}")

            # Switch to Defensive stats view
            await self._switch_to_defensive_stats()

            # Scrape all pages of player data
            all_players = await self._scrape_player_stats_table(league, season)

            print(f"  Scraped {len(all_players)} players with defensive stats")

        except Exception as e:
            print(f"  Error scraping {league_code}: {e}")
            import traceback
            traceback.print_exc()

    async def _switch_to_defensive_stats(self):
        """Switch the statistics view to show defensive stats"""
        try:
            # Click the Defensive tab link (href contains #stage-top-player-stats-defensive)
            # First, scroll the tab into view and click it
            clicked = await self.page.evaluate("""
                () => {
                    const links = document.querySelectorAll('a');
                    for (const link of links) {
                        if (link.textContent.trim() === 'Defensive' ||
                            link.href.includes('stage-top-player-stats-defensive')) {
                            link.scrollIntoView();
                            link.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)

            if clicked:
                print("  Clicked Defensive tab via JavaScript")
                await asyncio.sleep(3)  # Wait for table to reload

                # Wait for the defensive div to be visible and contain data
                await self.page.evaluate("""
                    () => {
                        const defensiveDiv = document.querySelector('#stage-top-player-stats-defensive');
                        if (defensiveDiv) {
                            defensiveDiv.style.display = 'block';
                        }
                    }
                """)

                # Verify we're on defensive stats by checking headers in the defensive container
                headers = await self.page.evaluate("""
                    () => {
                        // Look for headers in the defensive table specifically
                        const defensiveDiv = document.querySelector('#stage-top-player-stats-defensive');
                        if (defensiveDiv) {
                            const headerRow = defensiveDiv.querySelector('thead tr');
                            if (headerRow) {
                                return Array.from(headerRow.querySelectorAll('th')).map(th => th.textContent.trim().toLowerCase());
                            }
                        }
                        // Fallback to any visible header
                        const headerRow = document.querySelector('#player-table-statistics-head tr');
                        if (!headerRow) return [];
                        return Array.from(headerRow.querySelectorAll('th')).map(th => th.textContent.trim().toLowerCase());
                    }
                """)
                print(f"  Headers after switch: {headers}")
            else:
                print("  Warning: Could not switch to Defensive view, using default stats")

        except Exception as e:
            print(f"  Warning: Could not switch to defensive stats: {e}")

    async def _scrape_player_stats_table(self, league: League, season: Season) -> list:
        """Scrape the player statistics table"""
        all_players = []
        page_num = 1

        while True:
            print(f"    Scraping page {page_num}...")

            # Wait for table to load - check the defensive div first, then fallback
            table_found = await self.page.evaluate("""
                () => {
                    // First check if defensive tab is active and has a table
                    const defensiveDiv = document.querySelector('#stage-top-player-stats-defensive');
                    if (defensiveDiv) {
                        const tbody = defensiveDiv.querySelector('tbody');
                        if (tbody && tbody.querySelectorAll('tr').length > 0) {
                            return 'defensive';
                        }
                    }

                    // Fallback to main table
                    const mainTbody = document.querySelector('#player-table-statistics-body');
                    if (mainTbody && mainTbody.querySelectorAll('tr').length > 0) {
                        return 'main';
                    }

                    // Check any table
                    const anyTbody = document.querySelector('table tbody');
                    if (anyTbody && anyTbody.querySelectorAll('tr').length > 0) {
                        return 'any';
                    }

                    return null;
                }
            """)

            if not table_found:
                print("    Table not found with any selector")
                break

            # Extract player rows
            players_on_page = await self._extract_players_from_table(league, season)
            if not players_on_page:
                break

            all_players.extend(players_on_page)

            # Try to go to next page
            try:

                # WhoScored uses pagination links - look for "next" or ">" button
                has_next = await self.page.evaluate("""
                    () => {
                        // WhoScored defensive stats pagination is inside the defensive div
                        const defensiveDiv = document.querySelector('#stage-top-player-stats-defensive');

                        // Look for pagination within the defensive container first
                        let pagingContainer = defensiveDiv ? defensiveDiv.querySelector('[id*="paging"], [class*="paging"]') : null;

                        // Fallback to page-level pagination
                        if (!pagingContainer) {
                            pagingContainer = document.querySelector('#statistics-paging, [class*="paging"], .pagination');
                        }

                        if (pagingContainer) {
                            const links = pagingContainer.querySelectorAll('a');
                            for (const link of links) {
                                const text = link.textContent.trim();
                                // Look for "next" or ">" or "»"
                                if (text === '>' || text === 'Next' || text === '»' || text.toLowerCase() === 'next') {
                                    if (!link.classList.contains('disabled') && !link.hasAttribute('disabled')) {
                                        link.click();
                                        return true;
                                    }
                                }
                            }

                            // Try to find and click next page number
                            const currentPage = pagingContainer.querySelector('.current, .active, [class*="current"]');
                            if (currentPage) {
                                let nextEl = currentPage.nextElementSibling;
                                while (nextEl) {
                                    if (nextEl.tagName === 'A' && /^[0-9]+$/.test(nextEl.textContent.trim())) {
                                        nextEl.click();
                                        return true;
                                    }
                                    nextEl = nextEl.nextElementSibling;
                                }
                            }
                        }

                        // Last resort: find any link with just a number higher than 1
                        const allLinks = document.querySelectorAll('a');
                        for (const link of allLinks) {
                            const text = link.textContent.trim();
                            if (text === '2' && link.href.includes('page')) {
                                link.click();
                                return true;
                            }
                        }

                        return false;
                    }
                """)

                if has_next:
                    await asyncio.sleep(self.request_delay)
                    page_num += 1
                else:
                    print(f"    No more pages found")
                    break
            except Exception as e:
                print(f"    Pagination error: {e}")
                break

            # Safety limit
            if page_num > 50:
                break

        self.session.commit()
        return all_players

    async def _extract_players_from_table(self, league: League, season: Season) -> list:
        """Extract player data from the current page's table"""
        players = []

        try:
            # Try to extract via JavaScript - check defensive div first
            rows_data = await self.page.evaluate("""
                () => {
                    // First try the defensive stats container
                    let container = document.querySelector('#stage-top-player-stats-defensive');
                    let tbody = container ? container.querySelector('tbody') : null;

                    // Fallback to main table body
                    if (!tbody || tbody.querySelectorAll('tr').length === 0) {
                        tbody = document.querySelector('#player-table-statistics-body') ||
                                document.querySelector('#statistics-table-body') ||
                                document.querySelector('table tbody');
                    }

                    if (!tbody) {
                        return [];
                    }

                    const rows = tbody.querySelectorAll('tr');
                    const data = [];

                    rows.forEach((row, idx) => {
                        const cells = row.querySelectorAll('td');
                        if (cells.length > 3) {
                            // Player link - look for the actual player name link
                            const playerLink = row.querySelector('a.player-link');

                            // Team link - look for team meta data link or team info in the row
                            const teamMetaLink = row.querySelector('a[href*="/Teams/"]');

                            let playerId = null;
                            let playerName = null;

                            if (playerLink && playerLink.href) {
                                const match = playerLink.href.match(/Players\\/(\\d+)/i);
                                if (match) playerId = parseInt(match[1]);

                                // Get just the player name text
                                // Try to get text from the link itself, excluding child rank elements
                                let rawName = '';
                                const nameSpan = playerLink.querySelector('.iconize-icon-left, .player-name, span');
                                if (nameSpan) {
                                    rawName = nameSpan.textContent.trim();
                                } else {
                                    // Use direct text nodes only (skip rank number children)
                                    for (const node of playerLink.childNodes) {
                                        if (node.nodeType === Node.TEXT_NODE) {
                                            rawName += node.textContent;
                                        }
                                    }
                                    rawName = rawName.trim();
                                }
                                // Fallback: strip leading digits (rank number)
                                if (!rawName || /^\\d+$/.test(rawName)) {
                                    rawName = playerLink.textContent.trim();
                                }
                                playerName = rawName.replace(/^\\d+\\.?\\s*/, '').trim();
                            }

                            // Get team name from various possible locations
                            let teamName = null;

                            // Method 1: Direct team link (look for span.team-name inside)
                            if (teamMetaLink) {
                                const teamNameSpan = teamMetaLink.querySelector('.team-name');
                                if (teamNameSpan) {
                                    teamName = teamNameSpan.textContent.trim().replace(/,\s*$/, '');
                                } else {
                                    teamName = teamMetaLink.textContent.trim().replace(/,\s*$/, '');
                                }
                            }

                            // Method 2: Look for team in player-meta-data spans
                            if (!teamName) {
                                const metaSpans = row.querySelectorAll('span.player-meta-data');
                                for (const span of metaSpans) {
                                    const text = span.textContent.trim();
                                    // Team names don't have commas and aren't just numbers or ages
                                    if (text && !text.includes(',') && !/^\\d+$/.test(text) && text.length > 2) {
                                        teamName = text;
                                        break;
                                    }
                                }
                            }

                            // Method 3: Look for team icon with title or alt attribute
                            if (!teamName) {
                                const teamIcon = row.querySelector('img[title], span[title]');
                                if (teamIcon) {
                                    const title = teamIcon.getAttribute('title');
                                    if (title && title.length > 1) {
                                        teamName = title;
                                    }
                                }
                            }

                            // Method 4: Look for incident-icon or team badge
                            if (!teamName) {
                                const iconSpan = row.querySelector('.incident-icon, [class*="team"]');
                                if (iconSpan && iconSpan.getAttribute('title')) {
                                    teamName = iconSpan.getAttribute('title');
                                }
                            }

                            // Method 5: Extract from the player info cell structure
                            if (!teamName) {
                                const playerInfoCell = cells[0] || cells[1];
                                if (playerInfoCell) {
                                    // Look for any span/div that isn't the player link
                                    const elements = playerInfoCell.querySelectorAll('span, div');
                                    for (const el of elements) {
                                        if (!el.classList.contains('player-link') && !el.querySelector('.player-link')) {
                                            const text = el.textContent.trim();
                                            // Skip if it's the player name, age, or position
                                            if (text && text !== playerName &&
                                                !text.includes(playerName) &&
                                                !/^\\d+$/.test(text) &&
                                                !/^(GK|DF|MF|FW|AM|DM|LB|RB|CB|LW|RW|ST|CF)$/i.test(text) &&
                                                text.length > 2 && text.length < 30) {
                                                teamName = text;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            // Default if nothing found
                            if (!teamName) {
                                teamName = 'Unknown';
                            }

                            // Get stats from cells (skip first two which are player info)
                            const stats = Array.from(cells).slice(2).map(c => c.textContent.trim());

                            if (playerName && playerId) {
                                data.push({
                                    playerId: playerId,
                                    playerName: playerName,
                                    teamName: teamName,
                                    stats: stats
                                });
                            }
                        }
                    });
                    return data;
                }
            """)

            if rows_data and len(rows_data) > 0:
                print(f"    Extracted {len(rows_data)} players")

            # Get column headers to understand data structure - check defensive div first
            headers = await self.page.evaluate("""
                () => {
                    // First try defensive container
                    let container = document.querySelector('#stage-top-player-stats-defensive');
                    let headerRow = container ? container.querySelector('thead tr') : null;

                    // Fallback to main header
                    if (!headerRow) {
                        headerRow = document.querySelector('#player-table-statistics-head tr') ||
                                    document.querySelector('#player-table-statistics-header tr') ||
                                    document.querySelector('table thead tr');
                    }

                    if (!headerRow) return [];

                    // Skip first two columns (player info) to match stats array
                    const ths = Array.from(headerRow.querySelectorAll('th')).slice(2);
                    return ths.map(th => th.textContent.trim().toLowerCase());
                }
            """)

            # Map headers to stats
            header_map = self._create_header_map(headers)

            for row in rows_data:
                if not row.get('playerName') or not row.get('playerId'):
                    continue

                ws_player = self._get_or_create_whoscored_player(
                    row['playerId'],
                    row['playerName']
                )

                team = self._get_or_create_team(row.get('teamName', 'Unknown'), league)

                # Check if stats already exist
                existing = self.session.query(WhoScoredPlayerSeasonStats).filter_by(
                    player_id=ws_player.id,
                    season_id=season.id,
                    team_id=team.id
                ).first()

                if existing:
                    stats = existing
                else:
                    stats = WhoScoredPlayerSeasonStats(
                        player_id=ws_player.id,
                        team_id=team.id,
                        season_id=season.id
                    )
                    self.session.add(stats)

                # Parse stats based on headers
                stat_values = row.get('stats', [])
                self._populate_defensive_stats(stats, stat_values, header_map)

                players.append(ws_player)

        except Exception as e:
            print(f"    Error extracting players: {e}")

        return players

    def _create_header_map(self, headers: list) -> dict:
        """Create mapping from stat names to column indices"""
        mapping = {}
        # Map WhoScored column names to our stat names
        # Headers from WhoScored defensive: apps, mins, tackles, inter, fouls, offsides, clear, drb, blocks, owng, rating
        defensive_stats = {
            'tackles': ['tackles', 'tkl'],
            'interceptions': ['interceptions', 'inter', 'int'],
            'clearances': ['clearances', 'clear', 'clr'],
            'blocks': ['blocks', 'blk'],
            'aerial': ['aerial', 'aer', 'aerialswon'],
            'fouls': ['fouls'],
            'recoveries': ['recoveries', 'rec'],
            'apps': ['apps', 'appearances', 'mp'],
            'mins': ['mins', 'minutes', 'min'],
            'dribbles': ['drb', 'dribbles'],
            'offsides': ['offsides', 'off'],
            'own_goals': ['owng', 'own goals'],
            'rating': ['rating'],
        }

        for i, header in enumerate(headers):
            header_lower = header.lower().strip()
            for stat_name, keywords in defensive_stats.items():
                if header_lower in keywords or any(kw in header_lower for kw in keywords):
                    mapping[stat_name] = i
                    break

        return mapping

    def _populate_defensive_stats(self, stats: WhoScoredPlayerSeasonStats,
                                   values: list, header_map: dict):
        """Populate defensive stats from extracted values"""
        def get_val(key, default='0'):
            idx = header_map.get(key)
            if idx is not None and idx < len(values):
                return values[idx]
            return default

        # Parse apps - format might be '22(1)' meaning 22 starts, 1 sub
        apps_val = get_val('apps', '0')
        if '(' in str(apps_val):
            # Extract just the main number (starts)
            apps_val = apps_val.split('(')[0]
        stats.games = self._safe_int(apps_val)
        stats.minutes = self._safe_int(get_val('mins'))

        # WhoScored shows per-game averages for defensive stats
        # Store them as floats (per-game) - we can calculate totals if needed
        tackles_pg = self._safe_float(get_val('tackles'))
        inter_pg = self._safe_float(get_val('interceptions'))
        clear_pg = self._safe_float(get_val('clearances'))
        blocks_pg = self._safe_float(get_val('blocks'))
        fouls_pg = self._safe_float(get_val('fouls'))

        # Calculate estimated totals from per-game averages
        games = stats.games or 1
        stats.tackles = round(tackles_pg * games)
        stats.interceptions = round(inter_pg * games)
        stats.clearances = round(clear_pg * games)
        stats.blocks = round(blocks_pg * games)
        stats.fouls_committed = round(fouls_pg * games)

        # Store per-90 directly from per-game (approximate)
        # Per-90 = per-game * (90 / avg_mins_per_game)
        avg_mins_per_game = (stats.minutes / games) if games > 0 else 90
        mins_factor = 90.0 / avg_mins_per_game if avg_mins_per_game > 0 else 1.0

        stats.tackles_per_90 = round(tackles_pg * mins_factor, 2)
        stats.interceptions_per_90 = round(inter_pg * mins_factor, 2)
        stats.clearances_per_90 = round(clear_pg * mins_factor, 2)

        # Aerial duels - might be in a different column for this view
        aerial_val = get_val('aerial', '')
        if isinstance(aerial_val, str) and '/' in aerial_val:
            parts = aerial_val.split('/')
            stats.aerial_duels_won = self._safe_int(parts[0])
            stats.aerial_duels = self._safe_int(parts[1]) if len(parts) > 1 else stats.aerial_duels_won
        else:
            stats.aerial_duels_won = self._safe_int(aerial_val) if aerial_val else 0
            stats.aerial_duels = stats.aerial_duels_won

        if stats.aerial_duels and stats.aerial_duels > 0:
            stats.aerial_win_pct = round(100.0 * (stats.aerial_duels_won or 0) / stats.aerial_duels, 1)

    async def scrape_league_season(self, league_code: str, year: int):
        """Main entry point to scrape a league season"""
        await self.scrape_player_defensive_stats(league_code, year)

    async def scrape_all_leagues(self, year: int):
        """Scrape all supported leagues for a given season"""
        for league_code in WHOSCORED_LEAGUES.keys():
            try:
                await self.scrape_league_season(league_code, year)
            except Exception as e:
                print(f"Error scraping {league_code}: {e}")
                self.session.rollback()

    async def close(self):
        """Clean up resources"""
        await self._stop_browser()


async def main():
    scraper = WhoScoredScraper()

    try:
        # All supported leagues
        leagues = ["EPL", "La_liga", "Serie_A", "Bundesliga", "Ligue_1"]
        # Seasons from 2017-18 to 2025-26
        seasons = list(range(2017, 2026))  # 2017, 2018, ..., 2025

        total = len(leagues) * len(seasons)
        count = 0

        for year in seasons:
            for league_code in leagues:
                count += 1
                print(f"\n[{count}/{total}] ", end="")
                try:
                    await scraper.scrape_league_season(league_code, year)
                except Exception as e:
                    print(f"Error: {e}")
                    scraper.session.rollback()

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
