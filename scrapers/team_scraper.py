"""
FBref team history scraper module with enhanced caching, fallbacks, and parallel processing.

This module is responsible for scraping historical team data from FBref,
with improvements to handle multiple seasons, parallel scraping, and robust caching.
"""

import os
import time
import re
import random
import json
import pandas as pd
import numpy as np
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
from tqdm.auto import tqdm
from typing import Dict, List, Optional, Union, Any, Tuple
import requests
from requests.exceptions import RequestException, Timeout, HTTPError
from bs4 import BeautifulSoup

from utils.logger import PipelineLogger
from utils.http_utils import get_soup, make_request
from utils.data_utils import normalize_team_name, normalize_date, clean_number, generate_match_id
import config


class TeamHistoryScraper:
    """Scraper for team historical performance data from FBref with enhanced capabilities."""
    
    # Add this as a class constant
    PRIORITY_LEAGUES = {
        "Premier League", 
        "La Liga", 
        "Serie A", 
        "Bundesliga", 
        "Ligue 1",
        "Champions League",
        "Europa League"
    }
    
    def __init__(self, logger: PipelineLogger = None, cache_dir=None):
        """Initialize the team history scraper.
        
        Args:
            logger: Logger instance
            cache_dir: Directory for caching team data
        """
        self.logger = logger
        
        # Configure seasons to try (current season and previous two)
        self.seasons = self._get_seasons(3)  # Current + 2 previous seasons
        
        # Cache for team URLs to avoid redundant searches
        self.team_urls = {}
        
        # Set up caching
        self.setup_cache(cache_dir)
        
        # Track failed teams
        self.failed_teams = set()
        
        # Create output directory if it doesn't exist
        os.makedirs(config.RAW_DIR, exist_ok=True)
    
    def setup_cache(self, cache_dir=None):
        """Set up the caching system.
        
        Args:
            cache_dir: Directory for caching team data (default: data/cache/team_history)
        """
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path(config.DATA_DIR) / "cache" / "team_history"
        
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        
        # Track teams that failed to be scraped
        self.failed_teams = set()
    
    def _get_cache_path(self, team_name):
        """Get the cache file path for a team."""
        safe_name = normalize_team_name(team_name).lower().replace(' ', '_')
        return self.cache_dir / f"{safe_name}.json"
    
    def _load_from_cache(self, team_name):
        """Load team history from cache if available."""
        cache_path = self._get_cache_path(team_name)
        
        if cache_path.exists():
            try:
                if self.logger:
                    self.logger.info(f"Loading {team_name} data from cache: {cache_path}")
                
                # Load cached JSON
                cached_data = json.loads(cache_path.read_text())
                
                # Check if it's valid
                if cached_data and isinstance(cached_data, list) and len(cached_data) > 0:
                    # Convert to DataFrame
                    df = pd.DataFrame(cached_data)
                    
                    # Check cache freshness (within 7 days)
                    cache_time = cache_path.stat().st_mtime
                    cache_age = datetime.now().timestamp() - cache_time
                    
                    if cache_age < 7 * 24 * 3600:  # 7 days in seconds
                        return df
                    else:
                        if self.logger:
                            self.logger.info(f"Cache for {team_name} is older than 7 days, will refresh")
                        return None
                
                if self.logger:
                    self.logger.warning(f"Invalid cache data for {team_name}")
                return None
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error loading cache for {team_name}: {str(e)}")
                return None
        
        return None
    
    def _save_to_cache(self, team_name, df):
        """Save team history to cache."""
        if df.empty:
            return False
        
        cache_path = self._get_cache_path(team_name)
        
        try:
            # Convert DataFrame to list of dicts for JSON
            data = df.to_dict(orient='records')
            
            # Save to cache
            cache_path.write_text(json.dumps(data, default=str))
            
            if self.logger:
                self.logger.info(f"Saved {team_name} data to cache: {cache_path}")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error saving cache for {team_name}: {str(e)}")
            return False
    
    def _get_seasons(self, num_seasons: int = 3) -> List[str]:
        """Get a list of seasons to try, starting with the current season.
        
        Args:
            num_seasons: Number of seasons to return
            
        Returns:
            List[str]: List of seasons in format "YYYY-YYYY"
        """
        today = datetime.today()
        
        # Football season typically starts in August
        if today.month >= 8:  # August or later
            current_season_start = today.year
        else:
            current_season_start = today.year - 1
            
        seasons = []
        for i in range(num_seasons):
            start_year = current_season_start - i
            seasons.append(f"{start_year}-{start_year + 1}")
        
        return seasons
    
    def _get_current_season(self) -> str:
        """Get the current football season.
        
        Returns:
            str: Current season in format "YYYY-YYYY"
        """
        return self.seasons[0] if self.seasons else "2024-2025"
    
    def _find_team_url(self, team_name: str) -> Optional[str]:
        """Find the FBref URL for a team by name with enhanced error handling.
    
        Args:
            team_name: Team name to search for
        
        Returns:
            str or None: FBref team URL if found
        """
        if team_name in self.team_urls:
            if self.logger:
                self.logger.info(f"Using cached URL for {team_name}: {self.team_urls[team_name]}")
            return self.team_urls[team_name]
    
        normalized_name = normalize_team_name(team_name)
        search_name = normalized_name.replace(' ', '+')
    
        if self.logger:
            self.logger.info(f"Searching for team: {team_name} (normalized: {normalized_name})")
    
        # Pre-defined URLs for common teams to avoid searching
        common_teams = {
            "Manchester United": "https://fbref.com/en/squads/19538871/Manchester-United-Stats",
            "Arsenal": "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
            "Liverpool": "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
            "Manchester City": "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
            "Chelsea": "https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats",
            "Barcelona": "https://fbref.com/en/squads/206d90db/Barcelona-Stats",
            "Real Madrid": "https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats",
            "Bayern Munich": "https://fbref.com/en/squads/054efa67/Bayern-Munich-Stats",
            "Paris Saint-Germain": "https://fbref.com/en/squads/e2d8892c/Paris-Saint-Germain-Stats",
            "Juventus": "https://fbref.com/en/squads/e0652b02/Juventus-Stats"
       }
    
        # Check if this is a common team with a known URL
        for common_name, url in common_teams.items():
            if normalized_name.lower() == normalize_team_name(common_name).lower():
                if self.logger:
                    self.logger.info(f"Found predefined URL for {team_name}: {url}")
                self.team_urls[team_name] = url
                return url
    
        try:
            # Search for the team on FBref
            search_url = f"https://fbref.com/en/search/search.fcgi?search={search_name}"
        
            if self.logger:
                self.logger.info(f"Making request to: {search_url}")
        
            # Use simpler approach with direct requests instead of get_soup
            import requests
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
           }
        
            response = requests.get(search_url, headers=headers, timeout=30)
        
            if response.status_code != 200:
                if self.logger:
                    self.logger.warning(f"Search request failed with status code: {response.status_code}")
                    self.logger.warning(f"Response content: {response.text[:500]}...")
                return None
        
            if self.logger:
                self.logger.info(f"Search request completed successfully")
        
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
        
            # Look for team search results
            search_results = soup.select('.search-item-name')
        
            if self.logger:
                self.logger.info(f"Found {len(search_results)} search results")
        
            for item in search_results:
                link = item.find('a')
                if link and '/squads/' in link['href']:
                    result_name = link.text.strip()
                
                    # Check if the found name matches the search name
                    if (normalized_name.lower() in result_name.lower() or 
                        result_name.lower() in normalized_name.lower()):
                        team_url = f"https://fbref.com{link['href']}"
                    
                        # Store in cache
                        self.team_urls[team_name] = team_url
                    
                        if self.logger:
                            self.logger.info(f"Found team URL for {team_name}: {team_url}")
                    
                        return team_url
        
            # If no direct match, try to find the closest match
            for item in search_results:
                link = item.find('a')
                if link and '/squads/' in link['href']:
                    result_name = link.text.strip()
                
                     # Take the first squad result as a fallback
                    team_url = f"https://fbref.com{link['href']}"
                
                     # Store in cache
                    self.team_urls[team_name] = team_url
                
                    if self.logger:
                        self.logger.info(f"Found fallback team URL for {team_name}: {team_url}")
                
                    return team_url
        
            if self.logger:
                self.logger.warning(f"No team URL found for {team_name}")
        
            return None
        
        except Exception as e:
            import traceback
            if self.logger:
                self.logger.error(f"Error finding team URL for {team_name}: {str(e)}")
                self.logger.error(traceback.format_exc())
        
            return None
    
    def _get_team_id_from_url(self, team_url: str) -> Optional[str]:
        """Extract the team ID from a team URL.
        
        Args:
            team_url: Team URL
            
        Returns:
            str or None: Team ID
        """
        if not team_url:
            return None
        
        # Extract the team ID from a URL like https://fbref.com/en/squads/17859612/...
        match = re.search(r'/squads/([a-zA-Z0-9]+)/', team_url)
        return match.group(1) if match else None
    
    def _parse_matchlog_table(self, html: str, team_name: str) -> Tuple[pd.DataFrame, List[str]]:
        """Parse the matchlog table from HTML.
        
        Args:
            html: HTML content
            team_name: Team name
            
        Returns:
            Tuple[pd.DataFrame, List[str]]: DataFrame with match data and list of match URLs
        """
        soup = BeautifulSoup(html, "html.parser")
        
        # Try several possible table IDs used by FBref
        table_ids = ["matchlogs", "matchlogs_for", "matchlogs_all", "stats_table"]
        
        table = None
        for table_id in table_ids:
            table = soup.find("table", id=table_id)
            if table:
                if self.logger:
                    self.logger.info(f"Found match table with id={table_id}")
                break
        
        if not table:
            if self.logger:
                self.logger.warning(f"No match table found for {team_name}")
            return pd.DataFrame(), []
        
        # Try to extract the table with pandas
        try:
            df_list = pd.read_html(str(table))
            if not df_list:
                if self.logger:
                    self.logger.warning(f"pandas.read_html returned no tables for {team_name}")
                return pd.DataFrame(), []
            
            df = df_list[0]
            
            # Check if the table is empty or malformed
            if df.empty or "Date" not in df.columns:
                if self.logger:
                    self.logger.warning(f"Match table for {team_name} is empty or missing Date column")
                return pd.DataFrame(), []
            
            # Clean up separator rows (FBref inserts blank <tr> for styling)
            df = df.dropna(subset=["Date"])
            
            # Handle multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # Extract match URLs
            match_urls = []
            for a in table.select("tbody tr td a"):
                href = a.get("href", "")
                # Only match pages have `/en/matches/`
                if "/en/matches/" in href:
                    full_url = f"https://fbref.com{href}"
                    match_urls.append(full_url)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_urls = []
            for url in match_urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            
            return df, unique_urls
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error parsing match table for {team_name}: {str(e)}")
            return pd.DataFrame(), []
    
    def _try_get_fixtures_for_season(self, team_url: str, team_name: str, season: str) -> Tuple[pd.DataFrame, List[str]]:
        """Try to get fixtures for a specific season.
        
        Args:
            team_url: Base team URL
            team_name: Team name
            season: Season to try (e.g., "2023-2024")
            
        Returns:
            Tuple[pd.DataFrame, List[str]]: DataFrame with fixtures and list of match URLs
        """
        # Extract team ID from the URL
        team_id = self._get_team_id_from_url(team_url)
        if not team_id:
            if self.logger:
                self.logger.warning(f"Could not extract team ID for {team_name}")
            return pd.DataFrame(), []
        
        # Try different URL patterns
        url_patterns = [
            # All competitions
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-All-Competitions",
            # Basic URL without team name
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps/schedule/Scores-and-Fixtures-All-Competitions",
            # Try with just the ID
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps/schedule/{team_id}-Scores-and-Fixtures",
            # For some teams, the URL uses a specific competition ID
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/c9/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-Champions-League",
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/c12/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-La-Liga",
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/c13/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-Ligue-1",
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/c11/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-Serie-A",
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/c20/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-Bundesliga",
            f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/c8/schedule/{team_name.replace(' ', '-')}-Scores-and-Fixtures-Premier-League",
        ]
        
        # Try each URL pattern
        for url in url_patterns:
            try:
                if self.logger:
                    self.logger.info(f"Fetching fixtures from: {url}")
                
                html = make_request(url).text
                df, match_urls = self._parse_matchlog_table(html, team_name)
                
                if not df.empty:
                    if self.logger:
                        self.logger.info(f"Found {len(df)} matches for {team_name} in season {season}")
                    return df, match_urls
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Failed to fetch {url}: {str(e)}")
                continue
        
        if self.logger:
            self.logger.warning(f"No fixtures found for {team_name} in season {season}")
        
        return pd.DataFrame(), []
    
    def _extract_match_stats(self, match_url: str, team_name: str) -> Dict[str, Any]:
        """Extract detailed match statistics from a match URL.
        
        Args:
            match_url: URL to the match page
            team_name: Name of the team we're collecting data for
            
        Returns:
            Dict[str, Any]: Dictionary of match statistics
        """
        if self.logger:
            self.logger.info(f"Scraping match details from: {match_url}")
        
        try:
            # Get the match page
            html = make_request(match_url).text
            soup = BeautifulSoup(html, "html.parser")
            
            # Initialize stats dictionary
            stats = {}
            
            # Extract team names to determine which team is ours
            teams = []
            team_divs = soup.select('div.scorebox > div')
            for div in team_divs[:2]:  # First two divs are typically the teams
                team_header = div.select_one('strong a')
                if team_header:
                    teams.append(team_header.text.strip())
            
            if len(teams) != 2:
                return stats
            
            # Determine which team index is ours (0 for home, 1 for away)
            normalized_team_name = normalize_team_name(team_name)
            our_team_idx = -1
            for i, t in enumerate(teams):
                if normalize_team_name(t) == normalized_team_name:
                    our_team_idx = i
                    break
            
            if our_team_idx == -1:
                if self.logger:
                    self.logger.warning(f"Could not identify our team ({team_name}) on match page")
                return stats
            
            opponent_idx = 1 if our_team_idx == 0 else 0
            
            # Extract expected goals (xG)
            xg_divs = soup.select('div.scorebox_meta strong')
            for div in xg_divs:
                if 'xG' in div.text:
                    xg_text = div.text.strip()
                    xg_values = re.findall(r'([0-9.]+)', xg_text)
                    if len(xg_values) >= 2:
                        stats['xg'] = float(xg_values[our_team_idx])
                        stats['xg_against'] = float(xg_values[opponent_idx])
                    break
            
            # Define tables and stats to extract
            tables = {
                "possession": ["Possession"],
                "passing": ["Total Passes", "Pass Completion %"],
                "shooting": ["Shots", "Shots on Target", "Big Chances Created"],
                "misc": ["Corners", "Fouls Committed", "Yellow Cards", "Red Cards"]
            }
            
            # Extract stats from each table
            for table_id, stat_names in tables.items():
                table = soup.find("table", id=table_id)
                if not table:
                    continue
                
                try:
                    df_list = pd.read_html(str(table))
                    if not df_list:
                        continue
                    
                    df = df_list[0]
                    
                    # For tables with multiindex columns, flatten them
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = [' '.join(col).strip() for col in df.columns.values]
                    
                    # Set first column as index
                    if len(df.columns) >= 3:  # Typical format: Stat Name | Home | Away
                        stat_col = df.columns[0]
                        home_col = df.columns[1]
                        away_col = df.columns[2]
                        
                        # Set stat name column as index
                        df = df.set_index(stat_col)
                        
                        # Extract stats we're interested in
                        for name in stat_names:
                            if name in df.index:
                                our_col = home_col if our_team_idx == 0 else away_col
                                opp_col = away_col if our_team_idx == 0 else home_col
                                
                                # Clean the values
                                our_value = df.loc[name, our_col]
                                opp_value = df.loc[name, opp_col]
                                
                                # Remove % signs and convert to float
                                if isinstance(our_value, str) and '%' in our_value:
                                    our_value = our_value.replace('%', '')
                                if isinstance(opp_value, str) and '%' in opp_value:
                                    opp_value = opp_value.replace('%', '')
                                
                                # Store in stats dictionary
                                key = name.lower().replace(' ', '_')
                                stats[key] = clean_number(our_value)
                                stats[f'opponent_{key}'] = clean_number(opp_value)
                
                except Exception as table_error:
                    if self.logger:
                        self.logger.warning(f"Error parsing {table_id} table: {str(table_error)}")
                    continue
            
            return stats
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error scraping match details from {match_url}: {str(e)}")
            return {}
    
    def _detailed_scrape_team_history(self, team_name: str, team_url: str, lookback_matches: int = 7) -> pd.DataFrame:
        """Detailed team history scrape with match statistics.
        
        Args:
            team_name: Team name to scrape
            team_url: Team URL
            lookback_matches: Number of most recent matches to retrieve
            
        Returns:
            pd.DataFrame: Team history DataFrame
        """
        # Try each season until we find matches
        for season in self.seasons:
            try:
                fixtures_df, match_urls = self._try_get_fixtures_for_season(team_url, team_name, season)
                
                if fixtures_df.empty:
                    continue
                
                # Limit to most recent N matches
                fixtures_df = fixtures_df.head(lookback_matches)
                
                # Also limit match URLs
                match_urls = match_urls[:min(len(fixtures_df), len(match_urls))]
                
                # Create standardized DataFrame
                matches_data = []
                
                for idx, row in fixtures_df.iterrows():
                    try:
                        # Extract basic match info
                        match_data = {
                            'team': normalize_team_name(team_name),
                            'season': season
                        }
                        
                        # Map FBref column names to our standardized names
                        # Handle common column name variations
                        date_col = next((c for c in row.index if 'Date' in c), None)
                        comp_col = next((c for c in row.index if 'Comp' in c), None)
                        venue_col = next((c for c in row.index if 'Venue' in c), None)
                        opponent_col = next((c for c in row.index if 'Opponent' in c), None)
                        result_col = next((c for c in row.index if 'Result' in c), None)
                        score_col = next((c for c in row.index if 'Score' in c), None)
                        gf_col = next((c for c in row.index if any(x in c for x in ['GF', 'Goals For'])), None)
                        ga_col = next((c for c in row.index if any(x in c for x in ['GA', 'Goals Against'])), None)
                        
                        if date_col:
                            match_data['date'] = normalize_date(row[date_col])
                        
                        if comp_col:
                            match_data['competition'] = row[comp_col]
                        
                        if venue_col:
                            match_data['venue'] = row[venue_col]  # Home or Away
                        
                        if opponent_col:
                            match_data['opponent'] = normalize_team_name(row[opponent_col])
                        
                        if result_col:
                            match_data['result'] = row[result_col]  # W, D, L
                        
                        # Extract goals
                        if gf_col:
                            match_data['goals_for'] = clean_number(row[gf_col])
                        
                        if ga_col:
                            match_data['goals_against'] = clean_number(row[ga_col])
                        
                        # Try to extract from score if goals columns not found
                        if score_col and '-' in str(row[score_col]) and ('goals_for' not in match_data or 'goals_against' not in match_data):
                            parts = str(row[score_col]).split('-')
                            if len(parts) == 2:
                                try:
                                    if match_data.get('venue', '') == 'Home':
                                        match_data['goals_for'] = int(parts[0].strip())
                                        match_data['goals_against'] = int(parts[1].strip())
                                    else:
                                        match_data['goals_for'] = int(parts[1].strip())
                                        match_data['goals_against'] = int(parts[0].strip())
                                except ValueError:
                                    pass
                        
                        # Add home/away flag based on venue
                        if 'venue' in match_data:
                            is_home = match_data['venue'] == 'Home'
                            match_data['is_home'] = 1 if is_home else 0
                            
                            if 'opponent' in match_data:
                                if is_home:
                                    match_data['home_team'] = team_name
                                    match_data['away_team'] = match_data['opponent']
                                else:
                                    match_data['home_team'] = match_data['opponent']
                                    match_data['away_team'] = team_name
                        
                        # Generate a match ID
                        if 'date' in match_data and 'home_team' in match_data and 'away_team' in match_data:
                            match_data['match_id'] = generate_match_id(
                                match_data['date'],
                                match_data['home_team'],
                                match_data['away_team']
                            )
                        
                        # Add match URL if available
                        if idx < len(match_urls):
                            match_data['match_url'] = match_urls[idx]
                        
                        matches_data.append(match_data)
                        
                    except Exception as row_error:
                        if self.logger:
                            self.logger.error(f"Error processing match row for {team_name}: {str(row_error)}")
                        continue
                
                # Create DataFrame from match data
                team_df = pd.DataFrame(matches_data)
                
                if team_df.empty:
                    if self.logger:
                        self.logger.warning(f"No valid matches found for {team_name} in season {season}")
                    continue
                
                # Now fetch detailed match statistics for each match
                for idx, row in team_df.iterrows():
                    if 'match_url' in row and row['match_url']:
                        try:
                            # Add a random delay to avoid rate limiting
                            time.sleep(random.uniform(1, 2))
                            
                            # Fetch detailed match stats
                            detailed_stats = self._extract_match_stats(row['match_url'], team_name)
                            
                            if detailed_stats:
                                # Update with detailed stats
                                for key, value in detailed_stats.items():
                                    team_df.at[idx, key] = value
                                    
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"Error scraping detailed stats for match: {str(e)}")
                
                # Save raw data
                raw_file = os.path.join(
                    config.RAW_DIR, 
                    f"raw_team_history_{team_name.replace(' ', '_').lower()}.csv"
                )
                team_df.to_csv(raw_file, index=False)
                
                if self.logger:
                    self.logger.info(f"Scraped {len(team_df)} matches for {team_name} (season {season})")
                    self.logger.info(f"Raw data saved to {raw_file}")
                
                return team_df
                
            except Exception as season_error:
                if self.logger:
                    self.logger.error(f"Error scraping {team_name} for season {season}: {str(season_error)}")
                continue
        
        # If we get here, we couldn't find any matches across all seasons
        if self.logger:
            self.logger.warning(f"No matches found for {team_name} across any season")
        
        return pd.DataFrame()
    
    def _basic_scrape_team_history(self, team_name, team_url, lookback_matches=7):
        """Basic team history scrape without detailed match stats."""
        if self.logger:
            self.logger.info(f"Performing basic scrape for {team_name}")
        
        # Try each season until we find matches
        for season in self.seasons:
            try:
                fixtures_df, _ = self._try_get_fixtures_for_season(team_url, team_name, season)
                
                if fixtures_df.empty:
                    continue
                
                # Limit to most recent N matches and extract basic data
                matches_data = []
                for idx, row in fixtures_df.head(lookback_matches).iterrows():
                    match_data = {
                        'team': normalize_team_name(team_name),
                        'season': season
                    }
                    
                    # Extract basic info using common column patterns
                    for col_type, patterns in {
                        'date': ['Date'],
                        'competition': ['Comp', 'Competition'],
                        'venue': ['Venue'],
                        'opponent': ['Opponent'],
                        'result': ['Result'],
                        'goals_for': ['GF', 'Goals For'],
                        'goals_against': ['GA', 'Goals Against']
                    }.items():
                        col = next((c for c in row.index if any(p in c for p in patterns)), None)
                        if col:
                            if col_type == 'date':
                                match_data[col_type] = normalize_date(row[col])
                            else:
                                match_data[col_type] = row[col]
                   # Add home/away flag and team names
                    if 'venue' in match_data and 'opponent' in match_data:
                        is_home = match_data['venue'] == 'Home'
                        match_data['is_home'] = 1 if is_home else 0
                        match_data['home_team'] = team_name if is_home else match_data['opponent']
                        match_data['away_team'] = match_data['opponent'] if is_home else team_name
                        
                        # Generate match ID
                        if 'date' in match_data:
                            match_data['match_id'] = generate_match_id(
                                match_data['date'],
                                match_data['home_team'],
                                match_data['away_team']
                            )
                    
                    matches_data.append(match_data)
                
                if matches_data:
                    return pd.DataFrame(matches_data)
                    
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Basic scrape failed for {team_name} in season {season}: {str(e)}")
        
        return pd.DataFrame()

    def _retry_with_basic_scrape(self, team_name, lookback_matches):
        """Retry a failed team with basic scraping approach."""
        team_url = self._find_team_url(team_name)
        if not team_url:
            return pd.DataFrame()
        
        # Use only basic scrape for retry
        df = self._basic_scrape_team_history(team_name, team_url, lookback_matches)
        
        if not df.empty:
            self._save_to_cache(team_name, df)
        
        return df
    
    def scrape_team_history(self, team_name, lookback_matches=7):
        """Scrape team history with caching and fallback options."""
        if self.logger:
            self.logger.info(f"Scraping {lookback_matches} most recent matches for team: {team_name}")
        
        # Check if we have cached data
        cached_df = self._load_from_cache(team_name)
        if cached_df is not None and not cached_df.empty:
            if self.logger:
                self.logger.info(f"Using cached data for {team_name} ({len(cached_df)} matches)")
            return cached_df
        
        # Find team URL
        team_url = self._find_team_url(team_name)
        
        if not team_url:
            if self.logger:
                self.logger.warning(f"Cannot scrape history for {team_name}: team URL not found")
            self.failed_teams.add(team_name)
            return pd.DataFrame()
        
        # Attempt detailed scraping first
        try:
            df = self._detailed_scrape_team_history(team_name, team_url, lookback_matches)
            
            if not df.empty:
                # Cache the successful result
                self._save_to_cache(team_name, df)
                return df
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Detailed scrape failed for {team_name}: {str(e)}. Trying basic scrape.")
        
        # Fallback to basic scrape
        try:
            df = self._basic_scrape_team_history(team_name, team_url, lookback_matches)
            
            if not df.empty:
                # Cache the basic result
                self._save_to_cache(team_name, df)
                return df
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"All scraping methods failed for {team_name}: {str(e)}")
            self.failed_teams.add(team_name)
        
        return pd.DataFrame()
    
    def filter_priority_teams(self, fixtures_df):
        """Filter fixtures to only include teams from priority leagues."""
        # Get teams from priority leagues
        priority_fixtures = fixtures_df[fixtures_df['league'].isin(self.PRIORITY_LEAGUES)]
        
        # Extract unique teams
        home_teams = priority_fixtures['home_team'].unique().tolist()
        away_teams = priority_fixtures['away_team'].unique().tolist()
        
        return list(set(home_teams + away_teams))
    
    def send_notification(self, message, webhook_url=None):
        """Send a notification about scraping progress/completion."""
        if not webhook_url:
            # Try to get from environment
            webhook_url = os.environ.get('SLACK_WEBHOOK_URL') or os.environ.get('DISCORD_WEBHOOK_URL')
        
        if not webhook_url:
            if self.logger:
                self.logger.info(f"Notification: {message}")
            return False
        
        try:
            payload = {"text": message}
            response = requests.post(webhook_url, json=payload)
            return response.status_code == 200
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to send notification: {str(e)}")
            return False
    
    def scrape_teams_from_fixtures(
        self,
        fixtures_df: pd.DataFrame,
        max_workers: int = 4,
        lookback_matches: int = 7,
        max_teams: int = None,
        priority_only: bool = False,
        retry_failed: bool = True
    ) -> pd.DataFrame:
        """Scrape historical data for teams with enhanced controls and tracking.
        
        Args:
            fixtures_df: DataFrame with fixtures
            max_workers: Maximum concurrent workers
            lookback_matches: Number of matches to retrieve per team
            max_teams: Maximum number of teams to process (None for all)
            priority_only: Only process teams from priority leagues
            retry_failed: Retry teams that failed during first pass
            
        Returns:
            pd.DataFrame: Combined DataFrame with team history
        """
        if fixtures_df.empty:
            if self.logger:
                self.logger.warning("No fixtures provided for team history scraping")
            return pd.DataFrame()
        
        # Extract teams from fixtures
        home_teams = fixtures_df['home_team'].unique().tolist()
        away_teams = fixtures_df['away_team'].unique().tolist()
        all_teams = list(set(home_teams + away_teams))
        
        # Filter to priority leagues if requested
        if priority_only:
            priority_teams = self.filter_priority_teams(fixtures_df)
            team_list = [t for t in all_teams if t in priority_teams]
            if self.logger:
                self.logger.info(f"Filtered to {len(team_list)} priority league teams out of {len(all_teams)} total")
        else:
            team_list = all_teams
        
        # Limit number of teams if specified
        if max_teams and max_teams < len(team_list):
            team_list = team_list[:max_teams]
            if self.logger:
                self.logger.info(f"Limited to {max_teams} teams")
        
        if self.logger:
            self.logger.start_job(f"Team history scraping for {len(team_list)} teams")
            self.logger.info(f"Will collect {lookback_matches} most recent matches for each team")
        
        # Initialize progress tracking
        all_team_data = []
        self.failed_teams = set()
        
        # Use ThreadPoolExecutor with progress bar
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit scraping tasks
            future_to_team = {
                executor.submit(self.scrape_team_history, team, lookback_matches): team 
                for team in team_list
            }
            
            # Show progress bar
            for future in tqdm(concurrent.futures.as_completed(future_to_team), 
                              total=len(future_to_team), 
                              desc="Scraping Teams"):
                team = future_to_team[future]
                try:
                    team_df = future.result()
                    if not team_df.empty:
                        all_team_data.append(team_df)
                        if self.logger:
                            self.logger.info(f"Successfully scraped data for {team} ({len(team_df)} matches)")
                    else:
                        if self.logger:
                            self.logger.warning(f"No data scraped for team: {team}")
                        self.failed_teams.add(team)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Error processing team {team}: {str(e)}")
                    self.failed_teams.add(team)
                
                # Random delay between requests
                time.sleep(random.uniform(1, 3))
        
        # Retry failed teams if requested
        if retry_failed and self.failed_teams:
            if self.logger:
                self.logger.info(f"Retrying {len(self.failed_teams)} failed teams with basic scraping")
            
            retry_teams = list(self.failed_teams)
            self.failed_teams = set()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, max_workers // 2)) as retry_executor:
                retry_futures = {
                    retry_executor.submit(self._retry_with_basic_scrape, team, lookback_matches): team 
                    for team in retry_teams
                }
                
                for future in tqdm(concurrent.futures.as_completed(retry_futures), 
                                  total=len(retry_futures), 
                                  desc="Retrying Failed Teams"):
                    team = retry_futures[future]
                    try:
                        team_df = future.result()
                        if not team_df.empty:
                            all_team_data.append(team_df)
                            if self.logger:
                                self.logger.info(f"Retry success for {team} ({len(team_df)} matches)")
                        else:
                            if self.logger:
                                self.logger.warning(f"Retry failed for team: {team}")
                            self.failed_teams.add(team)
                    except Exception as e:
                        if self.logger:
                            self.logger.error(f"Retry error for team {team}: {str(e)}")
                        self.failed_teams.add(team)
                    
                    time.sleep(random.uniform(2, 4))
        
        # Process results
        if not all_team_data:
            if self.logger:
                self.logger.warning("No team history data was successfully scraped")
                self.logger.end_job("Team history scraping", {"teams_scraped": 0})
            return pd.DataFrame()
        
        # Combine all team data
        combined_df = pd.concat(all_team_data, ignore_index=True)
        
        # Save raw combined data
        raw_file = os.path.join(config.RAW_DIR, "raw_team_history_all.csv")
        combined_df.to_csv(raw_file, index=False)
        
        if self.logger:
            success_rate = 100 * (len(team_list) - len(self.failed_teams)) / len(team_list)
            self.logger.info(f"Combined data for {len(all_team_data)} teams ({len(combined_df)} matches total)")
            self.logger.info(f"Success rate: {success_rate:.1f}% ({len(team_list) - len(self.failed_teams)}/{len(team_list)})")
            self.logger.info(f"Raw combined data saved to {raw_file}")
            
            if self.failed_teams:
                self.logger.warning(f"Failed to scrape {len(self.failed_teams)} teams: {', '.join(sorted(list(self.failed_teams)[:10]))}...")
            
            self.logger.end_job("Team history scraping", {
                "teams_scraped": len(all_team_data),
                "total_matches": len(combined_df),
                "success_rate": f"{success_rate:.1f}%"
            })
        
        return combined_df