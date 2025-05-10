"""
FBref team history scraper module with enhanced fallback and error handling.

This module is responsible for scraping historical team data from FBref,
with improvements to handle multiple seasons and parse HTML more robustly.
"""

import os
import time
import re
import random
import pandas as pd
import logging
from datetime import datetime, timedelta
import concurrent.futures
from typing import Dict, List, Optional, Union, Any, Tuple
import requests
from requests.exceptions import RequestException, Timeout, HTTPError
from bs4 import BeautifulSoup
from utils.logger import PipelineLogger
from utils.http_utils import get_soup, make_request
from utils.data_utils import normalize_team_name, normalize_date, clean_number, generate_match_id
import config


class TeamHistoryScraper:
    """Scraper for team historical performance data from FBref."""
    
    def __init__(self, logger: PipelineLogger = None):
        """Initialize the team history scraper.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
        
        # Configure seasons to try (current season and previous two)
        self.seasons = self._get_seasons(3)  # Current + 2 previous seasons
        
        # Cache for team URLs to avoid redundant searches
        self.team_urls = {}
        
        # Create output directory if it doesn't exist
        os.makedirs(config.RAW_DIR, exist_ok=True)
    
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
        """Find the FBref URL for a team by name.
        
        Args:
            team_name: Team name to search for
            
        Returns:
            str or None: FBref team URL if found
        """
        if team_name in self.team_urls:
            return self.team_urls[team_name]
        
        normalized_name = normalize_team_name(team_name)
        search_name = normalized_name.replace(' ', '+')
        
        try:
            # Search for the team on FBref
            search_url = f"https://fbref.com/en/search/search.fcgi?search={search_name}"
            
            if self.logger:
                self.logger.info(f"Searching for team: {team_name}")
            
            soup = get_soup(search_url)
            
            # Look for team search results
            search_results = soup.select('.search-item-name')
            
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
                        
                        # Extract the team ID from the URL
                        team_id = link['href'].split('/squads/')[1].split('/')[0]
                        
                        if self.logger:
                            self.logger.info(f"Found team URL for {team_name}: {team_url} (ID: {team_id})")
                        
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
            if self.logger:
                self.logger.error(f"Error finding team URL for {team_name}: {str(e)}")
            
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
                    self.logger.info(f"Trying {season} fixtures URL: {url}")
                
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
    
    def scrape_team_history(
        self, 
        team_name: str, 
        lookback_matches: int = 7
    ) -> pd.DataFrame:
        """Scrape historical performance data for a team's most recent matches.
        
        Args:
            team_name: Team name to scrape history for
            lookback_matches: Number of most recent matches to retrieve (default: 7)
            
        Returns:
            pd.DataFrame: DataFrame containing team historical data
        """
        if self.logger:
            self.logger.info(f"Scraping {lookback_matches} most recent matches for team: {team_name}")
        
        # Find team URL
        team_url = self._find_team_url(team_name)
        
        if not team_url:
            if self.logger:
                self.logger.warning(f"Cannot scrape history for {team_name}: team URL not found")
            return pd.DataFrame()
        
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
    
    def scrape_teams_from_fixtures(
        self,
        fixtures_df: pd.DataFrame,
        max_workers: int = 4,
        lookback_matches: int = 7
    ) -> pd.DataFrame:
        """Scrape historical data for all teams in a fixtures DataFrame.
        
        Args:
            fixtures_df: DataFrame containing fixtures with home_team and away_team columns
            max_workers: Maximum number of concurrent scraping workers
            lookback_matches: Number of most recent matches to collect per team
            
        Returns:
            pd.DataFrame: Combined DataFrame containing all teams' historical data
        """
        if fixtures_df.empty:
            if self.logger:
                self.logger.warning("No fixtures provided for team history scraping")
            return pd.DataFrame()
        
        # Extract unique team names from fixtures
        home_teams = fixtures_df['home_team'].unique().tolist()
        away_teams = fixtures_df['away_team'].unique().tolist()
        all_teams = list(set(home_teams + away_teams))
        
        if self.logger:
            self.logger.start_job(f"FBref team history scraping for {len(all_teams)} teams")
            self.logger.info(f"Will collect {lookback_matches} most recent matches for each team")
        
        all_team_data = []
        
        # Use ThreadPoolExecutor for concurrent scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit scraping tasks for all teams
            future_to_team = {
                executor.submit(self.scrape_team_history, team, lookback_matches): team 
                for team in all_teams
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_team):
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
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Error processing team {team}: {str(e)}")
                
                # Add a random delay between requests to avoid rate limiting
                time.sleep(random.uniform(2, 5))
        
        if not all_team_data:
            if self.logger:
                self.logger.warning("No team history data was successfully scraped")
                self.logger.end_job("FBref team history scraping", {"teams_scraped": 0})
            return pd.DataFrame()
        
        # Combine all team data
        combined_df = pd.concat(all_team_data, ignore_index=True)
        
        # Save combined raw data
        raw_file = os.path.join(config.RAW_DIR, "raw_team_history_all.csv")
        combined_df.to_csv(raw_file, index=False)
        
        if self.logger:
            self.logger.info(f"Combined data for {len(all_team_data)} teams ({len(combined_df)} matches total)")
            self.logger.info(f"Raw combined data saved to {raw_file}")
            self.logger.end_job("FBref team history scraping", {
                "teams_scraped": len(all_team_data),
                "total_matches": len(combined_df)
            })
        
        return combined_df