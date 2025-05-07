"""
FBref team history scraper module.

This module is responsible for scraping historical team data from FBref.
"""

import os
import time
import re
import random
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
from typing import Dict, List, Optional, Union, Any

from logger import PipelineLogger
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
        self.season = self._get_current_season()
        
        # Cache for team URLs to avoid redundant searches
        self.team_urls = {}
        
        # Create output directory if it doesn't exist
        os.makedirs(config.RAW_DIR, exist_ok=True)
    
    def _get_current_season(self) -> str:
        """Get the current football season based on the current date.
        
        Returns:
            str: Current season in format "YYYY-YYYY"
        """
        today = datetime.today()
        
        # Football season typically starts in August
        if today.month >= 8:  # August or later
            season_start = today.year
        else:
            season_start = today.year - 1
            
        season_end = season_start + 1
        
        return f"{season_start}-{season_end}"
    
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
            if self.logger:
                self.logger.error(f"Error finding team URL for {team_name}: {str(e)}")
            
            return None
    
    def scrape_team_history(
        self, 
        team_name: str, 
        lookback_days: int = None
    ) -> pd.DataFrame:
        """Scrape historical performance data for a team.
        
        Args:
            team_name: Team name to scrape history for
            lookback_days: Number of days to look back (default from config)
            
        Returns:
            pd.DataFrame: DataFrame containing team historical data
        """
        if lookback_days is None:
            lookback_days = config.TEAM_HISTORY_DAYS
        
        if self.logger:
            self.logger.info(f"Scraping history for team: {team_name} (last {lookback_days} days)")
        
        # Find team URL
        team_url = self._find_team_url(team_name)
        
        if not team_url:
            if self.logger:
                self.logger.warning(f"Cannot scrape history for {team_name}: team URL not found")
            return pd.DataFrame()
        
        try:
            # Get the team page
            soup = get_soup(team_url)
            
            # Find the link to the team's 'Scores & Fixtures' page
            fixtures_link = None
            
            for link in soup.select('a'):
                if link.text and 'Scores & Fixtures' in link.text:
                    fixtures_link = link['href']
                    break
            
            if not fixtures_link:
                if self.logger:
                    self.logger.warning(f"Cannot find Scores & Fixtures link for {team_name}")
                return pd.DataFrame()
            
            # Get the team's fixtures page
            fixtures_url = f"https://fbref.com{fixtures_link}"
            
            if self.logger:
                self.logger.info(f"Fetching fixtures from: {fixtures_url}")
            
            # Load the fixtures page
            fixtures_soup = get_soup(fixtures_url)
            
            # Find the fixtures table
            matches_table = fixtures_soup.select_one('table#matchlogs_for')
            
            if not matches_table:
                matches_table = fixtures_soup.select_one('table#matchlogs_all')
            
            if not matches_table:
                if self.logger:
                    self.logger.warning(f"Cannot find matches table for {team_name}")
                return pd.DataFrame()
            
            # Get column names - handle colspan with proper headers
            header_rows = matches_table.select('thead tr')
            
            if not header_rows:
                if self.logger:
                    self.logger.warning(f"Cannot find table headers for {team_name}")
                return pd.DataFrame()
            
            # Process complex headers (FBref uses multi-level headers)
            headers = []
            main_headers = []
            
            for row in header_rows:
                cells = row.select('th')
                if cells:
                    # If this is the first row with non-empty cells, treat as main headers
                    if not main_headers:
                        main_headers = [cell.text.strip() for cell in cells]
                    else:
                        # This is a sub-header row
                        sub_headers = []
                        cell_idx = 0
                        
                        for cell in cells:
                            col_name = cell.text.strip()
                            
                            # Handle colspans
                            colspan = int(cell.get('colspan', 1))
                            
                            # If we have a colspan > 1, use the main header as a prefix
                            if colspan > 1 and cell_idx < len(main_headers):
                                prefix = main_headers[cell_idx]
                                for _ in range(colspan):
                                    sub_headers.append(f"{prefix}_{col_name}")
                            else:
                                sub_headers.append(col_name)
                            
                            cell_idx += colspan
                        
                        headers = sub_headers
            
            # If no complex headers, use the main headers
            if not headers:
                headers = main_headers
            
            # Clean the header names
            headers = [re.sub(r'\s+', '_', h.lower()) for h in headers]
            
            # Get the match rows
            match_rows = matches_table.select('tbody tr')
            
            matches_data = []
            
            # Calculate the date threshold based on lookback days
            date_threshold = datetime.now() - timedelta(days=lookback_days)
            date_threshold_str = date_threshold.strftime('%Y-%m-%d')
            
            for row in match_rows:
                # Skip non-match rows (sometimes used for section headings)
                if 'class' in row.attrs and 'spacer' in row['class']:
                    continue
                
                # Skip divider rows
                if not row.select('th, td'):
                    continue
                
                cells = row.select('th, td')
                
                if len(cells) != len(headers):
                    # Skip rows with incorrect number of cells
                    continue
                
                match_data = {}
                
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        # Extract text content
                        value = cell.text.strip()
                        
                        # If this is the date column, parse and check against threshold
                        if headers[i] == 'date' and value:
                            try:
                                match_date = normalize_date(value)
                                if match_date < date_threshold_str:
                                    # Skip matches outside the lookback period
                                    break
                                match_data[headers[i]] = match_date
                            except:
                                match_data[headers[i]] = value
                        else:
                            match_data[headers[i]] = value
                            
                            # Extract xG values which are sometimes in data-stat attributes
                            if 'data-stat' in cell.attrs:
                                stat_name = cell['data-stat'].lower()
                                if 'xg' in stat_name and stat_name not in match_data:
                                    match_data[stat_name] = value
                
                # Only add if we have a valid date within the threshold
                if match_data and 'date' in match_data:
                    # Add the normalized team name
                    match_data['team'] = normalize_team_name(team_name)
                    
                    # Extract opponents
                    if 'opponent' in match_data:
                        match_data['opponent'] = normalize_team_name(match_data['opponent'])
                    
                    # Add whether the match is home or away
                    if 'venue' in match_data:
                        match_data['is_home'] = 1 if match_data['venue'] == 'Home' else 0
                    
                    # Generate a match ID
                    if 'date' in match_data and 'opponent' in match_data:
                        date = match_data['date']
                        home_team = team_name if match_data.get('is_home', 0) == 1 else match_data['opponent']
                        away_team = match_data['opponent'] if match_data.get('is_home', 0) == 1 else team_name
                        match_data['match_id'] = generate_match_id(date, home_team, away_team)
                    
                    matches_data.append(match_data)
            
            # Convert to DataFrame
            if not matches_data:
                if self.logger:
                    self.logger.warning(f"No match data found for {team_name} within lookback period")
                return pd.DataFrame()
            
            team_df = pd.DataFrame(matches_data)
            
            # Clean numeric columns
            numeric_cols = ['gf', 'ga', 'xg', 'xga', 'poss', 'sh', 'sot', 'dist', 'fk', 'pk', 'pkatt']
            
            for col in team_df.columns:
                if any(nc in col.lower() for nc in numeric_cols):
                    team_df[col] = team_df[col].apply(clean_number)
            
            # Rename columns for consistency
            col_mapping = {
                'gf': 'goals_for',
                'ga': 'goals_against',
                'xg': 'expected_goals',
                'xga': 'expected_goals_against',
                'sh': 'shots',
                'sot': 'shots_on_target',
                'dist': 'distance_covered',
                'fk': 'free_kicks',
                'pk': 'penalties_scored',
                'pkatt': 'penalties_attempted',
                'poss': 'possession'
            }
            
            for old_col, new_col in col_mapping.items():
                for col in team_df.columns:
                    if col.lower() == old_col or col.lower().endswith(f'_{old_col}'):
                        team_df = team_df.rename(columns={col: new_col})
            
            # Save raw data
            raw_file = os.path.join(
                config.RAW_DIR, 
                f"raw_team_history_{team_name.replace(' ', '_').lower()}.csv"
            )
            team_df.to_csv(raw_file, index=False)
            
            if self.logger:
                self.logger.info(f"Scraped {len(team_df)} matches for {team_name}")
                self.logger.info(f"Raw data saved to {raw_file}")
            
            return team_df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error scraping history for {team_name}: {str(e)}", exc_info=True)
            
            return pd.DataFrame()
    
    def scrape_teams_from_fixtures(
        self,
        fixtures_df: pd.DataFrame,
        max_workers: int = 4,
        lookback_days: int = None
    ) -> pd.DataFrame:
        """Scrape historical data for all teams in a fixtures DataFrame.
        
        Args:
            fixtures_df: DataFrame containing fixtures with home_team and away_team columns
            max_workers: Maximum number of concurrent scraping workers
            lookback_days: Number of days to look back
            
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
        
        all_team_data = []
        
        # Use ThreadPoolExecutor for concurrent scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit scraping tasks for all teams
            future_to_team = {
                executor.submit(self.scrape_team_history, team, lookback_days): team 
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
                time.sleep(random.uniform(1, 3))
        
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