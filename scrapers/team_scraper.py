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
            
            # Get match rows
            match_rows = matches_table.select('tbody tr')
            
            # Filter out non-match rows (sometimes used for section headings)
            valid_match_rows = []
            for row in match_rows:
                # Skip spacer rows or rows without proper data
                if 'class' in row.attrs and 'spacer' in row['class']:
                    continue
                
                # Skip divider rows
                if not row.select('th, td'):
                    continue
                
                # Only include completed matches (with a score)
                cells = row.select('td')
                if len(cells) > 4:  # Make sure it has enough cells
                    # Look for score cell (format like "2-1" or "1-0")
                    for cell in cells:
                        if cell.get('data-stat') == 'score' and cell.text and '-' in cell.text:
                            valid_match_rows.append(row)
                            break
            
            # Get only the most recent N matches
            recent_matches = valid_match_rows[:lookback_matches]
            
            # Collect individual match data and match URLs
            matches_data = []
            match_urls = []
            
            for row in recent_matches:
                cells = row.select('th, td')
                
                # Extract match URL for detailed stats
                match_url = None
                for cell in cells:
                    # Find the score cell, which typically has the match report link
                    if cell.get('data-stat') == 'score':
                        link = cell.find('a')
                        if link and '/matches/' in link['href']:
                            match_url = f"https://fbref.com{link['href']}"
                            match_urls.append(match_url)
                            break
                
                # Extract basic match info
                match_data = {
                    'team': normalize_team_name(team_name),
                    'match_url': match_url
                }
                
                for cell in cells:
                    stat_name = cell.get('data-stat', '')
                    if stat_name:
                        # Extract the value
                        value = cell.text.strip()
                        
                        # Map FBref column names to our standardized names
                        if stat_name == 'date':
                            match_data['date'] = normalize_date(value)
                        elif stat_name == 'comp':
                            match_data['competition'] = value
                        elif stat_name == 'round':
                            match_data['round'] = value
                        elif stat_name == 'venue':
                            match_data['venue'] = value  # Home or Away
                        elif stat_name == 'opponent':
                            match_data['opponent'] = normalize_team_name(value)
                        elif stat_name == 'result':
                            match_data['result'] = value  # W, D, L
                        elif stat_name == 'goals_for':
                            match_data['goals_for'] = clean_number(value)
                        elif stat_name == 'goals_against':
                            match_data['goals_against'] = clean_number(value)
                        elif stat_name == 'score':
                            # Extract from format like "2-1" or "1-0"
                            if '-' in value:
                                parts = value.split('-')
                                if len(parts) == 2:
                                    try:
                                        if match_data.get('venue') == 'Home':
                                            match_data['goals_for'] = int(parts[0].strip())
                                            match_data['goals_against'] = int(parts[1].strip())
                                        else:
                                            match_data['goals_for'] = int(parts[1].strip())
                                            match_data['goals_against'] = int(parts[0].strip())
                                    except ValueError:
                                        pass
                
                # Add home/away flag
                if match_data.get('venue') == 'Home':
                    match_data['is_home'] = 1
                    match_data['home_team'] = team_name
                    match_data['away_team'] = match_data.get('opponent', '')
                else:
                    match_data['is_home'] = 0
                    match_data['home_team'] = match_data.get('opponent', '')
                    match_data['away_team'] = team_name
                
                # Generate a match ID
                if 'date' in match_data and 'home_team' in match_data and 'away_team' in match_data:
                    match_data['match_id'] = generate_match_id(
                        match_data['date'],
                        match_data['home_team'],
                        match_data['away_team']
                    )
                
                matches_data.append(match_data)
            
            # Create initial DataFrame with basic match info
            team_df = pd.DataFrame(matches_data)
            
            # Now fetch detailed match statistics for each match
            for match_url in match_urls:
                try:
                    # Extract match_id from URL
                    match_id_part = match_url.split('/matches/')[1].split('/')[0]
                    
                    # Fetch detailed match stats
                    detailed_stats = self._scrape_match_details(match_url, team_name)
                    
                    if detailed_stats:
                        # Find the corresponding row in the DataFrame
                        for idx, row in team_df.iterrows():
                            if row.get('match_url') == match_url:
                                # Update with detailed stats
                                for key, value in detailed_stats.items():
                                    team_df.at[idx, key] = value
                                break
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Error scraping detailed stats for match {match_url}: {str(e)}")
            
            # Clean numeric columns
            numeric_cols = [
                'goals_for', 'goals_against', 
                'xg', 'xg_against',
                'possession', 'total_passes', 'pass_completion_pct',
                'shots', 'shots_on_target', 'big_chances',
                'corners', 'fouls_committed', 'yellow_cards', 'red_cards'
            ]
            
            for col in numeric_cols:
                if col in team_df.columns:
                    team_df[col] = team_df[col].apply(clean_number)
            
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
    
    def _scrape_match_details(self, match_url: str, team_name: str) -> dict:
        """Scrape detailed match statistics from a specific match page.
        
        Args:
            match_url: URL to the match page
            team_name: Name of the team we're collecting data for
            
        Returns:
            dict: Dictionary of match statistics
        """
        if self.logger:
            self.logger.info(f"Scraping detailed match stats from: {match_url}")
        
        try:
            # Get the match page
            soup = get_soup(match_url)
            
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
            
            # Extract expected goals
            xg_divs = soup.select('div.scorebox_meta strong')
            for div in xg_divs:
                if 'xG' in div.text:
                    xg_text = div.text.strip()
                    xg_values = re.findall(r'([0-9.]+)', xg_text)
                    if len(xg_values) >= 2:
                        stats['xg'] = float(xg_values[our_team_idx])
                        stats['xg_against'] = float(xg_values[opponent_idx])
                    break
            
            # Extract detailed stats from all stats tables
            stats_tables = soup.select('div#team_stats table, div#team_stats_extra table')
            
            for table in stats_tables:
                # Extract stat name and values
                for row in table.select('tr'):
                    # Skip header rows
                    if row.find('th'):
                        continue
                    
                    # Get the cells (home team | stat name | away team)
                    cells = row.select('td')
                    if len(cells) >= 3:
                        stat_name = cells[1].text.strip().lower()
                        home_value = cells[0].text.strip()
                        away_value = cells[2].text.strip()
                        
                        our_value = home_value if our_team_idx == 0 else away_value
                        opponent_value = away_value if our_team_idx == 0 else home_value
                        
                        # Map common stat names to our standardized names
                        if 'possession' in stat_name:
                            stats['possession'] = clean_number(our_value.replace('%', ''))
                            stats['opponent_possession'] = clean_number(opponent_value.replace('%', ''))
                        elif 'passes' in stat_name and 'accurate' not in stat_name.lower():
                            stats['total_passes'] = clean_number(our_value)
                            stats['opponent_total_passes'] = clean_number(opponent_value)
                        elif 'pass accuracy' in stat_name or 'pass completion' in stat_name:
                            stats['pass_completion_pct'] = clean_number(our_value.replace('%', ''))
                            stats['opponent_pass_completion_pct'] = clean_number(opponent_value.replace('%', ''))
                        elif 'shots' == stat_name or 'shots total' in stat_name:
                            stats['shots'] = clean_number(our_value)
                            stats['opponent_shots'] = clean_number(opponent_value)
                        elif 'shots on target' in stat_name:
                            stats['shots_on_target'] = clean_number(our_value)
                            stats['opponent_shots_on_target'] = clean_number(opponent_value)
                        elif 'big chances' in stat_name:
                            stats['big_chances'] = clean_number(our_value)
                            stats['opponent_big_chances'] = clean_number(opponent_value)
                        elif 'corner' in stat_name:
                            stats['corners'] = clean_number(our_value)
                            stats['opponent_corners'] = clean_number(opponent_value)
                        elif 'fouls' in stat_name and 'committed' in stat_name:
                            stats['fouls_committed'] = clean_number(our_value)
                            stats['opponent_fouls_committed'] = clean_number(opponent_value)
                        elif 'yellow card' in stat_name:
                            stats['yellow_cards'] = clean_number(our_value)
                            stats['opponent_yellow_cards'] = clean_number(opponent_value)
                        elif 'red card' in stat_name:
                            stats['red_cards'] = clean_number(our_value)
                            stats['opponent_red_cards'] = clean_number(opponent_value)
            
            return stats
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error scraping match details from {match_url}: {str(e)}")
            return {}
    
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
        
        return combined_dffuture_to_team):
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
        
        return combined_dffuture_to_team):
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