"""
SofaScore fixtures scraper module.

This module is responsible for scraping upcoming fixtures from SofaScore.
It leverages the functionality from the daily_match_scraper.py file.
"""

import os
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any

from utils.logger import PipelineLogger
from utils.data_utils import normalize_team_name, normalize_date, generate_match_id
import config

# Import the SofaScore scraper class from daily_match_scraper.py
from daily_match_scraper import AdvancedSofaScoreScraper


class FixturesScraper:
    """Scraper for upcoming football fixtures from SofaScore."""
    
    def __init__(self, logger: PipelineLogger = None):
        """Initialize the fixtures scraper.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
        self.scraper = AdvancedSofaScoreScraper()
        
        # Create output directory if it doesn't exist
        os.makedirs(config.RAW_DIR, exist_ok=True)
    
    def scrape_fixtures(
        self,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        leagues: List[Dict] = None
    ) -> pd.DataFrame:
        """Scrape upcoming fixtures from SofaScore.
        
        Args:
            start_date: Start date for fixtures (default: today)
            end_date: End date for fixtures (default: 2 weeks from today)
            leagues: List of league dictionaries to filter by
            
        Returns:
            pd.DataFrame: DataFrame containing upcoming fixtures
        """
        # Use default dates if not provided
        if start_date is None:
            start_date = config.TODAY
            
        if end_date is None:
            end_date = config.FIXTURE_END_DATE
            
        if leagues is None:
            leagues = config.LEAGUES
            
        # Convert string dates to datetime.date if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # Log the scraping task
        if self.logger:
            self.logger.start_job(f"SofaScore fixtures scraping ({start_date} to {end_date})")
            self.logger.info(f"Scraping fixtures for {len(leagues)} leagues")
        
        # Use the scraper from daily_match_scraper.py
        try:
            # Initialize browser session to get cookies
            self.scraper.initialize_browser_session()
            
            # Fetch fixtures for the date range
            all_matches, total_matches = self.scraper.fetch_matches_for_date_range(start_date, end_date)
            
            if not all_matches or total_matches == 0:
                if self.logger:
                    self.logger.warning("No fixtures found for the specified date range")
                return pd.DataFrame()
            
            # Convert the nested dictionary to a list of fixtures
            fixtures_list = []
            for date_str, matches in all_matches.items():
                for match in matches:
                    # Add match to list only if it belongs to one of the target leagues
                    league_name = match.get('league', '')
                    country = match.get('country', '')
                    
                    # Check if this match belongs to one of the target leagues
                    match_included = False
                    for league in leagues:
                        if (league['name'].lower() in league_name.lower() or 
                            league['country'].lower() in country.lower()):
                            match_included = True
                            break
                    
                    if not match_included:
                        continue
                    
                    # Extract match details
                    home_team = match.get('home_team', '')
                    away_team = match.get('away_team', '')
                    
                    # Generate a unique match ID
                    match_id = match.get('id', generate_match_id(date_str, home_team, away_team))
                    
                    # Create a standardized fixture record
                    fixture = {
                        'match_id': match_id,
                        'date': normalize_date(date_str),
                        'home_team': normalize_team_name(home_team),
                        'away_team': normalize_team_name(away_team),
                        'league': league_name,
                        'country': country,
                        'venue': match.get('venue', ''),
                        'kickoff_time': match.get('start_time', ''),
                        'status': match.get('status', 'Scheduled'),
                        'competition_stage': match.get('round', '')
                    }
                    
                    fixtures_list.append(fixture)
            
            # Convert to DataFrame
            fixtures_df = pd.DataFrame(fixtures_list)
            
            # Save raw data
            raw_file = os.path.join(
                config.RAW_DIR, 
                f"raw_fixtures_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            )
            fixtures_df.to_csv(raw_file, index=False)
            
            if self.logger:
                self.logger.info(f"Scraped {len(fixtures_df)} fixtures")
                self.logger.info(f"Raw data saved to {raw_file}")
                self.logger.end_job(f"SofaScore fixtures scraping", {
                    "fixtures_count": len(fixtures_df),
                    "leagues_found": fixtures_df['league'].nunique(),
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "end_date": end_date.strftime('%Y-%m-%d')
                })
            
            return fixtures_df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error scraping fixtures: {str(e)}", exc_info=True)
            return pd.DataFrame()