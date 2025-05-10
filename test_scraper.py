"""
Test script for the team scraper.
"""

import os
import sys
import time
import logging
from utils.logger import PipelineLogger
from scrapers.team_scraper import TeamHistoryScraper
import config

# Set up logging
log_file = os.path.join(config.LOG_DIR, "scraper_test.log")
logger = PipelineLogger(
    name="scraper_test",
    log_file=log_file,
    level=logging.INFO
)

# Create scraper instance
scraper = TeamHistoryScraper(logger=logger)

# Test teams to scrape
test_teams = [
    "Manchester United",
    "Arsenal",
    "Barcelona",
    "Real Madrid",
    "Bayern Munich"
]

def test_individual_teams():
    """Test scraping each team individually."""
    logger.info("=== Testing Individual Team Scraping ===")
    
    for team_name in test_teams:
        logger.info(f"Testing team: {team_name}")
        try:
            # Try to find team URL
            team_url = scraper._find_team_url(team_name)
            logger.info(f"Team URL for {team_name}: {team_url}")
            
            if team_url:
                # Test basic scrape with short timeout
                logger.info(f"Attempting basic scrape for {team_name}")
                
                # Define a timeout function
                import signal
                
                class TimeoutError(Exception):
                    pass
                
                def timeout_handler(signum, frame):
                    raise TimeoutError("Function call timed out")
                
                # Set timeout of 30 seconds
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(30)
                
                try:
                    df = scraper._basic_scrape_team_history(team_name, team_url, lookback_matches=3)
                    signal.alarm(0)  # Reset the alarm
                    logger.info(f"Basic scrape result: {len(df)} rows")
                except TimeoutError:
                    logger.warning(f"Basic scrape for {team_name} timed out after 30 seconds")
                
                # Test detailed scrape with timeout
                logger.info(f"Attempting detailed scrape for {team_name}")
                signal.alarm(30)
                
                try:
                    df = scraper._detailed_scrape_team_history(team_name, team_url, lookback_matches=3)
                    signal.alarm(0)  # Reset the alarm
                    logger.info(f"Detailed scrape result: {len(df)} rows")
                except TimeoutError:
                    logger.warning(f"Detailed scrape for {team_name} timed out after 30 seconds")
            else:
                logger.warning(f"No URL found for {team_name}")
        
        except Exception as e:
            logger.error(f"Error testing {team_name}: {str(e)}", exc_info=True)
        
        logger.info(f"Completed test for {team_name}\n")
        time.sleep(2)  # Brief pause between teams

def test_parallel_scraping():
    """Test parallel scraping with a small number of teams."""
    logger.info("=== Testing Parallel Team Scraping ===")
    
    try:
        # Force max_workers to 2 for testing
        df = scraper.scrape_teams_from_fixtures(
            pd.DataFrame({
                'home_team': test_teams[:3],
                'away_team': test_teams[3:],
                'league': ['Test League'] * 3
            }),
            max_workers=2,  
            lookback_matches=3,
            max_teams=None, 
            priority_only=False,
            retry_failed=True
        )
        
        logger.info(f"Parallel scrape result: {len(df)} rows total")
        
    except Exception as e:
        logger.error(f"Error in parallel scraping: {str(e)}", exc_info=True)

def test_fbref_connectivity():
    """Test basic connectivity to FBref.com."""
    logger.info("=== Testing FBref Connectivity ===")
    
    try:
        import requests
        import time
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        # Test main page
        logger.info("Testing connection to FBref main page")
        start = time.time()
        response = requests.get("https://fbref.com/en/", headers=headers, timeout=10)
        duration = time.time() - start
        
        logger.info(f"Main page response: status={response.status_code}, time={duration:.2f}s, length={len(response.content)} bytes")
        
        # Test a specific team page
        logger.info("Testing connection to a specific team page")
        start = time.time()
        response = requests.get("https://fbref.com/en/squads/19538871/Manchester-United-Stats", headers=headers, timeout=10)
        duration = time.time() - start
        
        logger.info(f"Team page response: status={response.status_code}, time={duration:.2f}s, length={len(response.content)} bytes")
        
        # Test search page
        logger.info("Testing connection to search page")
        start = time.time()
        response = requests.get("https://fbref.com/en/search/search.fcgi?search=Arsenal", headers=headers, timeout=10)
        duration = time.time() - start
        
        logger.info(f"Search page response: status={response.status_code}, time={duration:.2f}s, length={len(response.content)} bytes")
        
        return True
    except Exception as e:
        logger.error(f"FBref connectivity test failed: {str(e)}", exc_info=True)
        return False
    
if __name__ == "__main__":
    logger.info("Starting Team Scraper Test")
    
    # Uncomment the test you want to run
    test_individual_teams()
    #test_parallel_scraping()
    
    logger.info("Team Scraper Test Completed")