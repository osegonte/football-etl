"""
Configuration module for the football data ETL pipeline.
"""

import os
from datetime import datetime, timedelta

# Base directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create directories if they don't exist
for directory in [DATA_DIR, RAW_DIR, PROCESSED_DIR, OUTPUT_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

# Date settings
TODAY = datetime.today().date()
DEFAULT_DAYS_AHEAD = 14  # Fetch fixtures for the next 2 weeks by default
FIXTURE_END_DATE = TODAY + timedelta(days=DEFAULT_DAYS_AHEAD)

# Team history lookback period (in days)
TEAM_HISTORY_DAYS = 365  # Get team stats for the past year

# Target leagues configuration
LEAGUES = [
    {"name": "Premier League", "country": "England", "id": "9"},
    {"name": "La Liga", "country": "Spain", "id": "12"},
    {"name": "Bundesliga", "country": "Germany", "id": "20"},
    {"name": "Serie A", "country": "Italy", "id": "11"},
    {"name": "Ligue 1", "country": "France", "id": "13"},
    {"name": "Champions League", "country": "Europe", "id": "8"}
]

# Mapping of team names between different sources
# This helps harmonize team names across SofaScore and FBref
TEAM_NAME_MAPPING = {
    # Premier League
    "Manchester United": "Man United",
    "Manchester City": "Man City",
    "Tottenham": "Tottenham Hotspur",
    "Tottenham Hotspur": "Tottenham",
    "Newcastle": "Newcastle United",
    "Newcastle United": "Newcastle",
    "Wolverhampton Wanderers": "Wolves",
    "Wolves": "Wolverhampton Wanderers",
    
    # La Liga
    "Atletico Madrid": "Atlético Madrid",
    "Atlético Madrid": "Atletico Madrid",
    "Atletico": "Atlético Madrid",
    "Real Betis": "Betis",
    "Betis": "Real Betis",
    
    # Bundesliga
    "Bayern Munich": "Bayern München",
    "Bayern München": "Bayern Munich",
    "RB Leipzig": "Leipzig",
    "Leipzig": "RB Leipzig",
    "Bayer Leverkusen": "Leverkusen",
    "Leverkusen": "Bayer Leverkusen",
    
    # Serie A
    "Inter": "Inter Milan",
    "Inter Milan": "Inter",
    "AC Milan": "Milan",
    "Milan": "AC Milan",
    
    # Ligue 1
    "Paris Saint Germain": "PSG",
    "Paris Saint-Germain": "PSG",
    "PSG": "Paris Saint-Germain"
}

# HTTP request settings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0"
}

# Request retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Output file settings
FIXTURES_FILE = os.path.join(OUTPUT_DIR, "upcoming_fixtures.csv")
TEAM_HISTORY_FILE = os.path.join(OUTPUT_DIR, "team_history.csv")
COMBINED_DATA_FILE = os.path.join(OUTPUT_DIR, "football_data.csv")

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FILE = os.path.join(LOG_DIR, f"football_etl_{TODAY.strftime('%Y%m%d')}.log")