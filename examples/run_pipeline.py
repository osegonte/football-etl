"""
Example script showing different ways to run the football data ETL pipeline.
"""

import os
import sys
import json
from datetime import datetime, timedelta

# Add parent directory to path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline import FootballDataPipeline
import config


def run_basic_example():
    """
    Run the pipeline with default settings:
    - Fixtures for the next 14 days
    - Team history for the past year
    - Default leagues from config
    """
    print("\n=== Running Basic Example ===")
    
    pipeline = FootballDataPipeline()
    pipeline.run()


def run_custom_dates_example():
    """
    Run the pipeline with custom date range:
    - Fixtures for next weekend only (Friday to Sunday)
    - Team history for the past 90 days
    """
    print("\n=== Running Custom Dates Example ===")
    
    # Calculate next weekend dates
    today = datetime.today().date()
    days_until_friday = (4 - today.weekday()) % 7  # 4 = Friday
    next_friday = today + timedelta(days=days_until_friday)
    next_sunday = next_friday + timedelta(days=2)
    
    # Override lookback days in config
    config.TEAM_HISTORY_DAYS = 90
    
    pipeline = FootballDataPipeline(
        start_date=next_friday,
        end_date=next_sunday
    )
    pipeline.run()


def run_custom_leagues_example():
    """
    Run the pipeline with custom leagues:
    - Only Premier League and Champions League
    - Default date range
    """
    print("\n=== Running Custom Leagues Example ===")
    
    custom_leagues = [
        {"name": "Premier League", "country": "England", "id": "9"},
        {"name": "Champions League", "country": "Europe", "id": "8"}
    ]
    
    pipeline = FootballDataPipeline(leagues=custom_leagues)
    pipeline.run()


def run_full_custom_example():
    """
    Run the pipeline with fully custom settings:
    - Custom date range
    - Custom leagues
    - Custom output directory
    """
    print("\n=== Running Full Custom Example ===")
    
    # Custom dates: next month
    today = datetime.today().date()
    start_date = today + timedelta(days=30)
    end_date = start_date + timedelta(days=14)
    
    # Custom leagues: top 5 European leagues
    custom_leagues = [
        {"name": "Premier League", "country": "England", "id": "9"},
        {"name": "La Liga", "country": "Spain", "id": "12"},
        {"name": "Bundesliga", "country": "Germany", "id": "20"},
        {"name": "Serie A", "country": "Italy", "id": "11"},
        {"name": "Ligue 1", "country": "France", "id": "13"}
    ]
    
    # Custom output directory
    custom_output = os.path.join(config.DATA_DIR, "custom_output")
    os.makedirs(custom_output, exist_ok=True)
    
    # Set custom config values
    config.OUTPUT_DIR = custom_output
    config.TEAM_HISTORY_DAYS = 180  # 6 months of team history
    
    pipeline = FootballDataPipeline(
        start_date=start_date,
        end_date=end_date,
        leagues=custom_leagues
    )
    pipeline.run()


def main():
    """Main function to run examples."""
    print("Football Data ETL Pipeline Examples")
    print("==================================")
    
    while True:
        print("\nSelect an example to run:")
        print("1. Basic example (default settings)")
        print("2. Custom dates example (next weekend)")
        print("3. Custom leagues example (Premier League and Champions League)")
        print("4. Full custom example (custom dates, leagues, and output)")
        print("0. Exit")
        
        choice = input("\nEnter your choice (0-4): ")
        
        if choice == '1':
            run_basic_example()
        elif choice == '2':
            run_custom_dates_example()
        elif choice == '3':
            run_custom_leagues_example()
        elif choice == '4':
            run_full_custom_example()
        elif choice == '0':
            print("\nExiting...")
            break
        else:
            print("\nInvalid choice. Please try again.")


if __name__ == "__main__":
    main()