"""
Main pipeline module for the football data ETL process.
"""

import os
import argparse
import json
from datetime import datetime, timedelta
import logging
import pandas as pd

from utils.logger import PipelineLogger
from scrapers.fixtures_scraper import FixturesScraper
from scrapers.team_scraper import TeamHistoryScraper
from processors.data_processor import FootballDataProcessor
import config


class FootballDataPipeline:
    """Main pipeline class for orchestrating the football data ETL process."""
    
    def __init__(self, start_date=None, end_date=None, leagues=None):
        """Initialize the football data pipeline.
        
        Args:
            start_date: Start date for fixtures (default: today)
            end_date: End date for fixtures (default: 2 weeks from today)
            leagues: List of league dictionaries to filter by (default: from config)
        """
        # Set up logger
        self.logger = PipelineLogger(
            name="football_etl",
            log_file=config.LOG_FILE,
            level=getattr(logging, config.LOG_LEVEL)
        )
        
        # Initialize dates
        self.start_date = start_date or config.TODAY
        self.end_date = end_date or config.FIXTURE_END_DATE
        
        # Initialize leagues
        self.leagues = leagues or config.LEAGUES
        
        # Initialize pipeline components
        self.fixtures_scraper = FixturesScraper(logger=self.logger)
        self.team_scraper = TeamHistoryScraper(logger=self.logger)
        self.data_processor = FootballDataProcessor(logger=self.logger)
    
    def run(self, lookback_matches=7, max_workers=4, max_teams=None, priority_only=False):
        """Run the complete pipeline.
        
        Args:
            lookback_matches: Number of most recent matches to collect per team
            max_workers: Maximum number of concurrent workers for team scraping
            max_teams: Maximum number of teams to process (None for all)
            priority_only: Only process teams from priority leagues
        """
        self.logger.start_pipeline("Football Data ETL")
        
        try:
            # Step 1: Scrape fixtures
            self.logger.info(f"Step 1: Scraping fixtures from {self.start_date} to {self.end_date}")
            fixtures_df = self.fixtures_scraper.scrape_fixtures(
                start_date=self.start_date,
                end_date=self.end_date,
                leagues=self.leagues
            )
            
            if fixtures_df.empty:
                self.logger.error("No fixtures found. Aborting pipeline.")
                return
            
            # Step 2: Process fixtures
            self.logger.info("Step 2: Processing fixtures data")
            processed_fixtures = self.data_processor.process_fixtures(fixtures_df)
            
            if processed_fixtures.empty:
                self.logger.error("Processing fixtures failed. Aborting pipeline.")
                return
            
            # Step 3: Scrape team history based on fixtures
            self.logger.info(f"Step 3: Scraping team history data (most recent {lookback_matches} matches)")
            team_history_df = self.team_scraper.scrape_teams_from_fixtures(
                processed_fixtures,
                max_workers=max_workers,
                lookback_matches=lookback_matches,
                max_teams=max_teams,
                priority_only=priority_only
            )
            
            if team_history_df.empty:
                self.logger.warning("No team history data found. Proceeding with fixtures only.")
            
            # Step 4: Process team history
            self.logger.info("Step 4: Processing team history data")
            processed_team_history = self.data_processor.process_team_history(team_history_df)
            
            # Step 5: Join data
            self.logger.info("Step 5: Joining fixtures and team history data")
            combined_data = self.data_processor.join_data(processed_fixtures, processed_team_history)
            
            # Output results
            pipeline_stats = {
                "fixtures_count": len(processed_fixtures),
                "teams_count": len(processed_team_history['team'].unique()) if not processed_team_history.empty else 0,
                "joined_records": len(combined_data),
                "leagues_covered": len(processed_fixtures['league'].unique()),
                "data_completion": f"{combined_data.notna().mean().mean() * 100:.1f}%" if not combined_data.empty else "0%",
                "start_date": self.start_date.strftime('%Y-%m-%d') if hasattr(self.start_date, 'strftime') else self.start_date,
                "end_date": self.end_date.strftime('%Y-%m-%d') if hasattr(self.end_date, 'strftime') else self.end_date,
                "lookback_matches": lookback_matches,
                "success_rate": f"{(len(team_history_df) / (processed_fixtures['home_team'].nunique() + processed_fixtures['away_team'].nunique()) * 100):.1f}%" if not team_history_df.empty else "0%"
            }
            
            # Write pipeline stats to JSON file
            stats_file = os.path.join(config.OUTPUT_DIR, "pipeline_stats.json")
            with open(stats_file, 'w') as f:
                json.dump(pipeline_stats, f, indent=2)
            
            self.logger.info(f"Pipeline completed successfully. Statistics saved to {stats_file}")
            self.logger.end_pipeline("Football Data ETL", pipeline_stats)
            
            # Send notification if webhook configured
            webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
            if webhook_url and hasattr(self.team_scraper, 'send_notification'):
                success_message = f"✅ Football ETL pipeline completed! Processed {len(processed_fixtures)} fixtures and {len(processed_team_history['team'].unique()) if not processed_team_history.empty else 0} teams."
                self.team_scraper.send_notification(success_message, webhook_url)
            
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {str(e)}", exc_info=True)
            
            # Send error notification if webhook configured
            webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
            if webhook_url and hasattr(self.team_scraper, 'send_notification'):
                error_message = f"❌ Football ETL pipeline failed: {str(e)}"
                self.team_scraper.send_notification(error_message, webhook_url)
            
            self.logger.end_pipeline("Football Data ETL", {"status": "failed", "error": str(e)})
            return None


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Football Data ETL Pipeline")
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for fixtures (YYYY-MM-DD, default: today)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for fixtures (YYYY-MM-DD, default: 2 weeks from today)"
    )
    
    parser.add_argument(
        "--lookback-matches",
        type=int,
        default=7,
        help="Number of most recent matches to look back for team history (default: 7)"
    )
    
    parser.add_argument(
        "--leagues",
        type=str,
        help="Path to JSON file with custom league configurations"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=config.OUTPUT_DIR,
        help=f"Output directory for pipeline results (default: {config.OUTPUT_DIR})"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of concurrent workers for team scraping (default: 4)"
    )
    
    parser.add_argument(
        "--max-teams",
        type=int,
        help="Maximum number of teams to process (default: all teams)"
    )
    
    parser.add_argument(
        "--priority-only",
        action="store_true",
        help="Only process teams from priority leagues (Premier League, La Liga, etc.)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the pipeline."""
    args = parse_args()
    
    # Process dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else config.TODAY
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else config.FIXTURE_END_DATE
    
    # Override config values if arguments provided
    lookback_matches = args.lookback_matches
    max_workers = args.max_workers
    max_teams = args.max_teams
    priority_only = args.priority_only
    
    if args.output_dir:
        config.OUTPUT_DIR = args.output_dir
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    
    # Load custom leagues if provided
    leagues = None
    if args.leagues:
        with open(args.leagues, 'r') as f:
            leagues = json.load(f)
    
    # Run the pipeline
    pipeline = FootballDataPipeline(
        start_date=start_date,
        end_date=end_date,
        leagues=leagues
    )
    
    pipeline.run(
        lookback_matches=lookback_matches,
        max_workers=max_workers,
        max_teams=max_teams,
        priority_only=priority_only
    )


if __name__ == "__main__":
    main()