"""
Data processor module for cleaning and joining football data.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple

from utils.logger import PipelineLogger
from utils.data_utils import (
    normalize_team_name, normalize_date, generate_match_id,
    validate_data, join_fixtures_with_team_history, aggregate_team_stats
)
import config


class FootballDataProcessor:
    """Processor for cleaning and joining football data."""
    
    def __init__(self, logger: PipelineLogger = None):
        """Initialize the data processor.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
        
        # Create output directories if they don't exist
        os.makedirs(config.PROCESSED_DIR, exist_ok=True)
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    
    def process_fixtures(self, fixtures_df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean fixtures data.
        
        Args:
            fixtures_df: Raw fixtures DataFrame
            
        Returns:
            pd.DataFrame: Processed fixtures DataFrame
        """
        if fixtures_df.empty:
            if self.logger:
                self.logger.warning("No fixtures data to process")
            return pd.DataFrame()
        
        if self.logger:
            self.logger.start_job("Processing fixtures data")
        
        try:
            # Create a copy to avoid modifying the original
            df = fixtures_df.copy()
            
            # Check required columns
            required_cols = ['match_id', 'date', 'home_team', 'away_team', 'league']
            validation_results = validate_data(df, required_cols)
            
            if not validation_results['valid']:
                if self.logger:
                    self.logger.warning(f"Fixtures data validation failed: {validation_results}")
                
                # If missing columns, try to recover
                if validation_results['missing_columns']:
                    if self.logger:
                        self.logger.warning(f"Missing required columns: {validation_results['missing_columns']}")
                    
                    # Generate match_id if missing
                    if 'match_id' in validation_results['missing_columns'] and 'date' in df.columns and 'home_team' in df.columns and 'away_team' in df.columns:
                        df['match_id'] = df.apply(
                            lambda row: generate_match_id(row['date'], row['home_team'], row['away_team']), 
                            axis=1
                        )
                        
                        if self.logger:
                            self.logger.info("Generated missing match_id column")
            
            # Normalize team names
            df['home_team'] = df['home_team'].apply(normalize_team_name)
            df['away_team'] = df['away_team'].apply(normalize_team_name)
            
            # Normalize dates
            df['date'] = df['date'].apply(normalize_date)
            
            # Add a normalized kickoff time column
            if 'kickoff_time' in df.columns:
                # Keep only the time part in 24-hour format (HH:MM)
                df['kickoff_time'] = df['kickoff_time'].apply(
                    lambda x: x.split(' ')[-1] if isinstance(x, str) and ' ' in x else x
                )
                
                # Ensure consistent time format (HH:MM)
                df['kickoff_time'] = df['kickoff_time'].apply(
                    lambda x: x if isinstance(x, str) and ':' in x else None
                )
            
            # Ensure venue field exists
            if 'venue' not in df.columns:
                df['venue'] = None
            
            # Drop duplicates based on match_id
            duplicates = df['match_id'].duplicated().sum()
            if duplicates > 0:
                if self.logger:
                    self.logger.warning(f"Dropping {duplicates} duplicate fixtures")
                df = df.drop_duplicates(subset=['match_id'], keep='first')
            
            # Keep only fixtures with future dates
            today = datetime.today().strftime('%Y-%m-%d')
            df = df[df['date'] >= today]
            
            # Sort by date and kickoff time
            if 'kickoff_time' in df.columns:
                df = df.sort_values(['date', 'kickoff_time'])
            else:
                df = df.sort_values('date')
            
            # Save processed data
            processed_file = os.path.join(config.PROCESSED_DIR, "processed_fixtures.csv")
            df.to_csv(processed_file, index=False)
            
            if self.logger:
                self.logger.info(f"Processed {len(df)} fixtures")
                self.logger.info(f"Processed data saved to {processed_file}")
                self.logger.end_job("Processing fixtures data", {
                    "fixtures_count": len(df),
                    "leagues_count": df['league'].nunique()
                })
            
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error processing fixtures data: {str(e)}", exc_info=True)
            
            return pd.DataFrame()
    
    def process_team_history(self, team_history_df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean team historical data.
        
        Args:
            team_history_df: Raw team history DataFrame
            
        Returns:
            pd.DataFrame: Processed team history DataFrame
        """
        if team_history_df.empty:
            if self.logger:
                self.logger.warning("No team history data to process")
            return pd.DataFrame()
        
        if self.logger:
            self.logger.start_job("Processing team history data")
        
        try:
            # Create a copy to avoid modifying the original
            df = team_history_df.copy()
            
            # Check required columns
            required_cols = ['team', 'date', 'opponent']
            validation_results = validate_data(df, required_cols)
            
            if not validation_results['valid']:
                if self.logger:
                    self.logger.warning(f"Team history data validation failed: {validation_results}")
                
                # If missing essential columns, we can't proceed
                if any(col in validation_results['missing_columns'] for col in ['team', 'date']):
                    if self.logger:
                        self.logger.error("Missing essential columns for team history data")
                    return pd.DataFrame()
            
            # Normalize team and opponent names
            df['team'] = df['team'].apply(normalize_team_name)
            if 'opponent' in df.columns:
                df['opponent'] = df['opponent'].apply(normalize_team_name)
            
            # Normalize dates
            df['date'] = df['date'].apply(normalize_date)
            
            # Convert 'result' column to standardized format (W/D/L)
            if 'result' in df.columns:
                # Map various result formats to W/D/L
                result_mapping = {
                    'W': 'W', 'D': 'D', 'L': 'L',
                    'Win': 'W', 'Draw': 'D', 'Loss': 'L',
                    'win': 'W', 'draw': 'D', 'loss': 'L',
                    '1': 'W', '0': 'L', '0.5': 'D'
                }
                
                df['result'] = df['result'].apply(
                    lambda x: result_mapping.get(str(x).strip(), 'U')  # U for unknown
                )
            
            # Ensure numeric columns are properly typed
            numeric_cols = [
                'goals_for', 'goals_against', 'expected_goals', 'expected_goals_against',
                'shots', 'shots_on_target', 'possession', 'distance_covered',
                'free_kicks', 'penalties_scored', 'penalties_attempted'
            ]
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate additional metrics
            if 'shots' in df.columns and 'shots_on_target' in df.columns:
                df['shot_accuracy'] = np.where(
                    df['shots'] > 0,
                    df['shots_on_target'] / df['shots'],
                    0
                )
            
            if 'goals_for' in df.columns and 'shots_on_target' in df.columns:
                df['conversion_rate'] = np.where(
                    df['shots_on_target'] > 0,
                    df['goals_for'] / df['shots_on_target'],
                    0
                )
            
            # Generate match_id if missing
            if 'match_id' not in df.columns:
                if 'team' in df.columns and 'opponent' in df.columns and 'date' in df.columns and 'is_home' in df.columns:
                    df['match_id'] = df.apply(
                        lambda row: generate_match_id(
                            row['date'],
                            row['team'] if row['is_home'] == 1 else row['opponent'],
                            row['opponent'] if row['is_home'] == 1 else row['team']
                        ),
                        axis=1
                    )
                elif 'team' in df.columns and 'opponent' in df.columns and 'date' in df.columns:
                    # Without home/away info, create a consistent match_id based on alphabetical order
                    df['match_id'] = df.apply(
                        lambda row: generate_match_id(
                            row['date'],
                            min(row['team'], row['opponent']),
                            max(row['team'], row['opponent'])
                        ),
                        axis=1
                    )
            
            # Sort by team and date
            df = df.sort_values(['team', 'date'], ascending=[True, False])
            
            # Filter out matches from the future (data integrity check)
            today = datetime.today().strftime('%Y-%m-%d')
            df = df[df['date'] <= today]
            
            # Save processed data
            processed_file = os.path.join(config.PROCESSED_DIR, "processed_team_history.csv")
            df.to_csv(processed_file, index=False)
            
            if self.logger:
                self.logger.info(f"Processed {len(df)} team history records")
                self.logger.info(f"Processed {df['team'].nunique()} unique teams")
                self.logger.info(f"Processed data saved to {processed_file}")
                self.logger.end_job("Processing team history data", {
                    "record_count": len(df),
                    "team_count": df['team'].nunique()
                })
            
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error processing team history data: {str(e)}", exc_info=True)
            
            return pd.DataFrame()
    
    def calculate_team_metrics(self, team_history_df: pd.DataFrame, window_days: int = 90) -> pd.DataFrame:
        """Calculate team performance metrics based on historical data.
        
        Args:
            team_history_df: Processed team history DataFrame
            window_days: Number of days to include in the rolling window
            
        Returns:
            pd.DataFrame: Team metrics DataFrame
        """
        if team_history_df.empty:
            if self.logger:
                self.logger.warning("No team history data for metric calculation")
            return pd.DataFrame()
        
        if self.logger:
            self.logger.start_job(f"Calculating team metrics (window: {window_days} days)")
        
        try:
            # Use the utility function to aggregate team stats
            metrics_df = aggregate_team_stats(team_history_df, window_days)
            
            if metrics_df.empty:
                if self.logger:
                    self.logger.warning("No team metrics generated")
                return pd.DataFrame()
            
            # Calculate additional metrics
            
            # Sort by team and date (latest first)
            metrics_df = metrics_df.sort_values(['team', 'date'], ascending=[True, False])
            
            # Save processed data
            processed_file = os.path.join(config.PROCESSED_DIR, "team_metrics.csv")
            metrics_df.to_csv(processed_file, index=False)
            
            if self.logger:
                self.logger.info(f"Generated metrics for {metrics_df['team'].nunique()} teams")
                self.logger.info(f"Metrics data saved to {processed_file}")
                self.logger.end_job("Calculating team metrics", {
                    "team_count": metrics_df['team'].nunique()
                })
            
            return metrics_df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error calculating team metrics: {str(e)}", exc_info=True)
            
            return pd.DataFrame()
    
    def join_data(self, fixtures_df: pd.DataFrame, team_history_df: pd.DataFrame) -> pd.DataFrame:
        """Join fixtures with team historical data.
        
        Args:
            fixtures_df: Processed fixtures DataFrame
            team_history_df: Processed team history DataFrame
            
        Returns:
            pd.DataFrame: Combined DataFrame with fixtures and team metrics
        """
        if fixtures_df.empty:
            if self.logger:
                self.logger.warning("No fixtures data for joining")
            return pd.DataFrame()
        
        if team_history_df.empty:
            if self.logger:
                self.logger.warning("No team history data for joining")
            return fixtures_df  # Return only fixtures if no history data
        
        if self.logger:
            self.logger.start_job("Joining fixtures and team history data")
        
        try:
            # First calculate team metrics
            metrics_df = self.calculate_team_metrics(team_history_df)
            
            if metrics_df.empty:
                if self.logger:
                    self.logger.warning("No team metrics available for joining")
                return fixtures_df
            
            # Use the utility function to join fixtures with team history
            joined_df = join_fixtures_with_team_history(fixtures_df, metrics_df)
            
            if joined_df.empty:
                if self.logger:
                    self.logger.warning("Joining produced empty DataFrame")
                return fixtures_df
            
            # Sort by date
            joined_df = joined_df.sort_values('date')
            
            # Save joined data
            output_file = os.path.join(config.OUTPUT_DIR, "football_data.csv")
            joined_df.to_csv(output_file, index=False)
            
            # Also save fixtures and team history separately
            fixtures_output = os.path.join(config.OUTPUT_DIR, "upcoming_fixtures.csv")
            fixtures_df.to_csv(fixtures_output, index=False)
            
            team_history_output = os.path.join(config.OUTPUT_DIR, "team_history.csv")
            team_history_df.to_csv(team_history_output, index=False)
            
            if self.logger:
                self.logger.info(f"Successfully joined data for {len(joined_df)} fixtures")
                self.logger.info(f"Joined data saved to {output_file}")
                self.logger.info(f"Fixtures saved to {fixtures_output}")
                self.logger.info(f"Team history saved to {team_history_output}")
                self.logger.end_job("Joining fixtures and team history data", {
                    "joined_record_count": len(joined_df),
                    "data_completion_rate": f"{(joined_df.notna().mean().mean() * 100):.1f}%"
                })
            
            return joined_df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error joining data: {str(e)}", exc_info=True)
            
            return fixtures_df  # Return only fixtures if joining fails