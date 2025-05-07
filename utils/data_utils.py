"""
Data utilities for cleaning, transforming, and joining football data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, List, Optional, Union, Any

from config import TEAM_NAME_MAPPING


def normalize_team_name(name: str) -> str:
    """Normalize team name to handle differences between data sources.
    
    Args:
        name: Team name to normalize
        
    Returns:
        str: Normalized team name
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Remove common suffixes
    name = re.sub(r'\s+FC$|\s+CF$|\s+AFC$', '', name.strip())
    
    # Check if this name has a mapping
    if name in TEAM_NAME_MAPPING:
        return TEAM_NAME_MAPPING[name]
    
    return name


def normalize_date(date_str: Union[str, datetime, pd.Timestamp], output_format: str = "%Y-%m-%d") -> str:
    """Normalize date to a consistent format.
    
    Args:
        date_str: Date string or object to normalize
        output_format: Format string for output date
        
    Returns:
        str: Normalized date string
    """
    if pd.isna(date_str) or date_str is None or date_str == "":
        return None
    
    if isinstance(date_str, (datetime, pd.Timestamp)):
        return date_str.strftime(output_format)
    
    # Try different date formats
    formats = [
        "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y", 
        "%d-%m-%Y", "%m-%d-%Y", "%d.%m.%Y", "%m.%d.%Y",
        "%d%m%Y", "%m%d%Y", "%b %d, %Y", "%d %b %Y"
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(str(date_str), fmt)
            return dt.strftime(output_format)
        except ValueError:
            continue
    
    # If all parsing attempts fail, try pandas
    try:
        dt = pd.to_datetime(date_str)
        return dt.strftime(output_format)
    except:
        return None


def clean_number(value: Any) -> Optional[float]:
    """Clean numeric value to ensure it's a proper number.
    
    Args:
        value: Value to clean
        
    Returns:
        float or None: Cleaned numeric value
    """
    if pd.isna(value) or value is None:
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Remove any non-numeric characters except decimal point
        value = re.sub(r'[^\d.-]', '', value)
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    return None


def generate_match_id(date: str, home_team: str, away_team: str) -> str:
    """Generate a unique match ID from date and team names.
    
    Args:
        date: Match date string (YYYY-MM-DD)
        home_team: Home team name
        away_team: Away team name
        
    Returns:
        str: Match ID
    """
    # Remove spaces and special characters
    home = re.sub(r'[^a-zA-Z0-9]', '', home_team.lower())
    away = re.sub(r'[^a-zA-Z0-9]', '', away_team.lower())
    
    # Create ID in format YYYYMMDD_HOME_AWAY
    date_part = date.replace('-', '')
    
    return f"{date_part}_{home}_{away}"


def join_fixtures_with_team_history(fixtures_df: pd.DataFrame, team_history_df: pd.DataFrame) -> pd.DataFrame:
    """Join fixtures with team history data.
    
    Args:
        fixtures_df: DataFrame containing upcoming fixtures
        team_history_df: DataFrame containing team historical data
        
    Returns:
        pd.DataFrame: Combined data with fixtures and team history
    """
    if fixtures_df.empty or team_history_df.empty:
        return pd.DataFrame()
    
    # Ensure team names are normalized in both DataFrames
    fixtures_df['home_team_norm'] = fixtures_df['home_team'].apply(normalize_team_name)
    fixtures_df['away_team_norm'] = fixtures_df['away_team'].apply(normalize_team_name)
    team_history_df['team_norm'] = team_history_df['team'].apply(normalize_team_name)
    
    # Create copies to avoid modifying the original DataFrames
    fixtures = fixtures_df.copy()
    history = team_history_df.copy()
    
    # Prepare home team stats
    home_stats = fixtures.merge(
        history,
        left_on='home_team_norm',
        right_on='team_norm',
        how='left',
        suffixes=('', '_home')
    )
    
    # Rename columns to prefix with 'home_'
    stats_cols = [c for c in history.columns if c not in ['team', 'team_norm', 'match_id', 'date']]
    for col in stats_cols:
        if col in home_stats.columns:
            home_stats = home_stats.rename(columns={col: f'home_{col}'})
    
    # Prepare away team stats
    away_stats = fixtures.merge(
        history,
        left_on='away_team_norm',
        right_on='team_norm',
        how='left',
        suffixes=('', '_away')
    )
    
    # Rename columns to prefix with 'away_'
    stats_cols = [c for c in history.columns if c not in ['team', 'team_norm', 'match_id', 'date']]
    for col in stats_cols:
        if col in away_stats.columns:
            away_stats = away_stats.rename(columns={col: f'away_{col}'})
    
    # Select relevant columns from home_stats
    home_cols = ['match_id', 'date'] + [f'home_{c}' for c in stats_cols if f'home_{c}' in home_stats.columns]
    home_df = home_stats[home_cols]
    
    # Select relevant columns from away_stats
    away_cols = ['match_id'] + [f'away_{c}' for c in stats_cols if f'away_{c}' in away_stats.columns]
    away_df = away_stats[away_cols]
    
    # Join the stats DataFrames
    joined = home_df.merge(away_df, on='match_id', how='left')
    
    # Merge with the original fixtures DataFrame to get all fixtures info
    final_df = fixtures.merge(joined, on=['match_id', 'date'], how='left')
    
    # Drop temporary columns
    final_df = final_df.drop(['home_team_norm', 'away_team_norm'], axis=1)
    
    return final_df


def validate_data(df: pd.DataFrame, required_cols: List[str]) -> Dict[str, Any]:
    """Validate a DataFrame by checking for required columns and missing values.
    
    Args:
        df: DataFrame to validate
        required_cols: List of required column names
        
    Returns:
        dict: Validation results with statistics
    """
    results = {
        "valid": True,
        "missing_columns": [],
        "missing_values": {},
        "record_count": len(df),
        "duplicate_ids": 0
    }
    
    # Check for required columns
    for col in required_cols:
        if col not in df.columns:
            results["missing_columns"].append(col)
            results["valid"] = False
    
    # If missing columns, return early
    if results["missing_columns"]:
        return results
    
    # Check for missing values in required columns
    for col in required_cols:
        missing = df[col].isna().sum()
        if missing > 0:
            results["missing_values"][col] = int(missing)
    
    # Check for duplicate match IDs if present
    if 'match_id' in df.columns:
        duplicate_ids = df['match_id'].duplicated().sum()
        results["duplicate_ids"] = int(duplicate_ids)
        if duplicate_ids > 0:
            results["valid"] = False
    
    return results


def aggregate_team_stats(team_history_df: pd.DataFrame, window_days: int = 90) -> pd.DataFrame:
    """Aggregate team statistics over a rolling window.
    
    Args:
        team_history_df: DataFrame containing team historical data
        window_days: Number of days to include in the rolling window
        
    Returns:
        pd.DataFrame: Aggregated team statistics
    """
    if team_history_df.empty:
        return pd.DataFrame()
    
    # Ensure date is in datetime format
    df = team_history_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by team and date
    df = df.sort_values(['team', 'date'])
    
    # Calculate rolling averages for each team
    teams = []
    
    for team, group in df.groupby('team'):
        # Create a date-indexed group for resampling
        date_indexed = group.set_index('date')
        
        # Calculate the rolling window
        numeric_cols = date_indexed.select_dtypes(include=[np.number]).columns
        
        # Calculate rolling averages
        window = f'{window_days}D'
        rolling_stats = date_indexed[numeric_cols].rolling(window, min_periods=1).mean()
        
        # Calculate rolling sum for goals and results
        if 'goals_for' in date_indexed.columns:
            rolling_stats['total_goals_for'] = date_indexed['goals_for'].rolling(window, min_periods=1).sum()
        
        if 'goals_against' in date_indexed.columns:
            rolling_stats['total_goals_against'] = date_indexed['goals_against'].rolling(window, min_periods=1).sum()
        
        if 'result' in date_indexed.columns:
            # Convert result to numeric win/draw/loss
            result_map = {'W': 1, 'D': 0.5, 'L': 0}
            if date_indexed['result'].dtype == 'object':
                date_indexed['result_numeric'] = date_indexed['result'].map(result_map)
            else:
                date_indexed['result_numeric'] = date_indexed['result']
                
            rolling_stats['win_ratio'] = date_indexed['result_numeric'].rolling(window, min_periods=1).mean()
        
        # Reset index to get date as a column
        rolling_stats = rolling_stats.reset_index()
        
        # Add team column
        rolling_stats['team'] = team
        
        teams.append(rolling_stats)
    
    # Combine all teams
    if teams:
        result = pd.concat(teams, ignore_index=True)
        return result
    
    return pd.DataFrame()