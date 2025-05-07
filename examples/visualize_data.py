"""
Data visualization script for the football data ETL pipeline results.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime, timedelta

# Add parent directory to path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config

# Set Seaborn style
sns.set_style("whitegrid")
plt.style.use('ggplot')


def load_data():
    """
    Load the pipeline output data files.
    
    Returns:
        tuple: (fixtures_df, team_history_df, combined_df)
    """
    fixtures_file = os.path.join(config.OUTPUT_DIR, "upcoming_fixtures.csv")
    team_history_file = os.path.join(config.OUTPUT_DIR, "team_history.csv") 
    combined_file = os.path.join(config.OUTPUT_DIR, "football_data.csv")
    
    fixtures_df = pd.DataFrame()
    team_history_df = pd.DataFrame()
    combined_df = pd.DataFrame()
    
    if os.path.exists(fixtures_file):
        fixtures_df = pd.read_csv(fixtures_file)
        print(f"Loaded {len(fixtures_df)} fixtures")
    else:
        print(f"Fixtures file not found: {fixtures_file}")
    
    if os.path.exists(team_history_file):
        team_history_df = pd.read_csv(team_history_file)
        print(f"Loaded {len(team_history_df)} team history records")
    else:
        print(f"Team history file not found: {team_history_file}")
    
    if os.path.exists(combined_file):
        combined_df = pd.read_csv(combined_file)
        print(f"Loaded {len(combined_df)} combined records")
    else:
        print(f"Combined file not found: {combined_file}")
    
    return fixtures_df, team_history_df, combined_df


def visualize_fixtures_by_league(fixtures_df):
    """
    Create a bar chart of fixtures by league.
    
    Args:
        fixtures_df: DataFrame of fixtures
    """
    if fixtures_df.empty:
        print("No fixtures data to visualize")
        return
    
    # Count fixtures by league
    league_counts = fixtures_df['league'].value_counts()
    
    # Create plot
    plt.figure(figsize=(12, 6))
    ax = league_counts.plot(kind='bar', color='skyblue')
    
    # Add labels and title
    plt.title('Upcoming Fixtures by League', fontsize=16)
    plt.xlabel('League', fontsize=12)
    plt.ylabel('Number of Fixtures', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    
    # Add count labels on bars
    for i, count in enumerate(league_counts):
        ax.text(i, count + 0.1, str(count), ha='center', fontweight='bold')
    
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(config.OUTPUT_DIR, "fixtures_by_league.png")
    plt.savefig(output_file, dpi=300)
    print(f"Saved fixtures by league chart to {output_file}")
    
    plt.close()


def visualize_fixtures_timeline(fixtures_df):
    """
    Create a timeline of fixtures.
    
    Args:
        fixtures_df: DataFrame of fixtures
    """
    if fixtures_df.empty:
        print("No fixtures data to visualize")
        return
    
    # Convert date to datetime
    fixtures_df['date'] = pd.to_datetime(fixtures_df['date'])
    
    # Count fixtures by date
    date_counts = fixtures_df.groupby('date').size()
    
    # Create plot
    plt.figure(figsize=(14, 6))
    ax = date_counts.plot(kind='line', marker='o', color='green', linewidth=2, markersize=8)
    
    # Add labels and title
    plt.title('Upcoming Fixtures Timeline', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Number of Fixtures', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Add count labels on points
    for i, (date, count) in enumerate(date_counts.items()):
        ax.text(date, count + 0.3, str(count), ha='center', fontweight='bold')
    
    # Format x-axis date labels
    plt.gcf().autofmt_xdate()
    
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(config.OUTPUT_DIR, "fixtures_timeline.png")
    plt.savefig(output_file, dpi=300)
    print(f"Saved fixtures timeline chart to {output_file}")
    
    plt.close()


def visualize_team_stats(team_history_df, top_n=10):
    """
    Create visualizations of team statistics.
    
    Args:
        team_history_df: DataFrame of team history
        top_n: Number of top teams to show
    """
    if team_history_df.empty:
        print("No team history data to visualize")
        return
    
    # Calculate team performance metrics
    team_metrics = team_history_df.groupby('team').agg({
        'result': lambda x: sum(1 for result in x if result == 'W') / len(x),  # Win ratio
        'goals_for': 'mean',
        'goals_against': 'mean',
        'shots': 'mean',
        'shots_on_target': 'mean'
    }).reset_index()
    
    # Rename columns
    team_metrics = team_metrics.rename(columns={
        'result': 'win_ratio',
        'goals_for': 'avg_goals_for',
        'goals_against': 'avg_goals_against',
        'shots': 'avg_shots',
        'shots_on_target': 'avg_shots_on_target'
    })
    
    # Calculate shot accuracy
    team_metrics['shot_accuracy'] = team_metrics['avg_shots_on_target'] / team_metrics['avg_shots']
    
    # Get top teams by win ratio
    top_teams = team_metrics.sort_values('win_ratio', ascending=False).head(top_n)
    
    # Create win ratio bar chart
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(x='team', y='win_ratio', data=top_teams, palette='viridis')
    
    # Add labels and title
    plt.title(f'Top {top_n} Teams by Win Ratio', fontsize=16)
    plt.xlabel('Team', fontsize=12)
    plt.ylabel('Win Ratio', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    
    # Add percentage labels on bars
    for i, row in enumerate(top_teams.itertuples()):
        ax.text(i, row.win_ratio + 0.01, f'{row.win_ratio:.1%}', ha='center', fontweight='bold')
    
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(config.OUTPUT_DIR, "team_win_ratios.png")
    plt.savefig(output_file, dpi=300)
    print(f"Saved team win ratios chart to {output_file}")
    
    plt.close()
    
    # Create goals scored vs. conceded scatter plot
    plt.figure(figsize=(12, 8))
    
    # Plot points
    scatter = plt.scatter(
        x=team_metrics['avg_goals_for'],
        y=team_metrics['avg_goals_against'],
        s=team_metrics['win_ratio'] * 500,  # Size based on win ratio
        c=team_metrics['shot_accuracy'],    # Color based on shot accuracy
        cmap='coolwarm',
        alpha=0.7
    )
    
    # Add team labels
    for i, row in team_metrics.iterrows():
        plt.annotate(
            row['team'],
            (row['avg_goals_for'], row['avg_goals_against']),
            fontsize=9,
            ha='center',
            va='center',
            xytext=(0, 10),
            textcoords='offset points'
        )
    
    # Add diagonal line (goals for = goals against)
    max_val = max(team_metrics['avg_goals_for'].max(), team_metrics['avg_goals_against'].max()) + 0.5
    plt.plot([0, max_val], [0, max_val], 'k--', alpha=0.3)
    
    # Add colorbar
    cbar = plt.colorbar(scatter)
    cbar.set_label('Shot Accuracy', fontsize=12)
    
    # Add labels and title
    plt.title('Team Performance: Goals Scored vs. Goals Conceded', fontsize=16)
    plt.xlabel('Average Goals Scored per Match', fontsize=12)
    plt.ylabel('Average Goals Conceded per Match', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.3)
    
    # Add explanatory text
    plt.text(
        0.05, 0.05,
        "Bubble size: Win ratio\nBubble color: Shot accuracy\nBelow diagonal line: Positive goal difference",
        transform=plt.gca().transAxes,
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.8)
    )
    
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(config.OUTPUT_DIR, "team_goals_analysis.png")
    plt.savefig(output_file, dpi=300)
    print(f"Saved team goals analysis chart to {output_file}")
    
    plt.close()


def visualize_combined_data(combined_df):
    """
    Create visualizations from the combined fixtures and team history data.
    
    Args:
        combined_df: DataFrame of combined data
    """
    if combined_df.empty:
        print("No combined data to visualize")
        return
    
    # Check if we have the necessary columns
    required_cols = ['home_team', 'away_team', 'home_win_ratio', 'away_win_ratio']
    if not all(col in combined_df.columns for col in required_cols):
        print("Combined data missing required columns for visualization")
        return
    
    # Create a 'match strength' metric
    combined_df['match_quality'] = (
        combined_df['home_win_ratio'].fillna(0.5) + 
        combined_df['away_win_ratio'].fillna(0.5)
    ) / 2
    
    # Create a heatmap of upcoming match quality
    plt.figure(figsize=(14, 10))
    
    # Prepare data for heatmap
    pivoted = combined_df.pivot_table(
        index='home_team',
        columns='away_team',
        values='match_quality',
        aggfunc='mean'
    )
    
    # Create heatmap
    sns.heatmap(
        pivoted,
        cmap='YlOrRd',
        annot=True,
        fmt='.2f',
        linewidths=0.5,
        cbar_kws={'label': 'Match Quality (Higher = Better Teams)'}
    )
    
    # Add labels and title
    plt.title('Upcoming Fixtures: Match Quality Heatmap', fontsize=16)
    plt.xlabel('Away Team', fontsize=12)
    plt.ylabel('Home Team', fontsize=12)
    
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(config.OUTPUT_DIR, "match_quality_heatmap.png")
    plt.savefig(output_file, dpi=300)
    print(f"Saved match quality heatmap to {output_file}")
    
    plt.close()


def main():
    """Main function to generate visualizations."""
    print("Generating visualizations from pipeline data...")
    
    # Load data
    fixtures_df, team_history_df, combined_df = load_data()
    
    # Generate visualizations
    if not fixtures_df.empty:
        visualize_fixtures_by_league(fixtures_df)
        visualize_fixtures_timeline(fixtures_df)
    
    if not team_history_df.empty:
        visualize_team_stats(team_history_df)
    
    if not combined_df.empty:
        visualize_combined_data(combined_df)
    
    print("\nAll visualizations complete!")


if __name__ == "__main__":
    main()