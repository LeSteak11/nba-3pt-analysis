"""
NBA 3-Point Shot Data Collection Script

This script fetches all 3-point attempts for a given NBA season using the nba_api package.
It retrieves shot chart data for all active players and saves the results as a Parquet file.

Author: Jake
Date: November 12, 2025
"""

import argparse
import re
import sys
import time
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from nba_api.stats.endpoints import shotchartdetail, commonallplayers
from nba_api.stats.static import teams


def get_player_list(season):
    """
    Fetch all active player IDs for a given season.
    
    Parameters:
    -----------
    season : str
        NBA season in format "2022-23"
    
    Returns:
    --------
    list of dict
        List of player dictionaries containing player_id and player_name
    """
    print(f"Fetching player list for {season} season...")
    
    # Query all players for the season
    # Season format for API: "2022-23"
    players_data = commonallplayers.CommonAllPlayers(
        season=season,
        is_only_current_season=1  # Only active players in this season
    ).get_data_frames()[0]
    
    # Filter to only include players (exclude teams)
    active_players = players_data[players_data['ROSTERSTATUS'] == 1].copy()
    
    player_list = [
        {
            'player_id': row['PERSON_ID'],
            'player_name': row['DISPLAY_FIRST_LAST']
        }
        for _, row in active_players.iterrows()
    ]
    
    print(f"Found {len(player_list)} active players")
    return player_list


def get_player_shots(player_id, player_name, season):
    """
    Fetch all shot attempts for a specific player in a given season.
    
    Parameters:
    -----------
    player_id : int
        NBA player ID
    player_name : str
        Player's full name (for logging purposes)
    season : str
        NBA season in format "2022-23"
    
    Returns:
    --------
    pd.DataFrame
        DataFrame containing shot chart data, or empty DataFrame if no data
    """
    try:
        # Fetch shot chart details
        shot_data = shotchartdetail.ShotChartDetail(
            team_id=0,  # 0 fetches all teams
            player_id=player_id,
            season_nullable=season,
            season_type_all_star='Regular Season',
            context_measure_simple='FGA'  # Field Goal Attempts
        )
        
        # Get the shot data DataFrame
        shots_df = shot_data.get_data_frames()[0]
        
        if not shots_df.empty:
            # Add player name for reference
            shots_df['PLAYER_NAME'] = player_name
        
        return shots_df
    
    except Exception as e:
        print(f"  ⚠️  Error fetching data for {player_name} (ID: {player_id}): {str(e)}")
        return pd.DataFrame()


def fetch_three_point_data(season="2022-23", rate_limit_seconds=0.6):
    """
    Main function to fetch all 3-point attempts for a season.
    
    Parameters:
    -----------
    season : str
        NBA season in format "2022-23"
    rate_limit_seconds : float
        Time to wait between API requests (default: 0.6 seconds)
    
    Returns:
    --------
    pd.DataFrame
        Combined DataFrame with all 3-point attempts
    """
    print(f"\n{'='*60}")
    print(f"NBA 3-Point Data Collection Pipeline")
    print(f"Season: {season}")
    print(f"{'='*60}\n")
    
    # Step 1: Get all active players
    player_list = get_player_list(season)
    
    # Step 2: Fetch shots for each player
    all_three_pointers = []
    
    print(f"\nFetching shot data for {len(player_list)} players...")
    print("(This may take a while due to rate limiting)\n")
    
    for player in tqdm(player_list, desc="Progress", unit="player"):
        player_id = player['player_id']
        player_name = player['player_name']
        
        # Fetch shot data
        shots_df = get_player_shots(player_id, player_name, season)
        
        # Filter for 3-point attempts only
        if not shots_df.empty:
            # Filter where SHOT_TYPE contains '3PT'
            three_pointers = shots_df[shots_df['SHOT_TYPE'] == '3PT Field Goal'].copy()
            
            if not three_pointers.empty:
                all_three_pointers.append(three_pointers)
        
        # Rate limiting - be polite to the API
        time.sleep(rate_limit_seconds)
    
    # Step 3: Combine all data
    if all_three_pointers:
        combined_df = pd.concat(all_three_pointers, ignore_index=True)
        print(f"\n✓ Successfully collected {len(combined_df):,} three-point attempts")
        return combined_df
    else:
        print("\n⚠️  No three-point attempts found")
        return pd.DataFrame()


def save_to_parquet(df, output_path):
    """
    Save DataFrame to Parquet file format.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame to save
    output_path : str or Path
        Path where the Parquet file will be saved
    """
    # Ensure the output directory exists
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save as Parquet
    df.to_parquet(output_path, engine='pyarrow', index=False)
    print(f"✓ Data saved to: {output_path}")


def validate_season_format(season):
    """
    Validate that the season string follows the correct "YYYY-YY" format.
    
    Parameters:
    -----------
    season : str
        Season string to validate
    
    Returns:
    --------
    str
        The validated season string
    
    Raises:
    -------
    argparse.ArgumentTypeError
        If the season format is invalid
    """
    pattern = r'^\d{4}-\d{2}$'
    if not re.match(pattern, season):
        raise argparse.ArgumentTypeError(
            f"Invalid season format: '{season}'. Expected format: 'YYYY-YY' (e.g., '2024-25')"
        )
    
    # Additional validation: check that years are consecutive
    years = season.split('-')
    start_year = int(years[0])
    end_year_short = int(years[1])
    expected_end = start_year % 100 + 1
    
    if end_year_short != expected_end:
        raise argparse.ArgumentTypeError(
            f"Invalid season: '{season}'. Years must be consecutive (e.g., '2024-25', not '2024-26')"
        )
    
    return season


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
    --------
    argparse.Namespace
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Fetch NBA three-point shot data for a given season',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/fetch_data.py
  python scripts/fetch_data.py --season "2023-24"
  python scripts/fetch_data.py --season "2021-22"
        """
    )
    
    parser.add_argument(
        '--season',
        type=validate_season_format,
        default='2024-25',
        help='NBA season in format "YYYY-YY" (default: 2024-25)'
    )
    
    return parser.parse_args()


def main():
    """
    Main execution function.
    """
    # Parse command-line arguments
    args = parse_arguments()
    season = args.season
    
    # Print season banner
    print(f"\n🏀 Fetching three-point data for {season} season...\n")
    
    # Generate output filename from season (e.g., "2024-25" -> "nba_3pt_2024_25.parquet")
    season_filename = season.replace('-', '_')
    output_path = Path(__file__).parent.parent / "data" / "raw" / f"nba_3pt_{season_filename}.parquet"
    
    # Fetch the data
    three_point_df = fetch_three_point_data(season=season)
    
    # Save to Parquet if data was collected
    if not three_point_df.empty:
        save_to_parquet(three_point_df, output_path)
        
        print(f"\n{'='*60}")
        print(f"Pipeline Complete!")
        print(f"Total 3-point attempts collected: {len(three_point_df):,}")
        print(f"Output file: {output_path}")
        print(f"{'='*60}\n")
    else:
        print("\n⚠️  No data to save.")


if __name__ == "__main__":
    main()
