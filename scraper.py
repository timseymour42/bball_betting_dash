import pandas as pd
from sbrscrape import Scoreboard
from datetime import datetime
from datetime import date
import boto3
from io import StringIO

SPORTSBOOKS = ['fanduel', 'betmgm', 'draftkings', 'bet365', 'pointsbet', 'bet_rivers_ny']
COLUMNS_TO_EXPAND = ['home_spread', 'away_spread', 'home_spread_odds', 'away_spread_odds', 'under_odds', 'over_odds', 'total', 'home_ml', 'away_ml']
SPORTS = ['NBA', 'NCAAB']

def expand_odds(dataframe, columns, sportsbooks):
    # Create a dictionary with keys as sportsbooks and NaN values
    sportsbook_template = {sb: None for sb in sportsbooks}
    
    # Iterate over the list of columns
    for column in columns:
        # Apply a lambda function to each cell to merge the sportsbook template with the cell's dictionary
        # This ensures that each cell has all possible sportsbooks
        dataframe[column] = dataframe[column].apply(
            lambda x: {k: v for k, v in x.items() if k in sportsbook_template} if isinstance(x, dict) else sportsbook_template
        )
        
        # Expand the column
        odds_df = dataframe[column].apply(pd.Series, dtype='float64')
        odds_df = odds_df.add_prefix(f"{column}_")
        
        # Join the new columns to the original dataframe and drop the original column
        dataframe = dataframe.join(odds_df).drop(column, axis=1)
    
    return dataframe

def scrape(columns=COLUMNS_TO_EXPAND, sportsbooks=SPORTSBOOKS, sports=SPORTS, date=date.today()):
    # Dictionary that contains lines for each game for that day
    games_dict = {}
    for sport in sports:
        scoreboard = Scoreboard(sport=sport, date=date)
        games = pd.DataFrame(scoreboard.games)
        games_df = expand_odds(games, columns, sportsbooks)
        # Storing the exact time the record was scraped
        games_df['time_scraped'] = datetime.now()
        games_dict[sport] = games_df
    return games_dict



def upload_to_s3(bucket_name, prefix, sport, data):
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    data.to_csv(csv_buffer)
    object_key = f'{prefix}{sport}/{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv'
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())

def main(event, context):
    sports = SPORTS
    bucket_name = 'sports-odds-data-collection-dev'
    prefix = 'sports_data/'  # Prefix within your S3 bucket for organization

    # Scrape the data
    games_dict = scrape(sports=sports, date=datetime.today())

    # Upload the data to S3
    for sport, games_df in games_dict.items():
        upload_to_s3(bucket_name, prefix, sport, games_df)

if __name__ == '__main__':
    main()