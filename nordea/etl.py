import pandas as pd
from nordea.transform import Transformer


class MatchesEtl:
    """Class for ETL process for matches file
    """
    
    def __init__(self,data_path='data',input_file_name : str='source/output.csv') -> None:
        """_summary_

        Args:
            data_path (str, optional): Path to data files. Defaults to 'data'.
            input_file_name (str, optional): Source file to be imported. Defaults to 'source/output.csv'.
        """
        self.input_file_name = input_file_name
        self.transformer = Transformer(data_path=data_path)


    def extract_teams(self) -> Transformer:
        """_summary_

        Returns:
            Transformer: Instance of Transformer class
        """
        columns = ['team_id','team_name']
        self.transformer.load(self.input_file_name) \
            .select_columns(columns) \
            .unique_rows() \
            .rename_columns({'team_id': 'Team Id', 'team_name': 'Team Name'}) \
            .save('output/team.json',format='jsonl')
        return self
 
    def extract_matches(self) -> Transformer:
        """Extract match.json file

        Returns:
            Transformer: Instance of Transformer class
        """
        
        #home
        columns = ['match_id','match_name','team_id','goals_scored','minutes_played'] 
        self.transformer.load(self.input_file_name) \
        .filter_df('is_home == True') \
        .select_columns(columns) \
        .rename_columns({'team_id': 'home_team_id','goals_scored': 'home_goals', 'minutes_played': 'home_minutes_played' }) \
        .sum(['match_id','match_name','home_team_id'], ['home_goals','home_minutes_played']) \
        .save('transformed/trf_home_matches.csv')
    
        #away
        columns = ['match_id','team_id','goals_scored','minutes_played'] 
        self.transformer.load(self.input_file_name) \
        .filter_df('is_home == False') \
        .select_columns(columns) \
        .rename_columns({'team_id': 'away_team_id','goals_scored': 'away_goals','minutes_played': 'away_minutes_played' }) \
        .sum(['match_id','away_team_id'], ['away_goals','away_minutes_played']) \
        .save('transformed/trf_away_matches.csv')
        return self

    def extract_players(self) -> Transformer:
        """Extract player.json file

        Returns:
            Transformer: Instance of Transformer class
        """
        columns = ['player_id','team_id','player_name']
        self.transformer.load(self.input_file_name) \
            .select_columns(columns) \
            .unique_rows() \
            .rename_columns({'player_id': 'Player Id', 'team_id': 'Team Id', 'player_name': 'Player Name'}) \
            .save('output/player.json',format='jsonl')
        return self


    def transpose_matches(self) -> Transformer:
        """Join matches files

        Returns:
            Transformer: Instance of Transformer class
        """
        total_goals = lambda x:  x['home_goals'] + x['away_goals']
        self.transformer.join_datasets('transformed/trf_home_matches.csv',
                                       'transformed/trf_away_matches.csv',
                                       'match_id') \
        .custom('total_goals',total_goals) \
        .save('transformed/trf_matches.csv',index=True)

        return self

    def extract_stats(self) -> Transformer:
        """Extract stats file

        Returns:
            Transformer: Instance of Transformer class
        """
    
        columns = 'match_id,player_id,goals_scored,minutes_played'.split(',')
        self.transformer.load(self.input_file_name) \
            .select_columns(columns) \
            .save('transformed/trf_stats.csv')
        return self
    
    def export_matches(self) -> Transformer:
        """Export match.json file

        Returns:
            Transformer: Instance of Transformer class
        """
        columns = 'match_id,match_name,home_team_id,home_goals,away_team_id,away_goals'.split(',')
        columns_renamed = {
            'match_id': 'Match Id',
            'match_name': 'Match Name',
            'home_team_id': 'Home Team Id',
            'home_goals': 'Home Goals',
            'away_team_id': 'Away Team Id',
            'away_goals': 'Away Goals'
        }

        self.transformer.load('transformed/trf_matches.csv') \
            .select_columns(columns) \
            .rename_columns(columns_renamed) \
            .save('output/match.json',format='jsonl')
            
    
    def export_stats(self) -> Transformer:
        """Export stats file

        Returns:
            Transformer: Instance of Transformer class
        """
        columns_to_drop = ['match_name','home_team_id','home_goals','home_minutes_played','away_team_id','away_goals','away_minutes_played','total_goals']
        columns_renamed = {
                           "player_id":'Player Id',
                           "match_id": 'Match Id',
                           "goals_scored":'Goals Scored',
                           "minutes_played":'Minutes Played',
                           "fraction_played":'Fraction of total minutes played',
                           "fraction_goals_scored":'Fraction of total goals scored'
                          }

        self.transformer.join_datasets('transformed/trf_stats.csv','transformed/trf_matches.csv','match_id') \
            .custom('fraction_played',lambda_function=lambda x: round(x['minutes_played'] / 90,3)) \
            .custom('fraction_goals_scored',lambda_function=lambda x: round(x['goals_scored'] / x['total_goals'],3)) \
            .drop_columns(columns_to_drop) \
            .rename_columns(columns_renamed) \
            .add_incremental_id('Stat Id') \
            .save('output/statistic.json',format='jsonl')
    
    
    def run(self):
        """Run the full pipeline
        """
        #teams
        self.extract_teams() 

        #players
        self.extract_players()

        #matches
        self.extract_matches().transpose_matches().export_matches()

        #stats
        self.extract_stats().export_stats()

