import pytest
import os
from nordea.transform import Transformer

def test_01_class_tranform():
    """Test if Transformer class exists
    """
    assert Transformer() is not None, 'Transform class does not exists'

@pytest.fixture
def data_transform() -> object:
    """Fixture to create the instance of Transformer class and load the initial dataset

    Returns:
        Transformer: returns the current instance of transformer class
    """
    columns = ['team_id','team_name']
    t = Transformer(data_path='tests/data')
    t.load('seeds/team_12.csv').select_columns(columns)
    return t

def test_02_load_staging(data_transform : Transformer):
    """Test if dataframe was loaded correctly

    Args:
        data_transform (Transformer): Instance of Transformer class
    """
    #assert
    assert data_transform.df is not None, 'Dataframe does not exists'

def test_unique_rows(data_transform : Transformer):
    """Test if the unique_rows method is working

    Args:
        data_transform (Transformer): Instance of Transformer class
    """
    data_transform.unique_rows()
    #assert
    assert not data_transform.df.duplicated().any(), 'Duplicated rows found'

def test_filter_df(data_transform : Transformer):
    """Test if filter is working

    Args:
        data_transform (Transformer): Instance of Transformer class
    """
    #act
    data_transform.filter_df('team_id == 17')
    #assert
    #Dataframe should be empty since seed file have only data from team 12
    assert data_transform.df.query('team_id != 17').empty, 'Dataframe not filtered'

def test_rename_columns(data_transform : Transformer):
    """_summary_

    Args:
        data_transform (Transformer): _description_
    """

    #act
    data_transform.rename_columns({'team_id': 'Team Id'})
    #assert
    assert not 'team_id' is data_transform.df.columns and 'Team Id' in data_transform.df.columns, 'Column not renamed'

def test_custom(data_transform : Transformer):
    """Test if the custom transformations are working

    Args:
        data_transform (Transformer): Instance of Transformer class
    """
    data_transform.custom('new_column',lambda x: x['team_name'])
    assert 'new_column' in data_transform.df.columns, 'Custom transformation did not worked'

def test_sum():
    """Test if the sum method is working
    """
    #arrange
    columns = ['match_id','match_name','goals_scored']
    t = Transformer(data_path='tests/data/seeds')

    aggregation_columns = ['goals_scored']
    group_columns = ['match_id','match_name']
    t.load('team_12.csv').select_columns(columns).filter_df('match_id == 412')

    #act
    t.sum(group_columns,aggregation_columns)

    #assert
    goals_scored = t.df['goals_scored'][0]
    assert goals_scored == 4,f'Sum is not correct. Correct value is {goals_scored}'

def test_save(data_transform : Transformer):
    """Test if the saving method is working

    Args:
        data_transform (Transformer): Instance of Transformer class
    """
    file_name = 'transformed/trf_teams.csv'
    data_transform.save(file_name)
    assert os.path.exists(f"{data_transform.data_path}/{file_name}"), 'File does not exist'
