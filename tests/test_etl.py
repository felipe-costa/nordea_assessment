from nordea.etl import MatchesEtl
import os
import pytest


@pytest.fixture
def matches_etl() -> MatchesEtl:
    """Create instance of MatchesEtl class

    Returns:
        MatchesEtl: Instance of MatchesEtl class
    """
    matches_etl = MatchesEtl(data_path='tests/data',input_file_name='seeds/all_matches_from_team_12.csv')
    return matches_etl

def test_01_extract_teams(matches_etl : MatchesEtl):
    """Test if teams extraction was succed

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    file_name = 'team.json'

    #act
    matches_etl.extract_teams()

    #assert
    assert os.path.exists(f"tests/data/output/{file_name}"), 'Transform file not found'


def test_02_extract_matches(matches_etl):
    """Test matchs extractions

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    home_file_name = 'trf_home_matches.csv'
    away_file_name = 'trf_away_matches.csv'


    #act
    matches_etl.extract_matches()

    #assert
    assert os.path.exists(f"tests/data/transformed/{home_file_name}"), 'Transform file for home matches not found'
    assert os.path.exists(f"tests/data/transformed/{away_file_name}"), 'Transform file for away matches not found'


def test_03_extract_players(matches_etl):
    """Test players extractions

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    file_name = 'player.json'

    #act
    matches_etl.extract_players()

    #assert
    assert os.path.exists(f"tests/data/output/{file_name}"), 'Transform file not found'

def test_04_transpose_matches(matches_etl):
    """Test transpose matches

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    file_name = 'trf_matches.csv'
    #act
    matches_etl.transpose_matches()

    #assert
    assert os.path.exists(f"tests/data/transformed/{file_name}"), 'Transposed file not found'


def test_05_extract_stats(matches_etl):
    """Test extract stats

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    file_name = 'trf_stats.csv'
    #act
    matches_etl.extract_stats()

    #assert
    assert os.path.exists(f"tests/data/transformed/{file_name}"), 'Transformed file not found'

def test_06_export_stats(matches_etl):
    """Test export stats

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    file_name = 'statistic.json'
    #act
    matches_etl.export_stats()
    #assert
    assert os.path.exists(f"tests/data/output/{file_name}"), 'Stats file not found'

def test_07_export_matches(matches_etl):
    """Test export matches

    Args:
        matches_etl (MatchesEtl): Instance of MatchesEtl class
    """
    #arrange
    file_name = 'match.json'
    #act
    matches_etl.export_matches()
    #assert
    assert os.path.exists(f"tests/data/output/{file_name}"), 'Match file not found'