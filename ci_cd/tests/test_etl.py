import pandas as pd
from etl import clean_data, load_data

def test_clean_data_remove_nulls():
    """
    Test that clean_data removes rows with missing values.
    """
    df = load_data()
    cleaned_df = clean_data(df)
    assert cleaned_df.isnull().sum().sum() == 0, "DataFrame should not contain null values after cleaning."

def test_clean_data_has_integer_age():
    """
    Test that clean_data converts age to integer.
    """
    df = load_data()
    cleaned_df = clean_data(df)
    assert cleaned_df['age'].dtype == 'int64', "Age column should be of type int64 after cleaning."