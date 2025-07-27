import pandas as pd 

def clean_data(df):
    """
    Cleans the input DataFrame by removing rows with missing values and converting age to integer.
    """
    df = df.dropna()  # Remove rows with missing values
    df["age"] = df["age"].astype(int)  # Convert age to integer
    return df

def load_data():
    """
    Loads data into a DataFrame. 
    """
    data = {
        'name': ['Alice', 'Bob', 'Charlie', None],
        'age': [25, 30, 22,13]
    }
    return pd.DataFrame(data)