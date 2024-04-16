import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine



@task
def extract_data_test_sheet (dataset_url: str) -> pd.DataFrame:
    #importing the file
    df = pd.read_excel (dataset_url, parse_dates = True)
    return (df)

@task
def clean_data_test_sheet (df) -> pd.DataFrame:
    #changing datatypes
    df ['Numbers'] = df ['Numbers'].astype ('float')
    df ['Strings'] = df ['Strings'].astype ('string')
    return (df)

@task
def multiplying_numbers_column (df) -> pd.DataFrame:
    #multiplying the numbers by 2
    df ['Numbers'] = df ['Numbers'] * 2
    #making all columns lower case
    df.columns = df.columns.str.lower()
    return (df)

@task
def load_data_test_sheet (df) -> pd.DataFrame:
      df.to_csv (r'/Users/dustinhoye/Desktop/DataAnalysisStack/DataTables/PrefectTestTableOutput.csv')


@flow
def test_mac_etl():
    dataset_url = r'/Users/dustinhoye/Desktop/DataAnalysisStack/DataTables/PrefectTestTableInput.xlsx'
    df = extract_data_test_sheet (dataset_url) 
    df = clean_data_test_sheet (df)
    df = multiplying_numbers_column (df)
    df_load = load_data_test_sheet (df)
    
if __name__ == "__main__":
    test_mac_etl()
    




