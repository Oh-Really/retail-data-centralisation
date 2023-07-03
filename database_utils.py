import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
        def __init__(self) -> None:
             #self.yaml_file = yaml_file
             pass

        def read_db_creds(self, yaml_file: str):
            '''Takes in a yaml file and return a dictionary representation'''
            with open(yaml_file, 'r') as read_file:
                contents = yaml.safe_load(read_file)
            return contents
        
        # Create a SQLAlchemy connection engine object
        def init_db_engine(self, cred):
             engine = create_engine(f"postgresql+psycopg2://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}")
             return engine        
        
        # List the tables available in this db
        def list_db_tables(self, engine):
             inspector = inspect(engine)
             return inspector.get_table_names()
        
        # Upload a df to the DB
        def upload_to_db(self, df, table_name:str, engine):
             df.to_sql(table_name, engine, if_exists='replace')