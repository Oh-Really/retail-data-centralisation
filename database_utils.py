import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
        def __init__(self, yaml_file) -> None:
             self.yaml_file = yaml_file

        def read_db_creds(self, yaml_file: str):
            '''Takes in a yaml file and return a dictionary representation'''
            with open(yaml_file, 'r') as read_file:
                contents = yaml.safe_load(read_file)
            return contents
        
        def init_db_engine(self):
             c = self.read_db_creds(self.yaml_file)
             engine = create_engine(f"postgresql+psycopg2://{c['RDS_USER']}:{c['RDS_PASSWORD']}@{c['RDS_HOST']}:{c['RDS_PORT']}/{c['RDS_DATABASE']}")
             return engine        
        
        def list_db_tables(self):
             inspector = inspect(self.init_db_engine())
             return inspector.get_table_names()

database = DatabaseConnector('db_creds.yaml')