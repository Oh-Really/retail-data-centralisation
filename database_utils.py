import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
        def read_db_creds(yaml_file: str):
            '''Takes in a yaml file and return a dictionary representation'''
            with open(yaml_file, 'r') as read_file:
                contents = yaml.safe_load(read_file)
            return contents
    
        def init_db_engine():
             c = read_db_creds(yaml_file)
             engine = create_engine(f"postgresql+psycopg2://{c['RDS_USER']}:{c['RDS_PASSWORD']}@{c['RDS_HOST']}:{c['RDS_PORT']}/{c['RDS_DATABASE']}")
             return engine        
        
        def list_db_tables():
             inspector = inspect(engine)
             inspector.get_table_names()
