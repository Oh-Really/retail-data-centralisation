import database_utils

class DataExtractor:
    def read_rds_table(self, ):
        database = database_utils.DatabaseConnector('db_creds.yaml')

