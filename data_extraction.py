# %%
import database_utils
import pandas as pd

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, DatabaseConnector: type[database_utils.DatabaseConnector], table_name: str) -> pd.DataFrame:
        '''
        Arguments:
        DatabaseConenctor: instance of the DatabaseConenctor class
        table_name: Which table one is looking to extract from the db
        ------
        Return: DataFrame object of table_name
        '''
        while True:
            if table_name not in DatabaseConnector.list_db_tables():
                print(f"Table name not in database. Please select from following options: \n")
                print(DatabaseConnector.list_db_tables())
                break           
            else:
                user_data = pd.read_sql_table(table_name, DatabaseConnector.init_db_engine(), index_col='index')
            #print(user_data.head())
            return user_data

database = database_utils.DatabaseConnector('db_creds.yaml')
table = DataExtractor()
df = table.read_rds_table(database, 'legacy_users')    


# if __name__ == '__main__':
#     main()

# %%
df.head()


# %%
df['company'].value_counts()

# %%
null_index = df[df['company'] == 'NULL']
print(null_index)


# %%
null_index['first_name'].info

# %%
df.loc[867]
# %%
