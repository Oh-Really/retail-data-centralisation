# multinational-retail-data-centralisation

In this project we create a local PostgreSQL database. We upload data from various sources, process it, create a database schema and run SQL queries. 

Key technologies used: Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas). 

## Project Utils

1. Data extraction. In "data_extraction.py" we store methods responsible for the upload of data into pandas data frame from different sources. 
2. Data cleaning. In "data_cleaning.py" we develop the class DataCleaning that clean different tables, which we uploaded in "data_extraction.py". 
3. Uploading data into the database. We write DatabaseConnector class "database_utils.py", which initiates the database engine based on credentials provided in ".yml" file.
4. "main.py" contains methods, which allow uploading data directly into the local database. 

## Step by Step Data Processing

We have 6 sources of data. 

1. Remote Postgres database in AWS Cloud. The table "order_table" is the data of the most interest for the client as it contains actual sales information. In the table, we need to use the following fields "date_uuid", "user_uuid", "card_number", "store_code", "product_code" and "product_quantity". The first 5 fields will become foreign keys in our database, therefore we need to clean these columns from all Nans and missing values. The "product_quantity" field has to be an integer.
2. Remote Postgres database in AWS Cloud. The user's data  "dim_users" table. This table is also stored in the remote database, so we use the same upload technics as in the previous case. The primary key here is the "user_uuid" field.
3. Public link in AWS cloud. The "dim_card_details" is accessible by a link from the s3 server and stored as a ".pdf" file. We handle reading ".pdf" using the "tabula" package. The primary key is the card number. The card number has to be converted into a string to avoid possible problems and cleaned from "?" artefacts.
4. The AWS-s3 bucket. The "dim_product" table. We utilise the boto3 package to download this data. The primary key is the "product code" field. The field "product_price" has to be converted into float number and the field "weight" has to convert into grams concerning cases like ("kg", "oz", "l", "ml").
5. The restful-API.  The "dim_store_details" data is available by the GET method. The ".json" response has to be converted into the pandas dataframe. The primary key field is "store_code".
6. The "dim_date_times" data is available by link. The ".json" response has to be converted into the pandas datagrame. The primary key is "date_uuid".

#### General Data Cleaning Notes

1. All data cleaning must be performed concerning the "primary key" field. Therefore, we remove rows of the table only in the case, if duplicates (NaNs, missing value etc) appear in this field. Otherwise, there is a risk that the "foreign key" in the "orders_table" will not be found in the "primary key" and the database schema would not work.
2. The date transformation has to account for different time formats, so we fix this issue in the following way
```
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
```
Once the clean data is loaded into the database, the data needs to be converted to the appropriate format and a few additional columns added with more information about the data.

Let's consider a typical workflow
1. Convert data fields
```
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE float USING product_price::double precision, 
	ALTER COLUMN weight TYPE float USING weight::double precision, 
	ALTER COLUMN product_code TYPE VARCHAR(255),
	ALTER COLUMN uuid TYPE uuid using uuid::uuid,
	ALTER COLUMN still_available Type Bool using still_available::boolean,
	ALTER COLUMN weight_class Type varchar(50),
	ALTER COLUMN "EAN" Type varchar(255),
```

2. Add foreign and primary keys in connected tables

```
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
ALTER TABLE orders_table 
	ADD FOREIGN KEY(product_code) 
	REFERENCES dim_products(product_code);
```
3. Create additional columns with conditional data segmentation. Here we want to have segments, which will help build store logistics based on product weight. Also, we want to remove string-based availability flags to proper boolean format
```
ALTER TABLE dim_products
	ADD weight_class VARCHAR(30);
UPDATE dim_products
	SET weight_class = 
		CASE 
			when weight/1000 < 2 then 'Light'
			when weight/1000 between 2 and 40 then 'Mid_Sized'
			when weight/1000 between 41 and 140 then 'Heavy'
			when weight/1000 > 140 then 'Truck_Required'  
		else 'Invalid' 
		END;
  
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;
  
UPDATE dim_products
	SET still_available = 
		CASE 
			when still_available = 'Still_available' then True
			when still_available = 'Removed' then False
		END;
```

