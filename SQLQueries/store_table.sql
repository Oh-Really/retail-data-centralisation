UPDATE dim_store_details
	SET latitude = 'N/A'

WHERE latitude IS NULL;

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);
