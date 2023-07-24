UPDATE dim_products
SET product_price = REPLACE(
	    product_price,'£',''); 
		
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = 
	CASE
		WHEN weight < 2 THEN 'Light'
		WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
		WHEN weight >= 140 THEN 'Truck_Required'
	END; 
	
--SELECT MAX(LENGTH("product_price")) FROM dim_products;

ALTER TABLE dim_products
	RENAME removed TO still_available;
	
	
BEGIN TRANSACTION;

SAVEPOINT sp_alter;
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING
	CASE 
		WHEN still_available = 'Still_avaliable' THEN TRUE
		ELSE FALSE
	END;

COMMIT;

ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING date_added::date,
	ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
	ALTER COLUMN weight_class TYPE VARCHAR(14);
