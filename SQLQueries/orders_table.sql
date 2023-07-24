ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(20),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(12),
    ALTER COLUMN product_quantity TYPE SMALLINT;
