
-- Criar tabela na camada silver  SCD2
create table if not exists projeto-e-commerce-484617.silver.cliente (
  customer_id STRING  
  ,customer_unique_id STRING
  ,customer_zip_code_prefix INT64
  ,customer_city STRING
  ,customer_state STRING
  ,updated_at TIMESTAMP 
  ,is_valid BOOL 
  ,start_date TIMESTAMP 
  ,end_date TIMESTAMP
  ,is_active BOOL 
);

 -- 2° Atualizar registros ativos existentes, caso haja alterações
MERGE INTO projeto-e-commerce-484617.silver.cliente as destino
USING (
  SELECT DISTINCT 
  customer_id 
  ,customer_unique_id 
  ,customer_zip_code_prefix 
  ,customer_city 
  ,customer_state 
  ,updated_at 
FROM projeto-e-commerce-484617.bronze.cliente) as origem 
ON destino.customer_id = origem.customer_id AND destino.is_active = true 

WHEN MATCHED AND 
           (
            destino.customer_unique_id IS DISTINCT FROM  origem.customer_unique_id OR  
            destino.customer_zip_code_prefix IS DISTINCT FROM  origem.customer_zip_code_prefix OR 
            destino.customer_city IS DISTINCT FROM  origem.customer_city OR 
            destino.customer_state IS DISTINCT FROM  origem.customer_state OR 
            destino.updated_at IS DISTINCT FROM  origem.updated_at
)
THEN UPDATE SET 
            destino.is_active = false,
            destino.end_date = current_timestamp();

--  3° Inserir registros novos ou atualizados
MERGE INTO projeto-e-commerce-484617.silver.cliente as destino 
USING (
  SELECT DISTINCT 
  customer_id 
  ,customer_unique_id 
  ,customer_zip_code_prefix 
  ,customer_city 
  ,customer_state 
  ,updated_at 
  ,(CASE 
        WHEN customer_id IS NULL OR 
        customer_unique_id IS NULL OR 
        customer_zip_code_prefix IS NULL OR 
        customer_city IS NULL OR 
        customer_state IS NULL THEN FALSE
        ELSE TRUE
      END) AS is_valid
  FROM projeto-e-commerce-484617.bronze.cliente) as origem 
  ON destino.customer_id = origem.customer_id AND destino.is_active = true 
  
  WHEN NOT MATCHED THEN 
  INSERT (
    customer_id 
    ,customer_unique_id 
    ,customer_zip_code_prefix 
    ,customer_city 
    ,customer_state 
    ,updated_at 
    ,is_valid 
    ,start_date 
    ,end_date 
    ,is_active 
  )
  VALUES (
    origem.customer_id 
    ,origem.customer_unique_id 
    ,origem.customer_zip_code_prefix 
    ,origem.customer_city 
    ,origem.customer_state 
    ,origem.updated_at 
    ,origem.is_valid 
    ,current_timestamp() 
    ,null
    ,true
  );
  
--------------------------------------------------------------------------------------------------
-- tabela vendedor  SCD2
create table if not exists projeto-e-commerce-484617.silver.vendedor (
  seller_id STRING
  ,seller_zip_code_prefix INT64
  ,seller_city STRING
  ,seller_state STRING
  ,updated_at TIMESTAMP
  ,is_valid BOOL
  ,start_date TIMESTAMP
  ,end_date TIMESTAMP
  ,is_active BOOL
);

MERGE INTO projeto-e-commerce-484617.silver.vendedor AS destino
USING(
  select distinct 
   seller_id 
  ,seller_zip_code_prefix 
  ,seller_city
  ,seller_state 
  ,updated_at 

FROM projeto-e-commerce-484617.bronze.vendedor) as origem
ON destino.seller_id = origem.seller_id  and destino.is_active = true 

WHEN MATCHED AND (
  destino.seller_zip_code_prefix IS DISTINCT FROM origem.seller_zip_code_prefix OR 
  destino.seller_city IS DISTINCT FROM origem.seller_city OR 
  destino.seller_state IS DISTINCT FROM origem.seller_state 
  
)

THEN UPDATE SET 
    destino.is_active = false,
    destino.end_date = current_timestamp();


MERGE INTO projeto-e-commerce-484617.silver.vendedor AS destino
USING (
  select distinct 
   seller_id 
  ,seller_zip_code_prefix 
  ,seller_city
  ,seller_state 
  ,updated_at 
  , (CASE 
  WHEN seller_id IS NULL OR
  seller_zip_code_prefix IS NULL OR 
  seller_city IS NULL OR 
  seller_state IS NULL THEN FALSE
  ELSE TRUE 
  END) AS is_valid
  FROM projeto-e-commerce-484617.bronze.vendedor) AS origem
  ON destino.seller_id = origem.seller_id  and destino.is_active = true 

WHEN NOT MATCHED THEN 
INSERT (
   seller_id 
  ,seller_zip_code_prefix 
  ,seller_city
  ,seller_state 
  ,updated_at 
  ,is_valid
  ,start_date
  ,end_date
  ,is_active
)
VALUES (
   origem.seller_id 
  ,origem.seller_zip_code_prefix 
  ,origem.seller_city
  ,origem.seller_state 
  ,origem.updated_at 
  ,origem.is_valid
  ,CURRENT_TIMESTAMP()
  ,NULL
  ,TRUE
);

---------------------------------------------------------------------------------------------------
-- Tabela pedido SCD1


create table if not exists projeto-e-commerce-484617.silver.pedido (
  order_id STRING
 ,customer_id STRING
 ,order_status STRING
 ,order_purchase_timestamp timestamp
 ,order_approved_at timestamp
 ,order_delivered_carrier_date timestamp  
 ,order_delivered_customer_date timestamp
 ,order_estimated_delivery_date timestamp
 ,updated_at TIMESTAMP
 ,is_valid BOOL

);

MERGE INTO projeto-e-commerce-484617.silver.pedido as destino 
USING(
  select 
   order_id 
 ,customer_id
 ,order_status 
 ,order_purchase_timestamp 
 ,order_approved_at 
 ,order_delivered_carrier_date 
 ,order_delivered_customer_date 
 ,order_estimated_delivery_date 
 ,updated_at 
 ,(
      CASE 
      WHEN order_id IS NULL OR
      customer_id IS NULL OR 
      order_purchase_timestamp  IS NULL
      THEN FALSE 
      ELSE TRUE 
  END
  
  ) AS is_valid
  FROM projeto-e-commerce-484617.bronze.pedido) as origem
 ON destino.order_id = origem.order_id 
WHEN  MATCHED AND (
  destino.customer_id IS DISTINCT FROM  origem.customer_id OR
  destino.order_status  IS DISTINCT FROM  origem.order_status  OR
  destino.order_purchase_timestamp  IS DISTINCT FROM  origem.order_purchase_timestamp  OR
  destino.order_approved_at  IS DISTINCT FROM  origem.order_approved_at  OR
  destino.order_delivered_carrier_date  IS DISTINCT FROM  origem.order_delivered_carrier_date  OR
  destino.order_delivered_customer_date  IS DISTINCT FROM  origem.order_delivered_customer_date  OR
  destino.order_estimated_delivery_date  IS DISTINCT FROM  origem.order_estimated_delivery_date  
)
 
THEN UPDATE SET 
   
  destino.customer_id= origem.customer_id
 ,destino.order_status = origem.order_status 
 ,destino.order_purchase_timestamp = origem.order_purchase_timestamp 
 ,destino.order_approved_at = origem.order_approved_at 
 ,destino.order_delivered_carrier_date = origem.order_delivered_carrier_date 
 ,destino.order_delivered_customer_date = origem.order_delivered_customer_date 
 ,destino.order_estimated_delivery_date = origem.order_estimated_delivery_date 
 ,destino.updated_at = origem.updated_at 
 ,destino.is_valid  = origem.is_valid 

WHEN NOT MATCHED THEN
INSERT (
  order_id 
 ,customer_id
 ,order_status 
 ,order_purchase_timestamp 
 ,order_approved_at 
 ,order_delivered_carrier_date 
 ,order_delivered_customer_date 
 ,order_estimated_delivery_date 
 ,updated_at 
 ,is_valid 

)
VALUES(
  origem.order_id 
 ,origem.customer_id
 ,origem.order_status 
 ,origem.order_purchase_timestamp 
 ,origem.order_approved_at 
 ,origem.order_delivered_carrier_date 
 ,origem.order_delivered_customer_date 
 ,origem.order_estimated_delivery_date 
 ,origem.updated_at 
 ,origem.is_valid 
 );



------------------------------------------------------------------------------
-- Tabela geolocalizacao  -- TABELA FULL LOAD

create table if not exists projeto-e-commerce-484617.silver.geolocalizacao (
  geo_id INT64
 ,geolocation_zip_code_prefix INT64
,geolocation_lat FLOAT64
,geolocation_lng FLOAT64
,geolocation_city STRING
,geolocation_state STRING
,updated_at TIMESTAMP
,is_valid BOOL
);

TRUNCATE TABLE projeto-e-commerce-484617.silver.geolocalizacao;

INSERT INTO projeto-e-commerce-484617.silver.geolocalizacao
SELECT *,
(CASE
WHEN 
geo_id IS NULL OR 
geolocation_zip_code_prefix IS NULL OR
geolocation_lat IS NULL OR
geolocation_lng  IS NULL OR
geolocation_city IS NULL OR
geolocation_state IS NULL 
THEN FALSE
ELSE TRUE
END) AS is_valid
FROM projeto-e-commerce-484617.bronze.geolocalizacao
;

---------------------------------------------------------------
-- Tabela pagamento SCD1

create table if not exists projeto-e-commerce-484617.silver.pagamento (
  order_id STRING
  ,payment_sequential INT64
  ,payment_type STRING
  ,payment_installments INT64
  ,payment_value NUMERIC 
  ,updated_at TIMESTAMP
  ,is_valid BOOL
);
MERGE INTO projeto-e-commerce-484617.silver.pagamento AS destino 
USING (
  select order_id 
  ,payment_sequential 
  ,payment_type 
  ,payment_installments 
  ,CAST(payment_value AS NUMERIC) AS payment_value
  ,updated_at 
  ,(CASE 
  WHEN
  order_id IS NULL OR
  payment_sequential IS NULL OR
  payment_type IS NULL OR
  payment_installments IS NULL OR
  payment_value IS NULL OR
  updated_at IS NULL
  THEN FALSE
  ELSE TRUE
  END) as is_valid
FROM projeto-e-commerce-484617.bronze.pagamento) AS origem
ON destino.order_id = origem.order_id
AND destino.payment_sequential = origem.payment_sequential

WHEN MATCHED AND (
  destino.payment_type         IS DISTINCT FROM origem.payment_type OR
  destino.payment_installments IS DISTINCT FROM origem.payment_installments OR
  destino.payment_value        IS DISTINCT FROM origem.payment_value 
)
THEN 
UPDATE SET 
  destino.payment_type = origem.payment_type 
  ,destino.payment_installments = origem.payment_installments 
  ,destino.payment_value = origem.payment_value 
  ,destino.updated_at = origem.updated_at 
  ,destino.is_valid   = origem.is_valid             

WHEN NOT MATCHED THEN
INSERT(
  order_id 
  ,payment_sequential 
  ,payment_type 
  ,payment_installments 
  ,payment_value 
  ,updated_at 
  ,is_valid 
)
VALUES (
   origem.order_id 
  ,origem.payment_sequential 
  ,origem.payment_type 
  ,origem.payment_installments 
  ,origem.payment_value 
  ,origem.updated_at 
  ,origem.is_valid 
)
;


-----------------------------------------------------------------
-- Tabela avaliacao - SDC1 INCREMENTAL
create table if not exists projeto-e-commerce-484617.silver.avaliacao(
review_id STRING
,order_id STRING
,review_score INT64
,review_comment_title STRING
,review_comment_message STRING
,review_creation_date timestamp
,review_answer_timestamp timestamp
,updated_at TIMESTAMP
,is_valid BOOL

);
MERGE INTO projeto-e-commerce-484617.silver.avaliacao AS destino 
USING(select 
review_id 
,order_id 
,review_score 
,review_comment_title 
,review_comment_message 
,review_creation_date 
,review_answer_timestamp 
,updated_at 
,(
  CASE 
  WHEN 
  review_id IS NULL OR 
  order_id IS NULL OR 
  review_score IS NULL 
  THEN FALSE
  ELSE TRUE
  END
) AS is_valid
FROM projeto-e-commerce-484617.bronze.avaliacao) AS origem
ON  destino.review_id = origem.review_id
AND destino.order_id = origem.order_id

WHEN MATCHED AND (
destino.review_score IS DISTINCT FROM origem.review_score OR
destino.review_comment_title IS DISTINCT FROM origem.review_comment_title OR
destino.review_comment_message IS DISTINCT FROM origem.review_comment_message OR
destino.review_creation_date IS DISTINCT FROM origem.review_creation_date OR
destino.review_answer_timestamp  IS DISTINCT FROM origem.review_answer_timestamp
)
THEN 
UPDATE SET 
 destino.review_score = origem.review_score 
,destino.review_comment_title = origem.review_comment_title 
,destino.review_comment_message = origem.review_comment_message 
,destino.review_creation_date = origem.review_creation_date 
,destino.review_answer_timestamp = origem.review_answer_timestamp
,destino.updated_at  = origem.updated_at
,destino.is_valid = origem.is_valid

WHEN NOT MATCHED THEN 
INSERT(
  review_id 
  ,order_id 
  ,review_score 
  ,review_comment_title 
  ,review_comment_message 
  ,review_creation_date 
  ,review_answer_timestamp 
  ,updated_at 
  ,is_valid 

)
VALUES (
  origem.review_id 
  ,origem.order_id 
  ,origem.review_score 
  ,origem.review_comment_title 
  ,origem.review_comment_message 
  ,origem.review_creation_date 
  ,origem.review_answer_timestamp
  ,origem.updated_at 
  ,origem.is_valid   
);


---------------------------------------------------------------------------------------------
-- Tabela categoria  - SCD1 FULL LOAD 

create table if not exists projeto-e-commerce-484617.silver.categoria (
  product_category_name STRING
 ,product_category_name_english STRING
 ,updated_at TIMESTAMP
 ,is_valid BOOL
);

TRUNCATE TABLE projeto-e-commerce-484617.silver.categoria;

INSERT INTO projeto-e-commerce-484617.silver.categoria
SELECT *,
  (
    CASE
    WHEN 
    product_category_name IS NULL OR 
    product_category_name_english  IS NULL 
    THEN FALSE
    ELSE TRUE 
    END
  ) AS is_valid
FROM projeto-e-commerce-484617.bronze.categoria;


-------------------------------------------------------------
-- Tabela produto -  SCD 2

create table if not exists projeto-e-commerce-484617.silver.produto (
   product_id STRING
    ,product_category_name STRING
    ,product_name_lenght FLOAT64
    ,product_description_lenght FLOAT64
    ,product_photos_qty FLOAT64
    ,product_weight_g FLOAT64
    ,product_length_cm FLOAT64
    ,product_height_cm FLOAT64
    ,product_width_cm FLOAT64
    ,updated_at TIMESTAMP
    ,is_valid BOOL
    ,start_date TIMESTAMP
    ,end_date TIMESTAMP
    ,is_active BOOL
);

MERGE INTO projeto-e-commerce-484617.silver.produto AS destino
USING (SELECT * 
FROM projeto-e-commerce-484617.bronze.produto) AS origem 
ON destino.product_id = origem.product_id
AND destino.is_active= true

WHEN MATCHED AND
( destino.product_category_name IS DISTINCT FROM origem.product_category_name OR 
  destino.product_name_lenght IS DISTINCT FROM  origem.product_name_lenght OR 
  destino.product_photos_qty IS DISTINCT FROM  origem.product_photos_qty OR 
  destino.product_weight_g IS DISTINCT FROM  origem.product_weight_g OR 
  destino.product_height_cm IS DISTINCT FROM  origem.product_height_cm OR 
  destino.product_width_cm IS DISTINCT FROM  origem.product_width_cm OR
  destino.product_length_cm IS DISTINCT FROM  origem.product_length_cm OR
  destino.product_description_lenght IS DISTINCT FROM  origem.product_description_lenght 
)
THEN UPDATE SET 
destino.is_active= false, 
destino.end_date = current_timestamp();

MERGE INTO projeto-e-commerce-484617.silver.produto AS destino
USING (
  SELECT *,
  (CASE 
  WHEN 
    product_id IS NULL OR
    product_category_name IS NULL 
    THEN FALSE 
    ELSE TRUE
    END) AS is_valid
FROM projeto-e-commerce-484617.bronze.produto) AS origem 
ON destino.product_id = origem.product_id
AND destino.is_active= true

WHEN NOT MATCHED THEN
INSERT (
    product_id 
    ,product_category_name 
    ,product_name_lenght 
    ,product_description_lenght 
    ,product_photos_qty 
    ,product_weight_g 
    ,product_length_cm 
    ,product_height_cm 
    ,product_width_cm
    ,updated_at 
    ,is_valid 
    ,start_date 
    ,end_date
    ,is_active 
)
VALUES(
origem.product_id 
,origem.product_category_name 
,origem.product_name_lenght 
,origem.product_description_lenght 
,origem.product_photos_qty 
,origem.product_weight_g 
,origem.product_length_cm 
,origem.product_height_cm 
,origem.product_width_cm
,origem.updated_at 
,origem.is_valid 
,current_timestamp() 
,NULL
,true
);


----------------------------------------

-- Tabela itens_pedido SCD 1

create table if not exists projeto-e-commerce-484617.silver.itens_pedido (
   order_id STRING
  ,order_item_id INT64
  ,product_id STRING
  ,seller_id STRING
  ,shipping_limit_date timestamp
  ,price NUMERIC
  ,freight_value NUMERIC
  ,updated_at TIMESTAMP
  ,is_valid BOOL
);
MERGE INTO projeto-e-commerce-484617.silver.itens_pedido AS destino
USING (
 select 
   order_id 
  ,order_item_id 
  ,product_id 
  ,seller_id 
  ,shipping_limit_date 
  ,CAST(price AS NUMERIC) AS price
  ,CAST(freight_value AS NUMERIC) AS freight_value
  ,updated_at              
  ,(CASE 
 WHEN
  order_id IS NULL OR
  order_item_id IS NULL OR
  product_id IS NULL OR
  shipping_limit_date IS NULL OR
  price IS NULL OR
  freight_value  IS NULL
  THEN FALSE
  ELSE TRUE
  END ) AS is_valid
FROM projeto-e-commerce-484617.bronze.itens_pedido ) AS origem
ON  destino.order_id = origem.order_id 
AND destino.order_item_id = origem.order_item_id

WHEN MATCHED AND (
  destino.product_id IS DISTINCT FROM  origem.product_id OR 
  destino.seller_id IS DISTINCT FROM  origem.seller_id OR
  destino.shipping_limit_date IS DISTINCT FROM  origem.shipping_limit_date OR
  destino.price IS DISTINCT FROM  origem.price OR
  destino.freight_value  IS DISTINCT FROM  origem.freight_value 
)
THEN 
UPDATE SET 
   destino.product_id = origem.product_id 
  ,destino.seller_id = origem.seller_id 
  ,destino.shipping_limit_date = origem.shipping_limit_date 
  ,destino.price = origem.price 
  ,destino.freight_value = origem.freight_value 
  ,destino.updated_at = origem.updated_at              
  ,destino.is_valid = origem.is_valid
  
WHEN NOT MATCHED THEN
INSERT(
  order_id 
  ,order_item_id 
  ,product_id 
  ,seller_id 
  ,shipping_limit_date 
  ,price 
  ,freight_value 
  ,updated_at              
  ,is_valid
)
VALUES (
   origem.order_id 
  ,origem.order_item_id 
  ,origem.product_id 
  ,origem.seller_id 
  ,origem.shipping_limit_date 
  ,origem.price 
  ,origem.freight_value 
  ,origem.updated_at              
  ,origem.is_valid 
);














