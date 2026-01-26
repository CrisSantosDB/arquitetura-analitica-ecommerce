-- Tabela cliente

create external table if not exists projeto-e-commerce-484617.bronze.cliente (
customer_id STRING  
,customer_unique_id STRING
,customer_zip_code_prefix INT64
,customer_city STRING
,customer_state STRING
,updated_at TIMESTAMP
)
OPTIONS(
  FORMAT = "JSON",
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/crm.cliente/*.json"]
);

-- Tabela vendedor
create external table if not exists projeto-e-commerce-484617.bronze.vendedor(
seller_id STRING
,seller_zip_code_prefix INT64
,seller_city STRING
,seller_state STRING
,updated_at TIMESTAMP
)
OPTIONS(
  FORMAT = "JSON",
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/crm.vendedor/*.json"]
);

-- Tabela pedido
create external table if not exists projeto-e-commerce-484617.bronze.pedido (
 order_id STRING
 ,customer_id STRING
 ,order_status STRING
 ,order_purchase_timestamp timestamp
 ,order_approved_at timestamp
 ,order_delivered_carrier_date timestamp  
 ,order_delivered_customer_date timestamp
 ,order_estimated_delivery_date timestamp
 ,updated_at TIMESTAMP
)
OPTIONS(
  FORMAT = "JSON",
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/vendas.pedido/*.json"]
);

-- Tabela geolocalizacao
create external table if not exists projeto-e-commerce-484617.bronze.geolocalizacao(
  geo_id INT64
 ,geolocation_zip_code_prefix INT64
,geolocation_lat FLOAT64
,geolocation_lng FLOAT64
,geolocation_city STRING
,geolocation_state STRING
,updated_at TIMESTAMP
)
OPTIONS(
  FORMAT = "JSON",
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/logistica.geolocalizacao/*.json"]
);

-- Tabela pagamento
create external table if not exists projeto-e-commerce-484617.bronze.pagamento (
  order_id STRING
,payment_sequential INT64
,payment_type STRING
,payment_installments INT64
,payment_value FLOAT64
,updated_at TIMESTAMP

)
OPTIONS(
  FORMAT = "JSON",
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/vendas.pagamento/*.json"]
);

-- Tabela avaliacao
create external table if not exists projeto-e-commerce-484617.bronze.avaliacao (
review_id STRING
,order_id STRING
,review_score INT64
,review_comment_title STRING
,review_comment_message STRING
,review_creation_date timestamp
,review_answer_timestamp timestamp
,updated_at TIMESTAMP
)
OPTIONS(
  FORMAT="JSON",
  URIS =["gs://datalake-ecommerce-2026/landing/ecommerce_db/analise.avaliacao/*.json"]
);

-- Tabela categoria 
create external table if not exists projeto-e-commerce-484617.bronze.categoria (
 product_category_name STRING
 ,product_category_name_english STRING
 ,updated_at TIMESTAMP
)
OPTIONS(
  FORMAT = "JSON",
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/catalogo.categoria/*.json"]
)
;

-- Tsbela produto 
create external table if not exists projeto-e-commerce-484617.bronze.produto (
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
)
OPTIONS(
  FORMAT=("JSON"),
  URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/catalogo.produto/*.json"]
)
;

-- Tabela orde_items
create external table if not exists projeto-e-commerce-484617.bronze.itens_pedido (
  order_id STRING
  ,order_item_id INT64
  ,product_id STRING
  ,seller_id STRING
  ,shipping_limit_date timestamp
  ,price FLOAT64
  ,freight_value FLOAT64
  ,updated_at TIMESTAMP
  )
  OPTIONS(
    FORMAT = "JSON",
    URIS = ["gs://datalake-ecommerce-2026/landing/ecommerce_db/vendas.itens_pedido/*.json"]
  )
  ;
