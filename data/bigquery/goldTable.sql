-- Entender como estão as vendas por categoria, produto e vendedor; 

create or replace table  projeto-e-commerce-484617.gold.rank_categoria
as (
with venda_total as (
select p.product_category_name as categoria
       ,sum(price) + sum(freight_value) AS total_venda
from projeto-e-commerce-484617.silver.itens_pedido ip
inner join projeto-e-commerce-484617.silver.produto p
on  p.product_id =  ip.product_id
inner join projeto-e-commerce-484617.silver.pedido pd
on ip.order_id =pd.order_id
where p.is_valid  = true
and  ip.is_valid = true
and order_status != 'canceled'
and p.is_active = true
group by categoria
)


select 
       categoria
      ,total_venda
      ,rank() over(order by total_venda desc) rank_categoria
from venda_total
order by total_venda desc

);


------------------------------------------------------------------------------------------

create or replace table  projeto-e-commerce-484617.gold.rank_vendedor
as (
with Venda_Total as (
select ip.seller_id as ID_Vendedor
       ,sum(price) + sum(freight_value)AS total_venda
from projeto-e-commerce-484617.silver.itens_pedido ip
inner join projeto-e-commerce-484617.silver.produto p
on  p.product_id =  ip.product_id
inner join projeto-e-commerce-484617.silver.pedido pd
on ip.order_id =pd.order_id
inner join projeto-e-commerce-484617.silver.vendedor v
on v.seller_id = ip.seller_id
where p.is_valid  = true
and  ip.is_valid = true
and order_status != 'canceled'
and v.is_active = true
group by ip.seller_id

)

select 
     id_vendedor
     ,total_venda
     ,rank() over(order by total_venda desc) Rank_Vendedor
from Venda_Total
);


------------------------------------------------------------------------------------------

-- Acompanhar o faturamento, bem como os custos por pedido (frete,taxas, etc.); 
create or replace table  projeto-e-commerce-484617.gold.faturamento
as (
select 
      ip.order_id as id_pedido 
      ,count(*) qtd_itens
      ,sum(ip.price) as valor_total_preco 
      ,sum(ip.freight_value) as valor_total_frete,
      sum(ip.price) + sum(ip.freight_value) as valor_total_pedido
from projeto-e-commerce-484617.silver.itens_pedido ip
inner join projeto-e-commerce-484617.silver.pedido p
on ip.order_id = p.order_id
where p.order_status != 'canceled'
and p.is_valid = true
group by ip.order_id 
order by valor_total_pedido desc
);

------------------------------------------------------------------------------------------

--- Avaliar a satisfação dos clientes, como base em avaliações de notas

create or replace table  projeto-e-commerce-484617.gold.avaliacao
as (
select a.order_id as id_pedido
      ,p.customer_id as id_cliente
      ,a.review_score as nota_avaliacao
      ,(CASE
       WHEN review_score = 1 THEN "horrivel"
       WHEN review_score = 2 THEN "ruim"
       WHEN review_score = 3 THEN "moderado"
       WHEN review_score = 4 THEN "bom" 
       ELSE "otimo"
       END) AS avaliacao
from projeto-e-commerce-484617.silver.avaliacao a
inner join projeto-e-commerce-484617.silver.pedido p
on a.order_id = p.order_id
where p.is_valid = true 
and a.is_valid = true
)
;

--------------------------------------------------------------------------------------------

-- Identificar regiões com maior volume de vendas e maiores atrasos de entrega

create or replace table  projeto-e-commerce-484617.gold.resumo_entrega_estado 
as (
select 
       c.customer_state as estado_cliente
       ,count(*) qtd_vendas

      ,sum(
            case 
            when p.order_delivered_customer_date > p.order_estimated_delivery_date then 1
            else 0
            end) as qtd_pedidos_atrasados
      ,round
       (sum 
            (case 
            when p.order_delivered_customer_date > p.order_estimated_delivery_date then 1
            else 0
            end) / count(*) * 100 ,2)as porcentual_atraso

      ,sum(f.valor_total_pedido) as valor_total_vendas

from projeto-e-commerce-484617.silver.pedido p
inner join projeto-e-commerce-484617.silver.cliente c
on c.customer_id = p.customer_id
inner join projeto-e-commerce-484617.gold.faturamento f
on f.id_pedido = p.order_id
where p.is_valid = true 
and c.is_valid = true
and p.order_status = 'delivered'
group by  c.customer_state

)
;
------------------------------------------------------------------------------------
-- Analisar as formas de pagamento mais utilizadas, além de  cancelamentos
create or replace table  projeto-e-commerce-484617.gold.pagamento_cancelado
as (
with total_pedidos as (
select payment_type as tipo_pagamento
      ,count(distinct order_id ) as total_pedidos
from projeto-e-commerce-484617.silver.pagamento
where is_valid = true 
group by payment_type
)

,
total_pedidos_cancelados as (
select  pg.payment_type as tipo_pagamento
        ,count(distinct pg.order_id) as total_pedidos_cancelados
from projeto-e-commerce-484617.silver.pagamento pg
inner join projeto-e-commerce-484617.silver.pedido p
on pg.order_id = p.order_id
where p.order_status = "canceled"
and pg.is_valid = true 
and p.is_valid = true 
group by pg.payment_type
)

select 
      t.tipo_pagamento
      ,t.total_pedidos
      ,coalesce(c.total_pedidos_cancelados,0) as total_pedidos_cancelados
      ,round(coalesce(c.total_pedidos_cancelados,0) / total_pedidos * 100,2) as taxa_cancelamento
from total_pedidos t
left join total_pedidos_cancelados c 
on t.tipo_pagamento = c.tipo_pagamento
order by taxa_cancelamento desc
);
--------------------------------------------------------------------------------------------
-- Comparar o desempenho ao longo do tempo, realizando análises mês a mês e ano a ano.

create or replace table  projeto-e-commerce-484617.gold.desempenho_ano_mes 
as (

with valor_total as (
select order_id
      ,sum(price) + sum(freight_value) as valor_total
  
from projeto-e-commerce-484617.silver.itens_pedido
group by order_id
) 
, 
periodo as (
  select 
         sum(valor_total) as total
         ,extract(year from order_purchase_timestamp) as ano
         ,extract(month from order_purchase_timestamp) as mes
        
  from valor_total v
  inner join projeto-e-commerce-484617.silver.pedido p
  on v.order_id = p.order_id
  group by extract(year from order_purchase_timestamp),extract(month from order_purchase_timestamp)
)

select  total
        ,ano
        ,mes
        ,lag(total) over (order by ano,mes) venda_mes_anterior
        ,lag(total) over(partition by mes order by ano)venda_ano_anterior
 from periodo
 order by ano,mes
);

