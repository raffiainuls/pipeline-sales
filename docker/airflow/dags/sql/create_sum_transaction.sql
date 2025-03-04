-- sum_transaction
with 
-- income for offline store 
iofs as (
select 
'income' as type,
id as sales_id,
branch_id,
null as employee_id,
'Penjualan ' || product_name || ' sejumlah ' || quantity as description,
order_date as date,
amount
from fact_sales 
where is_online_transaction = 'false'
), 
-- income for online_store
ions as(
select 
'income' as type,
id as sales_id,
branch_id,
null as employee_id,
'Penjualan ' || product_name || ' sejumlah ' || quantity as description,
order_date as date,
(price * quantity) as amount
from fact_sales
where is_online_transaction = 'true'
),
--outcome_ongkir_online_store
oos as (
select 
'outcome' as type,
id as sales_id,
branch_id,
null as employee_id,
'Pengeluaran untuk biaya ongkir' as description,
order_date as date,
delivery_fee as amount
from fact_sales
where is_free_delivery_fee = 'true'),
-- outcome diskon sales
ods as(
select 
'outcome' as type,
id as sales_id,
branch_id,
null as employee_id,
'Pengeluaran diskon ' || disc_name  as description,
order_date as date,
(price * quantity) * disc/100 as amount
from fact_sales
where disc is not null
)
select
* 
from iofs
union
select 
* 
from ions
union 
select 
*
from oos
union
select 
* 
from ods