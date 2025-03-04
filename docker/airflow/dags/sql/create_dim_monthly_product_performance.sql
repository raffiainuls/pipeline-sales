-- monthly_product_performance 
select 
fs2.product_id,
tp.product_name,
date(date_trunc('month', fs2.order_date)) as bulan,
sum(quantity)
from fact_sales fs2 
left join tbl_product tp 
on tp.id  = fs2.product_id 
group by 1,2,3
order by bulan desc, sum desc 