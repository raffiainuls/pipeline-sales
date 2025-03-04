SELECT 
    ts.*,
    tp.product_name,
    tp.category AS product_category,
    tp.sub_category AS sub_category_product,
    tp.price,
    tps.disc,
    tps.event_name AS disc_name,
    CAST(
        CASE 
            WHEN tps.disc IS NOT NULL 
            THEN (tp.price * ts.quantity) - (tp.price * ts.quantity) * COALESCE(tps.disc, 0) / 100
            ELSE tp.price * ts.quantity
        END AS BIGINT
    ) AS amount
FROM tbl_sales ts 
LEFT JOIN tbl_product tp ON tp.id = ts.product_id 
LEFT JOIN tbl_promotions tps ON tps.time = DATE(ts.order_date)
WHERE ts.order_status = 2 
    AND ts.payment_status = 2 
    AND (ts.shipping_status = 2 OR ts.shipping_status IS NULL)
