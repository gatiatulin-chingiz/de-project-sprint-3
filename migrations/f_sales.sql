insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select dc.date_id,
       item_id,
       customer_id,
       city_id,
       quantity,
       CASE
           WHEN status = 'refunded' THEN payment_amount * -1
           WHEN status = 'shipped' THEN payment_amount
       END AS payment_amount,
       CASE
           WHEN status = 'shipped' THEN 'shipped'
           WHEN status = 'refunded' THEN 'shipped'
       END AS status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';