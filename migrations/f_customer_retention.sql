DELETE FROM mart.f_customer_retention WHERE period_id = DATE_PART('week', '{{ds}}'::date);

INSERT INTO mart.f_customer_retention (new_customers_count, returning_customers_count, refunded_customer_count,
                                        period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
    SELECT
        count(DISTINCT CASE WHEN order_count = 1 THEN customer_id END  new_customers_count,
        count(distinct CASE WHEN order_count > 1 THEN customer_id END  returning_customers_count,
        count(DISTINCT CASE WHEN refunded_amount > 0 THEN customer_id END  refunded_customer_count,
        'weekly' period_name,
        EXTRACT(week FROM max(order_date)) period_id,
        item_id,
        sum(DISTINCT CASE WHEN order_count = 1 THEN total_payment END) new_customers_revenue,
        sum(DISTINCT CASE WHEN order_count > 1 THEN total_payment END) returning_customers_revenue,
        sum(DISTINCT CASE WHEN refunded_amount > 0 THEN total_payment END) customers_refunded
    FROM (SELECT 
            uol.customer_id,
            uol.item_id,
            count(uol.uniq_id) order_count,
            count(DISTINCT CASE WHEN uol.status = 'refunded' THEN uol.uniq_id END) refunded_amount,
            sum(fs.payment_amount) total_payment,
            max(uol.date_time) AS order_date
            FROM staging.user_order_log uol
        JOIN mart.f_sales fs 
            USING(customer_id, item_id)
        GROUP BY uol.customer_id, uol.item_id) t
    GROUP BY item_id
    HAVING max(order_date)::Date = '{{ds}}';