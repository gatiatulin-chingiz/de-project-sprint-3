DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count int,
    returning_customers_count int,
    refunded_customer_count int,
    period_name varchar,
    period_id int,
    new_customers_revenue int,
    returning_customers_revenue int,
    customers_refunded int
);


INSERT INTO mart.f_customer_retention (new_customers_count)
with new_customers_count AS (
    select count(customer_id) new_customers_count
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) = 1
    )
SELECT count(new_customers_count)
FROM new_customers_count;


INSERT INTO mart.f_customer_retention (returning_customers_count) 
with returning_customers_count AS (
    select count(customer_id) returning_customers_count
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) > 1
    )
SELECT count(returning_customers_count)
FROM returning_customers_count;


INSERT INTO mart.f_customer_retention (refunded_customer_count) 
with refunded_customer_count AS (
    select count(customer_id) refunded_customer_count
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
      AND payment_amount < 0
    GROUP BY customer_id
    )
SELECT count(refunded_customer_count)
FROM refunded_customer_count;


INSERT INTO mart.f_customer_retention (period_name) VALUES ('weekly');


INSERT INTO mart.f_customer_retention (period_id) 
    select week_of_year AS period_id
    from mart.d_calendar
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)


INSERT INTO mart.f_customer_retention (new_customers_revenue) 
with new_customers_revenue AS (
    select sum(payment_amount) new_customers_revenue
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) = 1
    )
SELECT sum(new_customers_revenue)
FROM new_customers_revenue;


INSERT INTO mart.f_customer_retention (returning_customers_revenue) 
with returning_customers_revenue AS (
    select sum(payment_amount) returning_customers_revenue
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) > 1
    )
SELECT sum(returning_customers_revenue)
FROM returning_customers_revenue;


INSERT INTO mart.f_customer_retention (customers_refunded) 
    select count(payment_amount) customers_refunded
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    AND payment_amount < 0;
