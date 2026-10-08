-- Q1 

-- QUERY:
/* 
SELECT 
	e.emp_id, 
	e.name,
	e.department, 
	e.salary 
FROM employees e 
left join employees m on e.emp_id = m.manager_id
where m.emp_id is null;
*/

-- ANSWER
emp_id|name       |department |salary   |
------+-----------+-----------+---------+
    11|Gizem Su   |Sales      | 73000.00|
    12|Hasan Duman|Engineering| 87000.00|
    10|Fatma Yurt |Engineering|105000.00|
     2|Elif Beyaz |Engineering| 88000.00|
     5|Can Deniz  |Marketing  | 72000.00|
     8|Derya Ulu  |Sales      | 68000.00|
     6|Selin Tas  |Marketing  | 72000.00|
     3|Mehmet Gul |Engineering| 95000.00|
     9|Emre Ak    |Sales      | 71000.00|


-- Q2

-- QUERY:
/* 
SELECT  
	e.name,
	m.name, 
	m.salary,
	case 
		when e.salary > m.salary then true
		else false 
	end as earns_more_than_manager
FROM employees e 
join employees m 
	on e.manager_id = m.emp_id;
*/

-- ANSWER
name       |name      |salary  |earns_more_than_manager|
-----------+----------+--------+-----------------------+
Elif Beyaz |Ahmet Kara|95000.00|false                  |
Mehmet Gul |Ahmet Kara|95000.00|false                  |
Can Deniz  |Zeynep Dag|78000.00|false                  |
Selin Tas  |Zeynep Dag|78000.00|false                  |
Derya Ulu  |Burak Koc |82000.00|false                  |
Emre Ak    |Burak Koc |82000.00|false                  |
Gizem Su   |Burak Koc |82000.00|false                  |
Hasan Duman|Ahmet Kara|95000.00|false                  |


-- Q3

-- QUERY:
/* 
SELECT
    c.customer_id,
    c.first_name,
    c.last_name
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.order_date >= '2026-01-01'
  AND o.order_date < '2026-02-01'

INTERSECT

SELECT
    c.customer_id,
    c.first_name,
    c.last_name
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.order_date >= '2026-02-01'
  AND o.order_date < '2026-03-01';
*/

-- ANSWER
customer_id|first_name|last_name|
-----------+----------+---------+
          1|Alice     |Yilmaz   |
          2|Bob       |Kaya     |
          3|Charlie   |Demir    |
          4|Diana     |Celik    |
          4|Diana     |Celik    |
          5|Eve       |Arslan   |
          7|Grace     |Sahin    |
          8|Hakan     |Yildiz   |
          8|Hakan     |Yildiz   |
          9|Irem      |Aydin    |
         10|Jack      |Korkmaz  |


-- Q4

-- QUERY:
/* 
WITH ranked_orders AS (
    SELECT 
        c.first_name,
        o.order_id,
        o.total_amount,

        DENSE_RANK() OVER (
            PARTITION BY c.customer_id
            ORDER BY o.total_amount DESC
        ) AS order_rank,

        COUNT(*) OVER (
            PARTITION BY c.customer_id
        ) AS order_count

    FROM customers c
    JOIN orders o 
        ON c.customer_id = o.customer_id
)
SELECT *
FROM ranked_orders
WHERE order_rank <= 2
  AND order_count > 1;
*/

-- ANSWER
first_name|order_id|total_amount|order_rank|order_count|
----------+--------+------------+----------+-----------+
Alice     |       1|     1329.98|         1|          3|
Alice     |       2|       79.98|         2|          3|
Bob       |       3|      149.99|         1|          3|
Bob       |       5|       89.99|         2|          3|
Charlie   |       6|      249.98|         1|          2|
Charlie   |      19|       34.98|         2|          2|
Diana     |       8|      199.99|         1|          3|
Diana     |       7|       49.99|         2|          3|
Eve       |      11|      129.98|         1|          3|
Eve       |      20|       89.99|         2|          3|
Hakan     |      14|      299.98|         1|          2|
Hakan     |      15|       49.99|         2|          2|



-- Q5

-- QUERY:
/* 
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_amount) AS revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (
        ORDER BY month
    ) AS previous_revenue,
    (
        (revenue - LAG(revenue) OVER (ORDER BY month))
        / LAG(revenue) OVER (ORDER BY month)
    ) * 100 AS revenue_change_pct
FROM monthly_revenue;
*/

-- ANSWER
month                  |revenue|previous_revenue|revenue_change_pct      |
-----------------------+-------+----------------+------------------------+
2026-01-01 00:00:00.000|1974.88|                |                        |
2026-02-01 00:00:00.000|2474.85|         1974.88| 25.31647492505873774600|
2026-03-01 00:00:00.000|  89.99|         2474.85|-96.36382002949673717600|


-- Q6

-- QUERY:
/*
select
	c.first_name,
	o.order_date,
	o.total_amount,
	sum(o.total_amount) over (
		partition by o.customer_id
		order by o.order_date
		rows between unbounded preceding and current row
	) as running_total
	
from customers c
join orders o
	on c.customer_id = o.customer_id 
order by c.customer_id, o.order_date;
*/

-- ANSWER
first_name|order_date             |total_amount|running_total|
----------+-----------------------+------------+-------------+
Alice     |2026-01-05 10:30:00.000|     1329.98|      1329.98|
Alice     |2026-01-12 14:15:00.000|       79.98|      1409.96|
Alice     |2026-02-22 09:30:00.000|       59.99|      1469.95|
Bob       |2026-01-08 09:00:00.000|      149.99|       149.99|
Bob       |2026-01-20 16:45:00.000|       29.99|       179.98|
Bob       |2026-02-03 11:00:00.000|       89.99|       269.97|
Charlie   |2026-01-15 13:20:00.000|      249.98|       249.98|
Charlie   |2026-02-25 14:00:00.000|       34.98|       284.96|
Diana     |2026-01-18 17:00:00.000|       49.99|        49.99|
Diana     |2026-02-01 08:30:00.000|      199.99|       249.98|
Diana     |2026-02-10 12:00:00.000|       39.99|       289.97|
Eve       |2026-01-22 15:30:00.000|       59.98|        59.98|
Eve       |2026-02-05 10:00:00.000|      129.98|       189.96|
Eve       |2026-03-01 10:00:00.000|       89.99|       279.95|
Frank     |2026-01-25 14:00:00.000|       24.99|        24.99|
Grace     |2026-02-08 09:45:00.000|       89.99|        89.99|
Hakan     |2026-02-12 11:30:00.000|      299.98|       299.98|
Hakan     |2026-02-15 16:00:00.000|       49.99|       349.97|
Irem      |2026-02-18 10:15:00.000|     1299.99|      1299.99|
Jack      |2026-02-20 13:45:00.000|      179.98|       179.98|


-- Q7

-- QUERY:
/*
select 
	e.name,
	e.department,
	e.salary,
	round(
		percent_rank() over (
			partition by e.department
			order by e.salary)::numeric, 3 
		) as dept_pct_rank
from employees e;
*/

-- ANSWER
name       |department |salary   |dept_pct_rank|
-----------+-----------+---------+-------------+
Hasan Duman|Engineering| 87000.00|        0.000|
Elif Beyaz |Engineering| 88000.00|        0.250|
Ahmet Kara |Engineering| 95000.00|        0.500|
Mehmet Gul |Engineering| 95000.00|        0.500|
Fatma Yurt |Engineering|105000.00|        1.000|
Selin Tas  |Marketing  | 72000.00|        0.000|
Can Deniz  |Marketing  | 72000.00|        0.000|
Zeynep Dag |Marketing  | 78000.00|        1.000|
Derya Ulu  |Sales      | 68000.00|        0.000|
Emre Ak    |Sales      | 71000.00|        0.333|
Gizem Su   |Sales      | 73000.00|        0.667|
Burak Koc  |Sales      | 82000.00|        1.000|


-- Q8   

-- QUERY:
/*
WITH customer_totals AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS full_name,
        COUNT(o.order_id) AS order_count,
        COALESCE(SUM(o.total_amount), 0) AS total_spent
    FROM customers c
    LEFT JOIN orders o 
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
),
customer_tiers as (
	select 
	full_name,
	order_count,
	total_spent,
	case
		when total_spent >= 1000 then 'VIP'
		when total_spent >= 200 then 'Regular'
		else 'Low-Value'
	end as customer_tier
	from customer_totals
)
SELECT 
    customer_tier,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_spent
FROM customer_tiers
GROUP BY customer_tier;
*/

-- ANSWER
customer_tier|customer_count|avg_spent            |
-------------+--------------+---------------------+
VIP          |             2|1384.9700000000000000|
Low-Value    |             5|  58.9920000000000000|
Regular      |             5| 294.9640000000000000|


-- Q9

-- QUERY:
/*
with recursive factorials as (
	select 1 as n, 1 as factorial
	union all
	select 	n + 1,
		factorial * (n + 1)
		from factorials 
		where n <  10
)
select * 
from factorials;
*/

-- ANSWER
n |factorial|
--+---------+
 1|        1|
 2|        2|
 3|        6|
 4|       24|
 5|      120|
 6|      720|
 7|     5040|
 8|    40320|
 9|   362880|
10|  3628800|


-- Q10

-- QUERY:
/*
SELECT
    c.first_name || ' ' || c.last_name AS customer_name,
    o.order_id,
    o.total_amount,
    (
        SELECT AVG(o2.total_amount)
        FROM orders o2
        WHERE o2.customer_id = o.customer_id
    ) AS customer_avg
FROM orders o
JOIN customers c
    ON c.customer_id = o.customer_id
WHERE o.total_amount > (
    SELECT AVG(o2.total_amount)
    FROM orders o2
    WHERE o2.customer_id = o.customer_id
);
*/

-- ANSWER
customer_name|order_id|total_amount|customer_avg        |
-------------+--------+------------+--------------------+
Alice Yilmaz |       1|     1329.98|489.9833333333333333|
Bob Kaya     |       3|      149.99| 89.9900000000000000|
Charlie Demir|       6|      249.98|142.4800000000000000|
Diana Celik  |       8|      199.99| 96.6566666666666667|
Eve Arslan   |      11|      129.98| 93.3166666666666667|
Hakan Yildiz |      14|      299.98|174.9850000000000000|


-- Q11

-- QUERY:
/*
CREATE TEMP TABLE raw_events AS
SELECT *
FROM user_events

UNION ALL

SELECT *
FROM user_events;

SELECT DISTINCT ON (user_id, event_type, event_time)
    *
FROM raw_events
ORDER BY user_id, event_type, event_time;
*/

-- ANSWER
event_id|user_id|event_type |event_time             |
--------+-------+-----------+-----------------------+
       2|      1|add_to_cart|2026-02-01 09:05:30.000|
       4|      1|checkout   |2026-02-01 09:15:45.000|
       1|      1|page_view  |2026-02-01 09:00:00.000|
       3|      1|page_view  |2026-02-01 09:12:00.000|
       5|      1|page_view  |2026-02-01 14:00:00.000|
       6|      1|page_view  |2026-02-01 14:03:20.000|
       9|      2|add_to_cart|2026-02-01 10:10:15.000|
       7|      2|page_view  |2026-02-01 10:00:00.000|
       8|      2|page_view  |2026-02-01 10:08:00.000|
      11|      2|page_view  |2026-02-01 16:30:00.000|
      10|      2|remove_cart|2026-02-01 10:12:00.000|
      13|      3|add_to_cart|2026-02-01 11:02:00.000|
      14|      3|checkout   |2026-02-01 11:05:00.000|
      16|      3|checkout   |2026-02-02 09:10:00.000|
      12|      3|page_view  |2026-02-01 11:00:00.000|
      15|      3|page_view  |2026-02-02 09:00:00.000|


-- Q12

-- QUERY:
/*
SELECT
    'phone' AS column_name,
    COUNT(*) FILTER (WHERE phone IS NULL) AS null_count,
    COUNT(*) AS total_count,
    ROUND(
        COUNT(*) FILTER (WHERE phone IS NULL)::numeric
        / COUNT(*) * 100,
        1
    ) AS null_percentage
FROM customers

UNION ALL

SELECT
    'city' AS column_name,
    COUNT(*) FILTER (WHERE city IS NULL) AS null_count,
    COUNT(*) AS total_count,
    ROUND(
        COUNT(*) FILTER (WHERE city IS NULL)::numeric
        / COUNT(*) * 100,
        1
    ) AS null_percentage
FROM customers;
*/

-- ANSWER
column_name|null_count|total_count|null_percentage|
-----------+----------+-----------+---------------+
phone      |         6|         12|           50.0|
city       |         0|         12|            0.0|


-- Q13

-- QUERY:
/*
SELECT
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
    COUNT(*) FILTER (WHERE status = 'shipped') AS shipped,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
    COUNT(*) AS total
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
*/



-- ANSWER
month                  |completed|shipped|pending|cancelled|total|
-----------------------+---------+-------+-------+---------+-----+
2026-01-01 00:00:00.000|        8|      0|      0|        0|    8|
2026-02-01 00:00:00.000|        6|      3|      1|        1|   11|
2026-03-01 00:00:00.000|        0|      0|      1|        0|    1|


-- Q14

-- QUERY:
/*
WITH category_revenue AS (
    SELECT
        p.category,
        SUM(oi.quantity * oi.unit_price) AS revenue
    FROM order_items oi
    JOIN products p
        ON p.product_id = oi.product_id
    GROUP BY p.category
)
SELECT
    category,
    revenue,
    ROUND(
        revenue / SUM(revenue) OVER () * 100,
        2
    ) AS revenue_contribution_pct,
    ROUND(
        SUM(revenue) OVER (
            ORDER BY revenue DESC
        )
        / SUM(revenue) OVER () * 100,
        2
    ) AS cumulative_revenue_pct
FROM category_revenue
ORDER BY revenue DESC;
*/

-- ANSWER
category   |revenue|revenue_contribution_pct|cumulative_revenue_pct|
-----------+-------+------------------------+----------------------+
Electronics|3519.87|                   77.19|                 77.19|
Clothing   | 719.94|                   15.79|                 92.98|
Sports     | 259.95|                    5.70|                 98.68|
Food       |  59.97|                    1.32|                100.00|


-- Q15

-- QUERY:
/*
WITH event_gaps AS (
    SELECT
        user_id,
        event_type,
        event_time,
        LAG(event_time) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS prev_event_time
    FROM user_events
),
session_markers AS (
    SELECT
        *,
        CASE
            WHEN prev_event_time IS NULL
              OR event_time - prev_event_time > INTERVAL '30 minutes'
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM event_gaps
),
sessions AS (
    SELECT
        *,
        SUM(is_new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS session_id
    FROM session_markers
),
session_stats AS (
    SELECT
        user_id,
        session_id,
        MIN(event_time) AS session_start,
        MAX(event_time) AS session_end,
        COUNT(*) AS event_count
    FROM sessions
    GROUP BY user_id, session_id
)
SELECT
    user_id,
    COUNT(*) AS total_sessions,
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM (session_end - session_start)) / 60
        )::numeric,
        1
    ) AS avg_session_duration_minutes,
    ROUND(
        AVG(event_count)::numeric,
        1
    ) AS avg_events_per_session
FROM session_stats
GROUP BY user_id
ORDER BY user_id;
*/

-- ANSWER:
user_id|total_sessions|avg_session_duration_minutes|avg_events_per_session|
-------+--------------+----------------------------+----------------------+
      1|             2|                         9.5|                   3.0|
      2|             2|                         6.0|                   2.5|


-- Q16

-- QUERY:
/*
WITH date_spine AS (
    SELECT d::date AS report_date
    FROM generate_series(
        '2026-02-01'::date,
        '2026-02-28'::date,
        '1 day'
    ) d
),
daily_revenue AS (
    SELECT
        order_date::date AS report_date,
        SUM(total_amount) AS daily_revenue
    FROM orders
    WHERE order_date >= '2026-02-01'
      AND order_date < '2026-03-01'
    GROUP BY order_date::date
),
daily_data AS (
    SELECT
        ds.report_date,
        COALESCE(dr.daily_revenue, 0) AS daily_revenue
    FROM date_spine ds
    LEFT JOIN daily_revenue dr
        ON ds.report_date = dr.report_date
)
SELECT
    report_date,
    daily_revenue,
    ROUND(
        AVG(daily_revenue) OVER (
            ORDER BY report_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_7_day_avg
FROM daily_data
ORDER BY report_date;
*/

-- ANSWER
report_date|daily_revenue|rolling_7_day_avg|
-----------+-------------+-----------------+
 2026-02-01|       199.99|           199.99|
 2026-02-02|            0|           100.00|
 2026-02-03|        89.99|            96.66|
 2026-02-04|            0|            72.50|
 2026-02-05|       129.98|            83.99|
 2026-02-06|            0|            69.99|
 2026-02-07|            0|            59.99|
 2026-02-08|        89.99|            44.28|
 2026-02-09|            0|            44.28|
 2026-02-10|        39.99|            37.14|
 2026-02-11|            0|            37.14|
 2026-02-12|       299.98|            61.42|
 2026-02-13|            0|            61.42|
 2026-02-14|            0|            61.42|
 2026-02-15|        49.99|            55.71|
 2026-02-16|            0|            55.71|
 2026-02-17|            0|            50.00|
 2026-02-18|      1299.99|           235.71|
 2026-02-19|            0|           192.85|
 2026-02-20|       179.98|           218.57|
 2026-02-21|            0|           218.57|
 2026-02-22|        59.99|           219.99|
 2026-02-23|            0|           219.99|
 2026-02-24|            0|           219.99|
 2026-02-25|        34.98|            39.28|
 2026-02-26|            0|            39.28|
 2026-02-27|            0|            13.57|
 2026-02-28|            0|            13.57|


 -- Q17


 -- QUERY:
/*
SELECT
    COALESCE(p.category, '** ALL **') AS category,
    COALESCE(c.city, '** ALL **') AS city,
    COUNT(*) AS order_count,
    SUM(oi.unit_price * oi.quantity) AS total_revenue
FROM order_items oi
JOIN orders o
    ON oi.order_id = o.order_id
JOIN customers c
    ON o.customer_id = c.customer_id
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY ROLLUP(p.category, c.city)
ORDER BY category, city;
*/

-- ANSWER
category   |city     |order_count|total_revenue|
-----------+---------+-----------+-------------+
** ALL **  |** ALL **|         27|      4559.73|
Clothing   |** ALL **|          6|       719.94|
Clothing   |Ankara   |          4|       479.96|
Clothing   |Istanbul |          2|       239.98|
Electronics|** ALL **|         13|      3519.87|
Electronics|Ankara   |          1|        29.99|
Electronics|Antalya  |          2|       249.98|
Electronics|Istanbul |          7|      1689.93|
Electronics|Izmir    |          3|      1549.97|
Food       |** ALL **|          3|        59.97|
Food       |Bursa    |          1|        24.99|
Food       |Izmir    |          2|        34.98|
Sports     |** ALL **|          5|       259.95|
Sports     |Ankara   |          1|        59.99|
Sports     |Antalya  |          2|        99.98|
Sports     |Istanbul |          2|        99.98|


-- Q18

-- QUERY:
/*
WITH customer_base AS (
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS full_name,
        c.city,
        c.signup_date
    FROM customers c
),

customer_orders AS (
    SELECT
        c.customer_id,
        COUNT(o.order_id) AS total_orders,
        COALESCE(SUM(o.total_amount), 0) AS total_spent,
        COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
        MAX(o.order_date) AS last_order_date
    FROM customers c
    LEFT JOIN orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
),

category_spending AS (
    SELECT
        o.customer_id,
        p.category,
        SUM(oi.quantity * oi.unit_price) AS category_spent
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY
        o.customer_id,
        p.category
),

category_ranked AS (
    SELECT
        customer_id,
        category,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY category_spent DESC
        ) AS rn
    FROM category_spending
),

customer_360 AS (
    SELECT
        cb.customer_id,
        cb.full_name,
        cb.city,

        DATE '2026-03-15' - cb.signup_date
            AS days_since_signup,

        co.total_orders,
        co.total_spent,
        co.avg_order_value,

        cr.category AS favorite_category,

        CASE
            WHEN co.last_order_date IS NULL THEN NULL
            ELSE DATE '2026-03-15' - co.last_order_date::date
        END AS days_since_last_order,

        CASE
            WHEN co.total_spent >= 1000 THEN 'VIP'
            WHEN co.total_spent >= 200 THEN 'Regular'
            ELSE 'Low-Value'
        END AS customer_tier

    FROM customer_base cb
    JOIN customer_orders co
        ON cb.customer_id = co.customer_id
    LEFT JOIN category_ranked cr
        ON cb.customer_id = cr.customer_id
        AND cr.rn = 1
)

SELECT
    full_name,
    city,
    days_since_signup,
    total_orders,
    total_spent,
    avg_order_value,
    favorite_category,
    days_since_last_order,
    customer_tier,
    DENSE_RANK() OVER (
        ORDER BY total_spent DESC
    ) AS spending_rank
FROM customer_360
ORDER BY total_spent DESC;
*/

-- ANSWER
full_name    |city    |days_since_signup|total_orders|total_spent|avg_order_value      |favorite_category|days_since_last_order|customer_tier|spending_rank|
-------------+--------+-----------------+------------+-----------+---------------------+-----------------+---------------------+-------------+-------------+
Alice Yilmaz |Istanbul|              424|           3|    1469.95| 489.9833333333333333|Electronics      |                   21|VIP          |            1|
Irem Aydin   |Izmir   |               54|           1|    1299.99|1299.9900000000000000|Electronics      |                   25|VIP          |            2|
Hakan Yildiz |Antalya |              165|           2|     349.97| 174.9850000000000000|Electronics      |                   28|Regular      |            3|
Diana Celik  |Istanbul|              287|           3|     289.97|  96.6566666666666667|Electronics      |                   33|Regular      |            4|
Charlie Demir|Izmir   |              309|           2|     284.96| 142.4800000000000000|Electronics      |                   18|Regular      |            5|
Eve Arslan   |Ankara  |              240|           3|     279.95|  93.3166666666666667|Clothing         |                   14|Regular      |            6|
Bob Kaya     |Ankara  |              358|           3|     269.97|  89.9900000000000000|Clothing         |                   40|Regular      |            7|
Jack Korkmaz |Istanbul|               29|           1|     179.98| 179.9800000000000000|Clothing         |                   23|Low-Value    |            8|
Grace Sahin  |Istanbul|              184|           1|      89.99|  89.9900000000000000|Clothing         |                   35|Low-Value    |            9|
Frank Ozturk |Bursa   |              222|           1|      24.99|  24.9900000000000000|Food             |                   49|Low-Value    |           10|
Kerem Bulut  |Ankara  |               14|           0|          0|                    0|                 |                     |Low-Value    |           11|
Lale Cinar   |Antalya |                5|           0|          0|                    0|                 |                     |Low-Value    |           11|