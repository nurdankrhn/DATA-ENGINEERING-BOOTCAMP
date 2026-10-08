# Advanced SQL Homework

This repository contains my solutions for the **Advanced SQL Homework** completed as part of the **Data Engineering Bootcamp**.

The homework focuses on advanced SQL concepts, analytical queries, performance optimization, and real-world data engineering patterns using **PostgreSQL**.

## Technologies

- PostgreSQL 16
- DBeaver
- SQL

## Database Schema

The exercises use the following main tables:

### `customers`

Stores customer information.

- `customer_id`
- `first_name`
- `last_name`
- `email`
- `phone`
- `city`
- `signup_date`
- `is_active`

### `orders`

Stores customer orders.

- `order_id`
- `customer_id`
- `order_date`
- `status`
- `total_amount`

### `employees`

Stores employee and manager information.

- `emp_id`
- `name`
- `department`
- `salary`
- `manager_id`

### `products`

Stores product information.

- `product_id`
- `product_name`
- `category`
- `price`
- `stock`

### `order_items`

Stores products included in orders.

- `order_id`
- `product_id`
- `quantity`
- `unit_price`

### `user_events`

Stores user activity events used for sessionization analysis.

- `event_id`
- `user_id`
- `event_type`
- `event_time`

---

# Exercises

## Q1 — Employees with No Direct Reports

Identifies employees who do not have any employees reporting directly to them.

**Concepts:**
- `LEFT JOIN`
- Self JOIN
- `IS NULL`

---

## Q2 — Employee Salary vs Manager Salary

Compares each employee's salary with their manager's salary.

**Concepts:**
- Self JOIN
- `CASE`
- Boolean expressions

---

## Q3 — Customers Ordering in Both January and February

Finds customers who placed orders in both January and February.

**Concepts:**
- `INTERSECT`
- Date filtering
- Timestamp ranges

---

## Q4 — Top 2 Orders per Customer

Finds the two highest-value orders for each customer and includes the total number of orders for that customer.

**Concepts:**
- `DENSE_RANK()`
- `PARTITION BY`
- Window functions

---

## Q5 — Month-over-Month Revenue Analysis

Calculates monthly revenue and compares each month with the previous month.

**Concepts:**
- `DATE_TRUNC()`
- `LAG()`
- Percentage change
- CTE

---

## Q6 — Running Total per Customer

Calculates the cumulative spending of each customer over time.

**Concepts:**
- `SUM() OVER()`
- `PARTITION BY`
- `ORDER BY`
- Running totals

---

## Q7 — Salary Percent Rank

Calculates the relative salary position of each employee within their department.

**Concepts:**
- `PERCENT_RANK()`
- `PARTITION BY`
- Window functions

---

## Q8 — Customer Tier Distribution

Classifies customers according to their total spending.

Customer tiers:

- **VIP**
- **Regular**
- **Low-Value**

The result also includes the number of customers and average spending for each tier.

**Concepts:**
- CTE
- `CASE`
- Aggregation
- Customer segmentation

---

## Q9 — Recursive Factorial

Generates factorial values from 1 to 10 using a recursive CTE.

**Concepts:**
- `WITH RECURSIVE`
- Recursive CTE
- `UNION ALL`

---

## Q10 — Orders Above Customer Average

Finds orders whose value is higher than the customer's own average order value.

**Concepts:**
- Correlated subquery
- `AVG()`
- JOIN

---

## Q11 — Removing Duplicate Events

Creates duplicated event data and removes duplicate records while keeping unique event combinations.

**Concepts:**
- Temporary tables
- `UNION ALL`
- `DISTINCT ON`

---

## Q12 — NULL Data Quality Report

Analyzes missing values for customer fields such as phone number and city.

The report includes:

- Number of NULL values
- Total records
- NULL percentage

**Concepts:**
- `UNION ALL`
- Conditional aggregation
- Data quality analysis

---

## Q13 — Monthly Order Status Pivot

Creates a monthly summary showing the number of orders by status.

Statuses include:

- Completed
- Shipped
- Pending
- Cancelled
- Total

**Concepts:**
- `FILTER`
- Conditional aggregation
- Pivot-style reporting
- `DATE_TRUNC()`

---

## Q14 — Category Revenue Contribution

Calculates each product category's contribution to total revenue and its cumulative revenue percentage.

**Concepts:**
- CTE
- Window functions
- `SUM() OVER()`
- Running percentages

---

## Q15 — User Sessionization

Groups user events into sessions using a **30-minute inactivity threshold**.

For each user, the analysis calculates:

- Total sessions
- Average session duration
- Average events per session

**Concepts:**
- `LAG()`
- `PARTITION BY`
- Cumulative `SUM()`
- Sessionization
- Window functions

---

## Q16 — Date Spine and Rolling Average

Creates a complete date range for February 2026, including days with no orders.

For each day, the analysis calculates:

- Daily revenue
- 7-day rolling average revenue

**Concepts:**
- `generate_series()`
- Date spine
- `LEFT JOIN`
- `COALESCE()`
- Rolling averages
- Window functions

---

## Q17 — Revenue Analysis with ROLLUP

Analyzes revenue by:

- Product category
- Customer city
- Category subtotal
- Grand total

**Concepts:**
- `ROLLUP`
- Aggregation
- Multi-level reporting

---

## Q18 — Complete Customer 360 View

Creates a comprehensive customer-level analysis.

The final result includes:

- Full name
- City
- Days since signup
- Total orders
- Total spending
- Average order value
- Favorite product category
- Days since last order
- Customer tier
- Spending rank

Customers are ordered by total spending.

**Concepts:**
- Multiple CTEs
- JOINs
- Aggregation
- `ROW_NUMBER()`
- `DENSE_RANK()`
- Date calculations
- Customer segmentation
- Customer 360 analysis

---

# Performance & Real-World SQL Patterns

The homework also covers practical database performance techniques.

## EXPLAIN ANALYZE

`EXPLAIN ANALYZE` is used to inspect query execution plans and understand how PostgreSQL executes queries.

Example:

```sql
EXPLAIN ANALYZE
SELECT ...
```

## Indexes

Indexes are created to support frequently used filtering and JOIN operations.

Example:

```sql
CREATE INDEX idx_orders_customer_id
ON orders(customer_id);
```

Another example:

```sql
CREATE INDEX idx_orders_status_date
ON orders(status, order_date);
```

## Materialized Views

A materialized view stores the result of a query and can be refreshed when the underlying data changes.

Example:

```sql
CREATE MATERIALIZED VIEW mv_customer_summary AS
SELECT ...
```

The materialized view can be refreshed using:

```sql
REFRESH MATERIALIZED VIEW mv_customer_summary;
```

## ROLLUP

`ROLLUP` is used to generate hierarchical subtotals and grand totals in analytical reports.

## FILTER

The `FILTER` clause is used for conditional aggregation without requiring separate queries.

Example:

```sql
COUNT(*) FILTER (WHERE status = 'completed')
```

## Date Spine

A date spine is created using `generate_series()` to ensure that dates with no transactions are also included in reports.

Example:

```sql
SELECT d::date
FROM generate_series(
    '2026-02-01'::date,
    '2026-02-28'::date,
    '1 day'
) d;
```

---

# Learning Objectives

Through these exercises, I practiced how to:

- Write advanced SQL queries
- Work with multiple related tables
- Use different types of JOINs
- Use subqueries and correlated subqueries
- Build CTEs and recursive CTEs
- Apply window functions
- Use ranking functions
- Calculate running totals and rolling averages
- Perform customer segmentation
- Analyze revenue and order data
- Implement sessionization logic
- Build date spines
- Generate hierarchical reports with `ROLLUP`
- Perform data quality analysis
- Analyze query execution plans
- Create and use indexes
- Work with materialized views
- Build real-world analytical SQL patterns

---

# Project Structure

```text
Advanced-SQL-Homework/
│
├── README.md
│
├── advanced_sql_homework.sql
│
└── screenshots/
    ├── q01.png
    ├── q02.png
    ├── ...
    └── q18.png
```

---

# How to Run

1. Install PostgreSQL 16.
2. Create the required database and tables.
3. Insert the provided sample data.
4. Open the SQL file using DBeaver or another PostgreSQL client.
5. Execute the queries individually to review the results.

---

# Course

This homework was completed as part of the **Data Engineering Bootcamp** organized by **Veri Bilimi Okulu**.

The exercises focus on practical SQL skills used in data engineering, analytical workloads, and real-world database applications.

---

## Author

**Nurdan Karahan**

Data Engineering | Backend Development | AI & LLM
