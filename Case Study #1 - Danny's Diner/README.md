- [🍣 Danny's Diner Data Analysis 🍜](#-dannys-diner-data-analysis-)
  - [Introduction](#introduction)
  - [Problem Statement](#problem-statement)
  - [Entity Relationship Diagram](#entity-relationship-diagram)
  - [Datasets](#datasets)
  - [Case Study Questions and Answers](#case-study-questions-and-answers)
  - [Bonus Questions and Answers](#bonus-questions-and-answers)
    - [Join All The Things](#join-all-the-things)
    - [Rank All The Things](#rank-all-the-things)


# 🍣 Danny's Diner Data Analysis 🍜

<img src="1.png" alt="Danny's Logo" width="350"/>

## Introduction

Danny loves Japanese food and, at the beginning of 2021, opened a restaurant selling sushi, curry, and ramen. He needs help using customer data to improve his business.

## Problem Statement

Danny wants insights on customer visiting patterns, spending, and favorite menu items to enhance the customer experience and decide on expanding the loyalty program.

## Entity Relationship Diagram

<img src="image.png" alt="Entity Relationship Diagram" width="700"/>

## Datasets

Danny has provided three key datasets for this case study:

1. **🛒 sales**: Captures all customer-level purchases with corresponding order dates and product information.
2. **📋 menu**: Maps the product ID to the actual product name and price of each menu item.
3. **🎟️ members**: Captures the join date when a customer joined the beta version of the Danny’s Diner loyalty program.

<details>
<summary><strong>Table 1: 🛒 sales</strong></summary>
Captures customer purchases with order dates and product IDs.

| customer_id | order_date | product_id |
|-------------|------------|------------|
| A           | 2021-01-01 | 1          |
| A           | 2021-01-01 | 2          |
| A           | 2021-01-07 | 2          |
| A           | 2021-01-10 | 3          |
| A           | 2021-01-11 | 3          |
| A           | 2021-01-11 | 3          |
| B           | 2021-01-01 | 2          |
| B           | 2021-01-02 | 2          |
| B           | 2021-01-04 | 1          |
| B           | 2021-01-11 | 1          |
| B           | 2021-01-16 | 3          |
| B           | 2021-02-01 | 3          |
| C           | 2021-01-01 | 3          |
| C           | 2021-01-01 | 3          |
| C           | 2021-01-07 | 3          |
</details>

<details>
<summary><strong>Table 2: 📋 menu</strong></summary>
Maps product IDs to product names and prices.

| product_id | product_name | price |
|------------|--------------|-------|
| 1          | sushi        | 10    |
| 2          | curry        | 15    |
| 3          | ramen        | 12    |
</details>

<details>
<summary><strong>Table 3: 🎟️ members</strong></summary>
Captures join dates for the loyalty program.

| customer_id | join_date  |
|-------------|------------|
| A           | 2021-01-07 |
| B           | 2021-01-09 |
</details>

## Case Study Questions and Answers

Each of the following case study questions can be answered using a single SQL statement:

<details>
<summary>1. 💵 What is the total amount each customer spent at the restaurant?</summary>
<!-- SQL solution goes here -->

    SELECT sales.customer_id, SUM(menu.price) as total_amount_spent
    FROM dannys_diner.sales sales
      JOIN dannys_diner.menu menu
        ON sales.product_id = menu.product_id
    GROUP BY 1
    ORDER BY 1;

| customer_id | total_amount_spent |
| ----------- | ------------------ |
| A           | 76                 |
| B           | 74                 |
| C           | 36                 |

---
</details>

<details>
<summary>2. 📅 How many days has each customer visited the restaurant?</summary>
<!-- SQL solution goes here -->

    SELECT sales.customer_id, COUNT(DISTINCT sales.order_date) as days_visited
    FROM dannys_diner.sales sales
      JOIN dannys_diner.menu menu
        ON sales.product_id = menu.product_id
    GROUP BY 1
    ORDER BY 1;

| customer_id | days_visited |
| ----------- | ------------ |
| A           | 4            |
| B           | 6            |
| C           | 2            |

---
</details>

<details>
<summary>3. 🥢 What was the first item from the menu purchased by each customer?</summary>
<!-- SQL solution goes here -->

**Query #1**

    SELECT 
        sub.customer, 
        STRING_AGG(sub.first_order, ', ' ORDER BY sub.first_order) as first_purchased_item
    FROM (
      SELECT sales.customer_id AS customer,
             menu.product_name AS first_order,
             DENSE_RANK() OVER(PARTITION BY sales.customer_id ORDER BY sales.order_date) as rank_of_dates
      FROM dannys_diner.sales sales
          JOIN dannys_diner.menu menu
              ON sales.product_id = menu.product_id
    ) as sub
    WHERE sub.rank_of_dates = 1
    GROUP BY 1;

| customer | first_purchased_item |
| -------- | -------------------- |
| A        | curry, sushi         |
| B        | curry                |
| C        | ramen, ramen         |

---
**Query #2**

    SELECT 
        DISTINCT sales.customer_id as customer,
    	menu.product_name as product
    FROM dannys_diner.sales sales
    	JOIN dannys_diner.menu menu
        	ON sales.product_id = menu.product_id
    WHERE order_date = '2021-01-01'	
    ORDER BY customer;

| customer | product |
| -------- | ------- |
| A        | curry   |
| A        | sushi   |
| B        | curry   |
| C        | ramen   |

---

</details>

<details>
<summary>4. 🍲 What is the most purchased item on the menu and how many times was it purchased by all customers?</summary>
<!-- SQL solution goes here -->

    WITH MostBoughtItem AS (
        SELECT menu.product_name
        FROM dannys_diner.sales sales
        JOIN dannys_diner.menu menu
            ON sales.product_id = menu.product_id
        GROUP BY menu.product_name
        ORDER BY COUNT(sales.product_id) DESC
        LIMIT 1
    ),
    CustomerPurchases AS (
        SELECT sales.customer_id AS customer,
               menu.product_name AS product,
               COUNT(sales.product_id) AS product_count
        FROM dannys_diner.sales sales
        JOIN dannys_diner.menu menu
            ON sales.product_id = menu.product_id
        GROUP BY sales.customer_id, menu.product_name
    ),
    MostBoughtItemPurchases AS (
        SELECT cp.customer,
               cp.product,
               COALESCE(cp.product_count, 0) AS product_count
        FROM MostBoughtItem mbi
        LEFT JOIN CustomerPurchases cp
            ON mbi.product_name = cp.product
    )
    SELECT customer, product, product_count
    FROM MostBoughtItemPurchases
    UNION ALL
    SELECT 'Total' AS customer, NULL AS product, SUM(product_count) AS product_count
    FROM MostBoughtItemPurchases
    ORDER BY customer;

| customer | product | product_count |
| -------- | ------- | ------------- |
| A        | ramen   | 3             |
| B        | ramen   | 2             |
| C        | ramen   | 3             |
| Total    |         | 8             |

---

</details>

<details>
<summary>5. 🍛 Which item was the most popular for each customer?</summary>
<!-- SQL solution goes here -->

    WITH products AS (
      SELECT
        sales.customer_id AS customer,
        menu.product_name AS product,
        count(product_name)
      FROM dannys_diner.sales AS sales
        JOIN dannys_diner.menu AS menu ON sales.product_id = menu.product_id
      GROUP BY 1,2
    )
    
    SELECT sub.customer, sub.product as popular_item
    FROM (
          SELECT
            customer,
            product,
            rank() over(PARTITION BY customer ORDER BY product) as ranking
          FROM products
          ) sub
    WHERE ranking = 1;

| customer | popular_item |
| -------- | ------------ |
| A        | curry        |
| B        | curry        |
| C        | ramen        |

---

</details>

<details>
<summary>6. 🥇 Which item was purchased first by the customer after they became a member?</summary>
<!-- SQL solution goes here -->

    WITH DatesOfOrders AS (
    SELECT 
        members.customer_id as customer, 
        join_date, 
        order_date, 
        product_id,
        ROW_NUMBER() OVER(PARTITION BY members.customer_id ORDER BY join_date, order_date) AS orders_rank
    FROM dannys_diner.members members
      JOIN dannys_diner.sales sales
        ON members.customer_id = sales.customer_id
    WHERE order_date >= join_date
    )
    
    SELECT customer, product_name
    FROM DatesofOrders doo
      JOIN dannys_diner.menu menu
        ON doo.product_id = menu.product_id
    WHERE orders_rank = 1
    ORDER BY customer;

| customer | product_name |
| -------- | ------------ |
| A        | curry        |
| B        | sushi        |

---
</details>

<details>
<summary>7. ⏪ Which item was purchased just before the customer became a member?</summary>
<!-- SQL solution goes here -->

    WITH DatesOfOrders AS (
    SELECT members.customer_id as customer,
      join_date,
      order_date,
      product_id,
      ROW_NUMBER() OVER(PARTITION BY members.customer_id ORDER BY join_date, order_date DESC) AS orders_rank
    FROM dannys_diner.members members
      JOIN dannys_diner.sales sales
        ON members.customer_id = sales.customer_id
    WHERE order_date < join_date
    )
    
    SELECT customer, product_name
    FROM DatesofOrders doo
      JOIN dannys_diner.menu menu
        ON doo.product_id = menu.product_id
    WHERE orders_rank = 1
    ORDER BY customer;

| customer | product_name |
| -------- | ------------ |
| A        | sushi        |
| B        | sushi        |

---
</details>

<details>
<summary>8. 🧾 What is the total items and amount spent for each member before they became a member?</summary>
<!-- SQL solution goes here -->

    WITH DatesOfOrders AS (
      SELECT sales.customer_id as customer,
        product_id,
        COUNT(sales.product_id) OVER(PARTITION BY sales.customer_id) as total_products
      FROM dannys_diner.members members
        LEFT JOIN dannys_diner.sales sales
          ON members.customer_id = sales.customer_id
      WHERE order_date < join_date
    )
    
    SELECT customer, total_products, SUM(price) as total_price
    FROM DatesofOrders doo
      JOIN dannys_diner.menu menu
        ON doo.product_id = menu.product_id
    GROUP BY customer, total_products
    ORDER BY customer;

| customer | total_products | total_price |
| -------- | -------------- | ----------- |
| A        | 2              | 25          |
| B        | 3              | 40          |

---
</details>

<details>
<summary>9. 💯 If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?</summary>
<!-- SQL solution goes here -->

    WITH PointsTable AS (
      SELECT 
        sales.customer_id as customer_id, product_name, price,
        CASE
          WHEN menu.product_name <> 'sushi' THEN price*10
          ELSE price*10*2
        END as points
      FROM dannys_diner.sales sales
        INNER JOIN dannys_diner.menu menu
          ON sales.product_id = menu.product_id
    )
    
    SELECT customer_id, SUM(points)
    FROM PointsTable
    GROUP BY 1
    ORDER BY 1;

| customer_id | sum |
| ----------- | --- |
| A           | 860 |
| B           | 940 |
| C           | 360 |

---
</details>

<details>
<summary>10. 🚀 In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?</summary>
<!-- SQL solution goes here -->

    WITH PointsTable AS (
       SELECT 
        sales.customer_id as customer_id,
        product_name,
        price,
        join_date,
        order_date,
        CASE
          WHEN order_date BETWEEN join_date AND join_date + INTERVAL '6 DAY' THEN price*10*2
          WHEN product_name = 'sushi' THEN price*10*2
          ELSE price*10
        END as points
      FROM dannys_diner.sales sales
        INNER JOIN dannys_diner.menu menu
          ON sales.product_id = menu.product_id
        INNER JOIN dannys_diner.members mem
          ON sales.customer_id = mem.customer_id
      WHERE order_date < '2021-02-01'
    )
    
    SELECT customer_id, SUM(points)
    FROM PointsTable
    GROUP BY customer_id
    ORDER BY customer_id;

| customer_id | sum  |
| ----------- | ---- |
| A           | 1370 |
| B           | 820  |

---
</details>

## Bonus Questions and Answers

### Join All The Things

<details>
<summary>Recreate the following table output using the available data:

| customer_id | order_date | product_name | price | member |
|-------------|------------|--------------|-------|--------|
| A           | 2021-01-01 | curry        | 15    | N      |
| A           | 2021-01-01 | sushi        | 10    | N      |
| A           | 2021-01-07 | curry        | 15    | Y      |
| A           | 2021-01-10 | ramen        | 12    | Y      |
| A           | 2021-01-11 | ramen        | 12    | Y      |
| A           | 2021-01-11 | ramen        | 12    | Y      |
| B           | 2021-01-01 | curry        | 15    | N      |
| B           | 2021-01-02 | curry        | 15    | N      |
| B           | 2021-01-04 | sushi        | 10    | N      |
| B           | 2021-01-11 | sushi        | 10    | Y      |
| B           | 2021-01-16 | ramen        | 12    | Y      |
| B           | 2021-02-01 | ramen        | 12    | Y      |
| C           | 2021-01-01 | ramen        | 12    | N      |
| C           | 2021-01-01 | ramen        | 12    | N      |
| C           | 2021-01-07 | ramen        | 12    | N      |
</summary>

<!-- SQL solution goes here -->
    
    SELECT
        sales.customer_id,
        sales.order_date,
        menu.product_name,
        menu.price,
        CASE 
          WHEN sales.order_date >= mem.join_date THEN 'Y'
          ELSE 'N'
        END AS member
      FROM dannys_diner.sales sales
      LEFT JOIN dannys_diner.menu menu
        ON sales.product_id = menu.product_id
      LEFT JOIN dannys_diner.members mem
        ON sales.customer_id = mem.customer_id
    ORDER BY sales.customer_id, order_date, product_name, price;
</details>

### Rank All The Things

<details>
<summary>Danny also requires further information about the ranking of customer products, but he purposely does not need the ranking for non-member purchases so he expects null ranking values for the records when customers are not yet part of the loyalty program.

| customer_id | order_date | product_name | price | member | ranking |
|-------------|------------|--------------|-------|--------|---------|
| A           | 2021-01-01 | curry        | 15    | N      | null    |
| A           | 2021-01-01 | sushi        | 10    | N      | null    |
| A           | 2021-01-07 | curry        | 15    | Y      | 1       |
| A           | 2021-01-10 | ramen        | 12    | Y      | 2       |
| A           | 2021-01-11 | ramen        | 12    | Y      | 3       |
| A           | 2021-01-11 | ramen        | 12    | Y      | 3       |
| B           | 2021-01-01 | curry        | 15    | N      | null    |
| B           | 2021-01-02 | curry        | 15    | N      | null    |
| B           | 2021-01-04 | sushi        | 10    | N      | null    |
| B           | 2021-01-11 | sushi        | 10    | Y      | 1       |
| B           | 2021-01-16 | ramen        | 12    | Y      | 2       |
| B           | 2021-02-01 | ramen        | 12    | Y      | 3       |
| C           | 2021-01-01 | ramen        | 12    | N      | null    |
| C           | 2021-01-01 | ramen        | 12    | N      | null    |
| C           | 2021-01-07 | ramen        | 12    | N      | null    |

</summary>
<!-- SQL solution goes here -->
    
    WITH JoinAllTheThings AS (
      SELECT
        sales.customer_id,
        sales.order_date,
        menu.product_name,
        menu.price,
        CASE 
          WHEN sales.order_date >= mem.join_date THEN 'Y'
          ELSE 'N'
        END AS member
      FROM dannys_diner.sales sales
      LEFT JOIN dannys_diner.menu menu
        ON sales.product_id = menu.product_id
      LEFT JOIN dannys_diner.members mem
        ON sales.customer_id = mem.customer_id
    ),
    RankedMembers AS (
      SELECT 
        customer_id,
        order_date,
        product_name,
        price,
        member,
        RANK() OVER(PARTITION BY customer_id ORDER BY order_date, product_name, price) as ranking
      FROM JoinAllTheThings
      WHERE member = 'Y'
    )

    SELECT 
      j.customer_id,
      j.order_date,
      j.product_name,
      j.price,
      j.member,
      r.ranking
    FROM JoinAllTheThings j
    LEFT JOIN RankedMembers r
    ON j.customer_id = r.customer_id
    AND j.order_date = r.order_date
    AND j.product_name = r.product_name
    AND j.price = r.price
    ORDER BY j.customer_id, j.order_date, j.product_name, j.price;

</details>