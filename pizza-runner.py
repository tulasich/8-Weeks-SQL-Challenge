# Databricks notebook source
# %fs ls abfss://source@internsstorage1.dfs.core.windows.net/tulasi/

# COMMAND ----------

# %sql

# DROP TABLE IF EXISTS runners;
# CREATE TABLE runners (
#   runner_id INTEGER,
#   registration_date DATE
# );
# INSERT INTO runners (runner_id, registration_date)
# VALUES
#   (1, '2021-01-01'),
#   (2, '2021-01-03'),
#   (3, '2021-01-08'),
#   (4, '2021-01-15');

# DROP TABLE IF EXISTS customer_orders;
# CREATE TABLE customer_orders (
#   order_id INTEGER,
#   customer_id INTEGER,
#   pizza_id INTEGER,
#   exclusions VARCHAR(4),
#   extras VARCHAR(4),
#   order_time TIMESTAMP
# );
# INSERT INTO customer_orders (order_id, customer_id, pizza_id, exclusions, extras, order_time)
# VALUES
#   (1, 101, 1, '', '', '2020-01-01 18:05:02'),
#   (2, 101, 1, '', '', '2020-01-01 19:00:52'),
#   (3, 102, 1, '', '', '2020-01-02 23:51:23'),
#   (3, 102, 2, '', NULL, '2020-01-02 23:51:23'),
#   (4, 103, 1, '4', '', '2020-01-04 13:23:46'),
#   (4, 103, 1, '4', '', '2020-01-04 13:23:46'),
#   (4, 103, 2, '4', '', '2020-01-04 13:23:46'),
#   (5, 104, 1, 'null', '1', '2020-01-08 21:00:29'),
#   (6, 101, 2, 'null', 'null', '2020-01-08 21:03:13'),
#   (7, 105, 2, 'null', '1', '2020-01-08 21:20:29'),
#   (8, 102, 1, 'null', 'null', '2020-01-09 23:54:33'),
#   (9, 103, 1, '4', '1, 5', '2020-01-10 11:22:59'),
#   (10, 104, 1, 'null', 'null', '2020-01-11 18:34:49'),
#   (10, 104, 1, '2, 6', '1, 4', '2020-01-11 18:34:49');

# DROP TABLE IF EXISTS runner_orders;
# CREATE TABLE runner_orders (
#   order_id INTEGER,
#   runner_id INTEGER,
#   pickup_time VARCHAR(19),
#   distance VARCHAR(7),
#   duration VARCHAR(10),
#   cancellation VARCHAR(23)
# );
# INSERT INTO runner_orders (order_id, runner_id, pickup_time, distance, duration, cancellation)
# VALUES
#   (1, 1, '2020-01-01 18:15:34', '20km', '32 minutes', ''),
#   (2, 1, '2020-01-01 19:10:54', '20km', '27 minutes', ''),
#   (3, 1, '2020-01-03 00:12:37', '13.4km', '20 mins', NULL),
#   (4, 2, '2020-01-04 13:53:03', '23.4', '40', NULL),
#   (5, 3, '2020-01-08 21:10:57', '10', '15', NULL),
#   (6, 3, 'null', 'null', 'null', 'Restaurant Cancellation'),
#   (7, 2, '2020-01-08 21:30:45', '25km', '25mins', 'null'),
#   (8, 2, '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'),
#   (9, 2, 'null', 'null', 'null', 'Customer Cancellation'),
#   (10, 1, '2020-01-11 18:50:20', '10km', '10minutes', 'null');

# DROP TABLE IF EXISTS pizza_names;
# CREATE TABLE pizza_names (
#   pizza_id INTEGER,
#   pizza_name VARCHAR(50)
# );
# INSERT INTO pizza_names (pizza_id, pizza_name)
# VALUES
#   (1, 'Meatlovers'),
#   (2, 'Vegetarian');

# DROP TABLE IF EXISTS pizza_recipes;
# CREATE TABLE pizza_recipes (
#   pizza_id INTEGER,
#   toppings VARCHAR(50)
# );
# INSERT INTO pizza_recipes (pizza_id, toppings)
# VALUES
#   (1, '1, 2, 3, 4, 5, 6, 8, 10'),
#   (2, '4, 6, 7, 9, 11, 12');

# DROP TABLE IF EXISTS pizza_toppings;
# CREATE TABLE pizza_toppings (
#   topping_id INTEGER,
#   topping_name VARCHAR(50)
# );
# INSERT INTO pizza_toppings (topping_id, topping_name)
# VALUES
#   (1, 'Bacon'),
#   (2, 'BBQ Sauce'),
#   (3, 'Beef'),
#   (4, 'Cheese'),
#   (5, 'Chicken'),
#   (6, 'Mushrooms'),
#   (7, 'Onions'),
#   (8, 'Pepperoni'),
#   (9, 'Peppers'),
#   (10, 'Salami'),
#   (11, 'Tomatoes'),
#   (12, 'Tomato Sauce');

# COMMAND ----------

# customer_orders_df.write.format("delta").mode("overwrite").save("/Volumes/interns_adls/tulasi/pizza-runner/customer_orders")
# runners_df.write.format("delta").mode("overwrite").save("/Volumes/interns_adls/tulasi/pizza-runner/runners")
# runner_orders_df.write.format("delta").mode("overwrite").save("/Volumes/interns_adls/tulasi/pizza-runner/runner_orders")
# pizza_names_df.write.format("delta").mode("overwrite").save("/Volumes/interns_adls/tulasi/pizza-runner/pizza_names")
# pizza_toppings_df.write.format("delta").mode("overwrite").save("/Volumes/interns_adls/tulasi/pizza-runner/pizza_toppings")
# pizza_recipes_df.write.format("delta").mode("overwrite").save("/Volumes/interns_adls/tulasi/pizza-runner/pizza_recipes")

# COMMAND ----------

customer_orders_df = spark.read.load("/Volumes/interns_adls/tulasi/pizza-runner/customer_orders")
runners_df = spark.read.load("/Volumes/interns_adls/tulasi/pizza-runner/runners")
runner_orders_df = spark.read.load("/Volumes/interns_adls/tulasi/pizza-runner/runner_orders")
pizza_names_df = spark.read.load("/Volumes/interns_adls/tulasi/pizza-runner/pizza_names")
pizza_toppings_df = spark.read.load("/Volumes/interns_adls/tulasi/pizza-runner/pizza_toppings")
pizza_recipes_df = spark.read.load("/Volumes/interns_adls/tulasi/pizza-runner/pizza_recipes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data cleaning
# MAGIC

# COMMAND ----------

customer_orders_df.printSchema()
pizza_names_df.printSchema()
pizza_recipes_df.printSchema()
pizza_toppings_df.printSchema()
runner_orders_df.printSchema()
runners_df.printSchema()

# COMMAND ----------

customer_orders_df.show()
runner_orders_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, regexp_replace, concat, lit
from pyspark.sql import functions as F

customer_orders_cleaned_df = customer_orders_df \
    .withColumn(
        "extras",
        when(col("extras") == "null", "").
        when(col("extras").isNull(), "").
        otherwise(trim(col("extras")))
    ) \
    .withColumn(
        "exclusions", 
        when(col("exclusions") == "null", "").
        when(col("exclusions").isNull(), "").
        otherwise(trim(col("exclusions")))
    )

runner_orders_cleaned_df = runner_orders_df \
    .withColumn("distance", 
               regexp_replace("distance", r"[^0-9.]", "")
               ) \
    .withColumn("distance", 
               concat("distance", lit(" km"))
               ) \
    .withColumn("distance", 
               when(trim(runner_orders_df["distance"]) == "km", lit(""))
               .otherwise(runner_orders_df["distance"])
               ) \
    .withColumn("distance",
               trim(runner_orders_df["distance"])
               ) \
    .withColumn("duration", 
               regexp_replace("duration", r"[^0-9.]", "")
               ) \
    .withColumn("duration", 
               concat("duration", lit(" min"))
               ) \
    .withColumn("duration", 
               when(trim(runner_orders_df["duration"]) == "min", lit(""))
               .otherwise(runner_orders_df["duration"])
               ) \
    .withColumn("duration", 
               trim(runner_orders_df["duration"])
               ) \
    .withColumn("cancellation", 
               when(col("cancellation") == "null", "").when(col("cancellation").isNull(), "")
               .otherwise(trim(col("cancellation")))
               ) \
    .withColumn("pickup_time",
               when(col("pickup_time") == "null", None)
               .when(col("pickup_time").isNull(), None)
               .otherwise(trim(col("pickup_time")))
               ) \
    .withColumn("pickup_time",
               col("pickup_time").cast("timestamp"))

customer_orders_cleaned_df.display()
runner_orders_cleaned_df.display()

# COMMAND ----------

print(runner_orders_cleaned_df.schema["pickup_time"].dataType)

# COMMAND ----------

# customer_orders_cleaned_df.write.format("delta").save("/Volumes/interns_adls/tulasi/pizza-runner/customer_orders_cleaned")
# runner_orders_cleaned_df.write.format("delta").save("/Volumes/interns_adls/tulasi/pizza-runner/runner_orders_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. Pizza Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC 1. How many pizzas were ordered?

# COMMAND ----------

customer_orders_cleaned_df.select("order_id").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. How many unique customer orders were made?
# MAGIC

# COMMAND ----------

customer_orders_cleaned_df.select("order_id").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. How many successful orders were delivered by each runner?
# MAGIC

# COMMAND ----------

runner_orders_cleaned_df \
    .where("cancellation == ''") \
    .groupBy("runner_id") \
    .count().withColumnRenamed("count", "successful_orders") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. How many of each type of pizza was delivered?

# COMMAND ----------

cust_runners_join = customer_orders_cleaned_df.join(runner_orders_cleaned_df, "order_id", "inner")

cust_runners_join \
    .where("cancellation == ''") \
    .groupBy("pizza_id") \
    .count() \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. How many Vegetarian and Meatlovers were ordered by each customer?

# COMMAND ----------

cust_pizza_names_join = customer_orders_cleaned_df.join(
    pizza_names_df,
    "pizza_id",
    "inner")
cust_pizza_names_join.groupBy("customer_id","pizza_name").count().orderBy("customer_id", "pizza_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. What was the maximum number of pizzas delivered in a single order?

# COMMAND ----------

from pyspark.sql.functions import max

customer_orders_cleaned_df.groupBy("order_id") \
    .count() \
    .agg(max("count").alias("maxpzzasordered")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. For each customer, how many delivered pizzas had at least 1 change and how many had no changes?

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from delta.`/Volumes/interns_adls/tulasi/pizza-runner/customer_orders_cleaned`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   cc.customer_id,
# MAGIC   sum(
# MAGIC     case when cc.exclusions <> '' or cc.extras <> '' then 1
# MAGIC     else 0
# MAGIC     end) as changed,
# MAGIC   sum(
# MAGIC     case when cc.exclusions = '' and cc.extras = '' then 1
# MAGIC     else 0
# MAGIC     end) as unchanged
# MAGIC from delta.`/Volumes/interns_adls/tulasi/pizza-runner/customer_orders_cleaned` cc
# MAGIC     join delta.`/Volumes/interns_adls/tulasi/pizza-runner/runner_orders_cleaned` ro
# MAGIC       on cc.order_id = ro.order_id
# MAGIC where ro.cancellation = ''
# MAGIC group by cc.customer_id
# MAGIC order by cc.customer_id

# COMMAND ----------

from pyspark.sql.functions import col, when, sum

changed_df = cust_runners_join \
  .orderBy("customer_id") \
  .filter("cancellation == ''") \
  .withColumn("changed", when((col("exclusions") != '') | (col("extras") != ''), 1)
               .otherwise(0)
    ) \
  .groupBy("customer_id") \
  .agg(sum("changed").alias("changed")) 

unchanged_df = cust_runners_join \
  .orderBy("customer_id") \
  .filter("cancellation == ''") \
  .withColumn("unchanged", when((col("exclusions") == '') & (col("extras") == ''), 1)
              .otherwise(0)
  )\
  .groupBy("customer_id") \
  .agg(sum("unchanged").alias("unchanged")) \
  .orderBy("customer_id")

changed_df \
  .join(unchanged_df, "customer_id", "inner") \
  .orderBy("customer_id") \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 8. How many pizzas were delivered that had both exclusions and extras?

# COMMAND ----------

cust_runners_join \
  .filter("cancellation == ''") \
  .withColumn("excl&extras",
               when(
                (col("exclusions") != '') & (col("extras") != ''), 1)
                .otherwise(0)) \
  .groupBy() \
  .agg(sum("excl&extras").alias("exclextras")) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 9. What was the total volume of pizzas ordered for each hour of the day?

# COMMAND ----------

# MAGIC %sql
# MAGIC select hour(c.order_time), count(c.pizza_id)
# MAGIC from delta.`/Volumes/interns_adls/tulasi/pizza-runner/customer_orders_cleaned` c
# MAGIC   join delta.`/Volumes/interns_adls/tulasi/pizza-runner/runner_orders_cleaned`r
# MAGIC     on c.order_id = r.order_id
# MAGIC where r.cancellation = '' 
# MAGIC group by hour(c.order_time)

# COMMAND ----------

from pyspark.sql.functions import hour, count

cust_runners_join \
    .filter("cancellation == ''") \
    .select(hour(col("order_time")).alias("hour")) \
    .groupBy("hour") \
    .count() \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 10. What was the volume of orders for each day of the week?

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   DATE_FORMAT(DATE_ADD(c.order_time, 2),'EEEE') AS day_of_week, -- add 2 to adjust 1st day of the week as Monday
# MAGIC   COUNT(c.order_id) AS total_pizzas_ordered
# MAGIC from delta.`/Volumes/interns_adls/tulasi/pizza-runner/customer_orders_cleaned` c
# MAGIC   join delta.`/Volumes/interns_adls/tulasi/pizza-runner/runner_orders_cleaned`r
# MAGIC     on c.order_id = r.order_id
# MAGIC group by 1

# COMMAND ----------

from pyspark.sql.functions import dayofweek, date_format, date_add

customer_orders_cleaned_df \
    .withColumn("dateadd", date_add("order_time", 2)) \
    .withColumn("day", date_format("dateadd", "E")) \
    .groupBy("day") \
    .count() \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B. Runner and Customer Experience

# COMMAND ----------

# MAGIC %md 1. How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)

# COMMAND ----------

runners_df \
    .groupBy(F.weekofyear("registration_date").alias("week")) \
    .count() \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?

# COMMAND ----------

# unix_timestamp converts timestamp to seconds

cust_runners_join \
    .filter("pickup_time IS NOT NULL") \
    .withColumn("avg_min_between",
                (F.unix_timestamp("pickup_time") - F.unix_timestamp("order_time"))/60) \
    .agg(F.round(F.avg("avg_min_between"), 0).alias("avg_min_between")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Is there any relationship between the number of pizzas and how long the order takes to prepare?

# COMMAND ----------

customer_orders_cleaned_df.display()

# COMMAND ----------

countp = cust_runners_join \
    .filter("pickup_time IS NOT NULL") \
    .groupBy("order_id") \
    .agg(
        F.count("pizza_id").alias("count_of_pizzas"),
        F.round(
            (F.unix_timestamp(F.max("pickup_time")) - F.unix_timestamp(F.max("order_time"))) / 60
        ).alias("min_between")
    )

countp \
    .groupBy("count_of_pizzas") \
    .agg(F.avg("min_between").alias("avg_min_between")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. What was the average distance travelled for each customer?

# COMMAND ----------

cust_runners_join \
    .withColumn("distance", 
                F.regexp_replace("distance", " km", "").cast("double")) \
    .filter("pickup_time IS NOT NULL") \
    .groupBy("customer_id") \
    .agg(F.avg("distance").alias("avg_distance")) \
    .show()


# COMMAND ----------

# MAGIC %md
# MAGIC 5. What was the difference between the longest and shortest delivery times for all orders?

# COMMAND ----------

temp_df = cust_runners_join \
    .filter("duration not like ''") \
    .select("order_id", "duration") \
    .withColumn("duration", F.regexp_replace("duration", " min", "").cast("float"))

temp_df \
  .select(F.max("duration") - F.min("duration")) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. What was the average speed for each runner for each delivery and do you notice any trend for these values?

# COMMAND ----------

runner_orders_cleaned_df \
  .withColumn("duration", F.regexp_replace("duration", " min", "").cast("float")) \
  .groupBy("runner_id") \
  .agg(F.avg("duration").alias("avg_duration")) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. What is the successful delivery percentage for each runner?

# COMMAND ----------

cust_runners_join.display()

# COMMAND ----------

runner_orders_cleaned_df.select("runner_id", "pickup_time").show()

# COMMAND ----------

runner_orders_cleaned_df \
    .groupBy("runner_id") \
    .agg(
        F.count("runner_id").alias("count"),
        F.try_divide((100*(F.round(
            F.sum(
                F.when(F.col("pickup_time").isNull(), 0).otherwise(1)
            )
        ))), F.col("count")).alias("delivery_percentage")
    ) \
    .orderBy("runner_id") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C. Ingredient Optimisation

# COMMAND ----------

customer_orders_cleaned_df.display()
coi_df = customer_orders_cleaned_df \
    .withColumn("extras",
                F.split("extras", ",")) \
    .withColumn("extras",
                F.explode("extras")) \
    .withColumn("exclusions",
            F.split("exclusions", ",")) \
    .withColumn("exclusions",
                F.explode("exclusions")) 

coi_df.show() 

# COMMAND ----------

pizza_recipes_cleaned = pizza_recipes_df \
    .withColumn("toppings", F.split(F.col("toppings"), ",")) \
    .withColumn("toppings", F.explode(F.col("toppings")))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. What are the standard ingredients for each pizza?

# COMMAND ----------

pizza_toppings_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. What was the most commonly added extra?

# COMMAND ----------

cois_df.join(
    pizza_toppings_df,
    cois_df["extras"] == pizza_toppings_df["topping_id"],
    "inner"
).show()

# COMMAND ----------

cois_df = coi_df.filter("extras != ''")

result_df = cois_df.join(
    pizza_toppings_df,
    cois_df["extras"] == pizza_toppings_df["topping_id"],
    "inner"
).groupBy(
    "extras", "topping_name"
).count(
).orderBy(
    "count", ascending=False
)

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. What was the most common exclusion?
# MAGIC

# COMMAND ----------

customer_orders_cleaned_df \
    .filter("exclusions != ''") \
    .groupBy("exclusions") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate an order item for each record in the customers_orders table in the format of one of the following:
# MAGIC - Meat Lovers
# MAGIC - Meat Lovers - Exclude Beef
# MAGIC - Meat Lovers - Extra Bacon
# MAGIC - Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers
# MAGIC

# COMMAND ----------

extra_top = coi_df.join(pizza_toppings_df, coi_df["extras"] == pizza_toppings_df["topping_id"], "inner").select("extras", "topping_name")

exclu_top = coi_df.join(pizza_toppings_df, coi_df["exclusions"] == pizza_toppings_df["topping_id"], "inner").select("exclusions", "topping_name")

extra_top.display()

# COMMAND ----------

df = coi_df \
    .join(pizza_names_df, "pizza_id", "inner") \
    .select("pizza_name", "extras", "exclusions")

df = df.collect()

for row in df:
    item_name = row[0]
    if row[1] == "":
        print(item_name, end="")
    if row[1] != "":
        print(item_name + " - Exclude " + row[1], end = "")
    if row[2] != "":
        print(" - Extra", row[2], end = "")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients
# MAGIC - For example: "Meat Lovers: 2xBacon, Beef, ... , Salami"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 6. What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### D. Pricing and Ratings

# COMMAND ----------

# MAGIC %md
# MAGIC 1. If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC What if there was an additional $1 charge for any pizza extras?
# MAGIC - Add cheese is $1 extra

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 3. The Pizza Runner team now wants to add an additional ratings system that allows customers to rate their runner, how would you design an additional table for this new dataset - generate a schema for this new table and insert your own data for ratings for each successful customer order between 1 to 5.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Using your newly generated table - can you join all of the information together to form a table which has the following information for successful deliveries?
# MAGIC - customer_id
# MAGIC - order_id
# MAGIC - runner_id
# MAGIC - rating
# MAGIC - order_time
# MAGIC - pickup_time
# MAGIC - Time between order and pickup
# MAGIC - Delivery duration
# MAGIC - Average speed
# MAGIC - Total number of pizzas

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC If a Meat Lovers pizza was $12 and Vegetarian $10 fixed prices with no cost for extras and each runner is paid $0.30 per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### E. Bonus Questions

# COMMAND ----------

# MAGIC %md
# MAGIC If Danny wants to expand his range of pizzas - how would this impact the existing data design? Write an INSERT statement to demonstrate what would happen if a new Supreme pizza with all the toppings was added to the Pizza Runner menu?

# COMMAND ----------

