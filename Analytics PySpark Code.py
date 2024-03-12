var df = spark.read.format("csv").option("delimiter", ",").option("header", "false").option("inferSchema", "true").load("ECommerce/olist_public_dataset.csv").toDF("id" ,"order_status" ,"order_products_value" ,"order_freight_value" ,"order_items_qty" ,"customer_city" ,"customer_state" ,"customer_zip_code_prefix" ,"product_name_length" ,"product_description_length" ,"product_photos_qty" ,"review_score" ,"order_purchase_timestamp" ,"order_aproved_at" ,"order_delivered_customer_date")

Extract only ‘date’ from all 3 timestamp columns respectively :
var date_df = df.withColumn("PurchaseDate",to_date(unix_timestamp(col("order_purchase_timestamp"), "dd/MM/yy H:mm").cast("timestamp"))).withColumn("ApproveDate",to_date(unix_timestamp(col("order_aproved_at"), "dd/MM/yy H:mm").cast("timestamp"))).withColumn("DeliveryDate",to_date(unix_timestamp(col("order_delivered_customer_date"), "dd/MM/yy H:mm").cast("timestamp")))

1. DAILY INSIGHTS
a. Sales

Total sales :
var sales_df = date_df.groupBy(col("PurchaseDate")).agg(round(sum(col("order_products_value")),2).as("Total_Sales")).orderBy(asc("PurchaseDate"))

Total sales in each city :
var sales_city_df = date_df.groupBy(col("PurchaseDate"),col("customer_city")).agg(round(sum(col("order_products_value")),2).as("Sales_per_City")).orderBy(asc("PurchaseDate"))

Total sales in each state :
var sales_state_df = date_df.groupBy(col("PurchaseDate"),col("customer_state")).agg(round(sum(col("order_products_value")),2).as("Sales_per_State")).orderBy(asc("PurchaseDate"))

b. Orders

Total number of orders :
var total_order = date_df.groupBy("PurchaseDate").agg(count("*").as("Total_Orders")).orderBy(asc("PurchaseDate"))

City-wise order distribution :
var city_order = date_df.groupBy(col("PurchaseDate"),col("customer_city")).agg(count("*").as("Total_Orders")).orderBy(asc("PurchaseDate"))

State-wise order distribution :
var state_order = date_df.groupBy(col("PurchaseDate"),col("customer_state")).agg(count("*").as("Total_Orders")).orderBy(asc("PurchaseDate"))

Average review score per order :
var avg_rev = date_df.groupBy(col("PurchaseDate")).agg(round(sum(col("review_score"))/count("*"),3).as("Avg_Rev_per_Order")).orderBy(asc("PurchaseDate"))

Average freight charges per order :
var avg_fre = date_df.groupBy(col("PurchaseDate")).agg(round(sum(col("order_freight_value"))/count("*"),3).as("Avg_Frei_per_Order")).orderBy(asc("PurchaseDate"))

Average time taken to approve the orders (order approved – order purchased) :
import org.apache.spark.sql.types.DoubleType
var approval_diff = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "dd/MM/yy H:mm")).withColumn("order_aproved_at", to_timestamp(col("order_aproved_at"), "dd/MM/yy H:mm")).withColumn("ApprovalDifference(InMinutes)", ((col("order_aproved_at").cast("long") - col("order_purchase_timestamp").cast("long")) / 60).cast(DoubleType)).select("id", "order_purchase_timestamp", "order_aproved_at", "ApprovalDifference(InMinutes)")
var avg_approve = approval_diff.agg(round(avg("ApprovalDifference(InMinutes)"),3).as("Average Time To Approve(Minutes)"))

Average order delivery time :
var delivery_diff = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "dd/MM/yy H:mm")).withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"), "dd/MM/yy H:mm")).withColumn("DeliveryDifference(InHours)", ((col("order_delivered_customer_date").cast("long") - col("order_purchase_timestamp").cast("long")) / 3600).cast(DoubleType)).select("id", "order_purchase_timestamp", "order_delivered_customer_date", "DeliveryDifference(InHours)")
var avg_delivery = delivery_diff.agg(round(avg("DeliveryDifference(InHours)"),3).as("Average Time To Deliver(Hours)"))


2. WEEKLY INSIGHTS

new dataframe having conversion of date to week number :
var weekly_df = date_df.withColumn("WeekNumber", weekofyear(col("PurchaseDate")))

a. Sales

Total sales :
var weeklysales_df = weekly_df.groupBy("WeekNumber").agg(round(sum(col("order_products_value")),2).as("Total_Weekly_Sales")).orderBy("WeekNumber")

Total sales in each city :
var weeklysales_city_df = weekly_df.groupBy(col("WeekNumber"),col("customer_city")).agg(round(sum(col("order_products_value")),2).as("Total_Weekly_Sales")).orderBy("WeekNumber")

Total sales in each state :
var weeklysales_state_df = weekly_df.groupBy(col("WeekNumber"),col("customer_state")).agg(round(sum(col("order_products_value")),2).as("Total_Weekly_Sales")).orderBy("WeekNumber")

b. Orders

Total number of orders :
var totalweekly_order = weekly_df.groupBy("WeekNumber").agg(count("*").as("Total_Weekly_Orders")).orderBy("WeekNumber")

City-wise order distribution :
var weekly_city_order = weekly_df.groupBy(col("WeekNumber"),col("customer_city")).agg(count("*").as("Total_Weekly_Orders")).orderBy("WeekNumber")

State-wise order distribution :
var weekly_state_order = weekly_df.groupBy(col("WeekNumber"),col("customer_state")).agg(count("*").as("Total_Weekly_Orders")).orderBy("WeekNumber")

Average review score per order :
var weekly_avg_rev = weekly_df.groupBy(col("WeekNumber")).agg(round(sum(col("review_score"))/count("*"),3).as("Avg_Rev_per_Order")).orderBy("WeekNumber")

Average freight charges per order :
var weekly_avg_fre = weekly_df.groupBy(col("WeekNumber")).agg(round(sum(col("order_freight_value"))/count("*"),3).as("Avg_Rev_per_Order")).orderBy("WeekNumber")

Average time taken to approve the orders (order approved – order purchased) :
converting the three ‘timestamp’ columns to unified format:
var df2 = date_df.withColumn("WeekNumber", weekofyear(col("PurchaseDate"))).withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "dd/MM/yy H:mm")).withColumn("order_aproved_at", to_timestamp(col("order_aproved_at"), "dd/MM/yy H:mm")).withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"), "dd/MM/yy H:mm"))
var weekly_approval_diff = df2.withColumn("TimeDifference(InMinutes)", ((col("order_aproved_at").cast("long") - col("order_purchase_timestamp").cast("long")) / 60).cast(DoubleType)).groupBy("WeekNumber").agg(avg("TimeDifference(InMinutes)").as("WeeklyAverageTimeToApprove(Minutes)")).orderBy("WeekNumber")
var weekly_avg_approve = weekly_approval_diff.agg(round(avg("WeeklyAverageTimeToApprove(Minutes)"),3).as("Weekly AVG Approval Time(InMinutes)"))

Average order delivery time :
var weekly_delivery_diff = df2.withColumn("TimeDifference(InHours)", ((col("order_delivered_customer_date").cast("long") - col("order_purchase_timestamp").cast("long")) / 3600).cast(DoubleType)).groupBy("WeekNumber").agg(avg("TimeDifference(InHours)").as("WeeklyAverageTimeToDeliver(Hours)")).orderBy("WeekNumber")
var weekly_avg_delivery = weekly_delivery_diff.agg(round(avg("WeeklyAverageTimeToDeliver(Hours)"),3).as("Weekly AVG Delivery Time(InHours)"))

c. Total freight charges :
var weeklyFreight_df = weekly_df.groupBy("WeekNumber").agg(round(sum(col("order_freight_value")),2).as("Total_Freight_Charges")).orderBy("WeekNumber")

d. Distribution of freight charges in each city :
var weeklyFreight_city_df = weekly_df.groupBy(col("WeekNumber"),col("customer_city")).agg(round(sum(col("order_freight_value")),2).as("City_Freight_Charges")).orderBy("WeekNumber")
