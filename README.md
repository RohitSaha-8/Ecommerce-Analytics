# E-Commerce Sales Analytics with Spark: Unveiling Insights from Daily and Weekly Transactions

### Description

You work for an e-commerce company as a big data consultant. Your job entails analyzing sales data. The company operates at a number of locations around the world. They want you to analyze the data from their sales transactions on a daily and weekly basis and want you to derive significant insights to understand their sales in various cities and states. You've also been asked to include a variety of other details (that are provided below) about the product evaluation.

Use Spark features for data analysis to derive valuable insights:
Domain: E-commerce
Analysis to be done: Exploratory analysis to determine actionable insights

#### Dataset Overview:
Id,
order_status,
order_products_value,
order_freight_value,
order_items_qty,
order_purchase_timestamp,
order_aproved_at,
order_delivered_customer_date,
customer_city,
customer_state,
customer_zip_code_prefix,
product_name_lenght,
product_description_lenght,
product_photos_qty,
review_score

#### Methodology:
Data Loading and Preprocessing:
- Load dataset into FTP location.
- Copy dataset from FTP location to HDFS.
- Load the dataset into Spark DataFrame from HDFS.
- Perform data cleansing and preprocessing to handle missing values or outliers.
- Aggregate sales and order metrics using Spark SQL queries.
- Calculate average performance metrics and derive insights.
- Utilize Spark's visualization capabilities to present findings effectively.

#### Insights on Historical Data:

       1. Daily insights

          a. Sales
Total sales,

Total sales in each city,

Total sales in each state

          b. Orders

Total number of orders,

City-wise order distribution,

State-wise order distribution,

Average review score per order,

Average freight charges per order,

Average time taken to approve the orders (order approved – order purchased),

Average order delivery time

2.       Weekly insights

        a. Sales

Total sales,

Total sales in each city,

Total sales in each state

        b. Orders

Total number of orders,

City-wise order distribution,

State-wise order distribution,

Average review score per order,

Average freight charges per order,

Average time taken to approve the orders (order approved – order purchased),

Average order delivery time

c . Total freight charges

d. Distribution of freight charges in each city

#### Architecture:

![image](https://github.com/RohitSaha-8/Ecommerce-Analytics/assets/63776719/5a36f7d8-d4fc-477c-b3db-08bc40c52b4c)

#### Conclusion:
By harnessing the power of Spark for data analysis, this project aims to equip the e-commerce company with actionable insights, enabling strategic decision-making and fostering a deeper understanding of sales dynamics across diverse locations. The outcomes will empower the company to enhance operational efficiency, refine marketing strategies, and ultimately improve customer satisfaction.

