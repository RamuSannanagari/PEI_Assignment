# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Summary Data Pipeline
# MAGIC ## Details of Pipeline
# MAGIC This notebook  ingest Raw Layer data and process curated layer datasets and generates final summary sales metrics
# MAGIC
# MAGIC Data Sets: customers(dim) ,product (dim), order transactions ,sales Summary (aggregates)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# import setup module for workspace directories and database ,tables creation
from python_lib.setup import *
from python_lib.dataload import *
# import predefine modules
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json

# COMMAND ----------

import logging

# Step 2: Configure the logger
logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..','logs' ,'debug.log')  , # Log file path
    level=logging.DEBUG,                # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    filemode='w'                        # 'w' to overwrite the file, 'a' to append
)

# Step 3: Create a logger instance
logger = logging.getLogger()


# COMMAND ----------

if __name__ == '__main__':
    try:
        # construtor initiated for loading final transformations
        root_dir=os.getcwd()

        set_up_obj=Setup(root_dir)

        #create database and tables
        set_up_obj.setup()
         

        sales_data_load_obj=Dataload(root_dir)

        # load customer data
        customer_raw_df=sales_data_load_obj.raw_layer_process("Customer.xlsx")
        customer_processed_df=sales_data_load_obj.processed_layer_load(customer_raw_df, "raw_customer")
        sales_data_load_obj.load_data(customer_raw_df, "raw_customer")
        sales_data_load_obj.load_data(customer_processed_df, "customer")

        # load product data
        product_raw_df=sales_data_load_obj.raw_layer_process("Product.csv")
        product_processed_df=sales_data_load_obj.processed_layer_load(product_raw_df, "raw_product")
        sales_data_load_obj.load_data(product_raw_df, "raw_product")
        sales_data_load_obj.load_data(product_processed_df, "product")

        # load orders data
        orders_raw_df=sales_data_load_obj.raw_layer_process("Order.json")
        sales_data_load_obj.load_data(orders_raw_df, "raw_orders")
   
        # load Transactions data
        trans_processed_df=sales_data_load_obj.processed_layer_load(product_raw_df, "raw_orders")

        # Join raw_orders with customers on customer_id
        orders_customers_joined = orders_raw_df.join(
                                                    customer_processed_df, 
                                                    orders_raw_df["customer_id"] == customer_processed_df["customer_id"],
                                                   "inner"
                                                   )
        
        # Join the result with products on product_id
        final_joined_df = orders_customers_joined.join(
                                                      product_processed_df,
                                                      orders_customers_joined["product_id"] == product_processed_df["product_id"],
                                                     "inner"
                                                     )
        
        
        # Select and rename columns as per the transactions table schema
        transactions_df = final_joined_df.select(
                                                 col("row_id"),
                                                 col("order_id"),
                                                 col("order_date"),
                                                 col("ship_date"),
                                                 col("ship_mode"),
                                                 col("quantity"),
                                                 col("price"),
                                                 col("discount"),
                                                 col("profit"),
                                                 col("customer_name"),
                                                 col("country"),
                                                 col("category").alias("product_category"),
                                                 col("subcategory").alias("product_sub_category"),
                                                 current_date().alias("created_dt"),
                                                 col("created_by"),
                                                 current_date().alias("updated_dt"),
                                                 col("updated_by")
                                                ) 

        sales_data_load_obj.load_data(transactions_df, "transactions")
 
        # Aggregate profit by Year, Product Category, Product Subcategory, and Customer
        aggregated_df = transactions_df.groupBy(
            year(col("order_date")).alias("year"),
            col("product_category"),
            col("product_sub_category"),
            col("customer_name")
        ).agg(
            _sum("profit").alias("total_profit")
        )
        
        # Create a temporary view for the new aggregated DataFrame
        new_aggregated_df.createOrReplaceTempView("new_aggregated_view")
        
        spark.sql("""
            MERGE INTO sales_summary AS existing
            USING new_aggregated_view AS new
            ON existing.year = new.year
               AND existing.product_category = new.product_category
               AND existing.product_sub_category = new.product_sub_category
               AND existing.customer_name = new.customer_name
            WHEN MATCHED AND existing.total_profit != new.total_profit THEN
                UPDATE SET existing.total_profit = new.total_profit
            WHEN NOT MATCHED THEN
                INSERT (year, product_category, product_sub_category, customer_name, total_profit)
                VALUES (new.year, new.product_category, new.product_sub_category, new.customer_name, new.total_profit)
        """)
        
        # Execute the query for profit by year
        profit_by_year = spark.sql("""
            SELECT year, SUM(total_profit) AS total_profit_by_year
            FROM sales_summary
            GROUP BY year
            ORDER BY year
        """)
        display(profit_by_year)

        # Execute the query for profit by year + product category
        profit_by_year_category = spark.sql("""
            SELECT year, product_category, SUM(total_profit) AS total_profit_by_year_and_category
            FROM sales_summary
            GROUP BY year, product_category
            ORDER BY year, product_category
        """)

        display(profit_by_year_category)
        
        # Execute the query for profit by customer
        profit_by_customer = spark.sql("""
            SELECT customer_name, SUM(total_profit) AS total_profit_by_customer
            FROM aggregate_transactions
            GROUP BY customer_name
            ORDER BY customer_name
        """)
        
        display(profit_by_customer)

        # Execute the query for profit by customer + year
        profit_by_customer_year = spark.sql("""
            SELECT customer_name, year, SUM(total_profit) AS total_profit_by_customer_and_year
            FROM sales_summary
            GROUP BY customer_name, year
            ORDER BY customer_name, year
        """)
       
        display(profit_by_customer_year)

        set_up_obj.clean_up()

    except Exception as e:
        logger.error("An error occurred: %s", e)
        raise

# COMMAND ----------

