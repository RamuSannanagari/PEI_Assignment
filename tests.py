import unittest
import datetime
from unittest.mock import patch,MagicMock,create_autospec,Mock

from datetime import timedelta
from datetime import date as dt

import pyspark.sql.functions as f
from pyspark.sql import *
from pyspark.sql.types import TimestampType,StringType,StructType,StructField,DateType,IntegerType,NumericType,DecimalType,LongType

import pyspark
import pandas as pd
import sys,datetime,json
import os

from python_lib.setup import *
from python_lib.dataload import *


class SalesloadTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.set_up_obj=Setup(root_dir)
        cls.sales_data_load_obj=Dataload(root_dir)

    def test_database_tables_check(self):
 
        self.set_up_obj.setup()

        expected_database='sales'
        expected_tables=['customers','products','transactions','raw_customers','raw_orders','raw_products','sales_summary']

        # Check if the database exists
        databases = [db.name for db in self.spark.sql("SHOW DATABASES").collect()]
        self.assertIn(expected_database, databases, f"Database '{expected_database}' does not exist.")

        # Switch to the expected database
        spark.sql(f"USE {expected_database}")

        # Get the list of tables in the database
        actual_tables = [table.name for table in self.spark.sql("SHOW TABLES").collect()]

        # Check if all expected tables are present in the database
        for table in expected_tables:
            self.assertIn(table, actual_tables, , f"Table '{table}' does not exist in database '{expected_database}'.") 

    def test_standardize_column_names_with_orders_data(self):
        # Create a DataFrame from the provided orders data
        orders_data = [
            Row(**{
                "Row ID": 9772,
                "Order ID": "CA-2016-119165",
                "Order Date": "31/10/2016",
                "Ship Date": "6/11/2016",
                "Ship Mode": "Standard Class",
                "Customer ID": "BD-11320",
                "Product ID": "FUR-CH-10000988",
                "Quantity": 5,
                "Price": 492.835,
                "Discount": 0.3,
                "Profit": -14.081
            })
        ]
        df = spark.createDataFrame(orders_data)

        # Apply the function
        standardized_df = self.sales_data_load_obj.standardize_column_names(df)

        # Expected standardized column names
        expected_columns = [
            "row_id", "order_id", "order_date", "ship_date", "ship_mode",
            "customer_id", "product_id", "quantity", "price", "discount", "profit"
        ]

        # Assert column names are standardized as expected
        self.assertEqual(standardized_df.columns, expected_columns, 
                         "Column names were not standardized correctly.")

    def test_raw_layer_process_with_valid_filename(self):
            
            file_name = "Product.csv"
        
            # Apply the function
            product_raw_df=self.sales_data_load_obj.raw_layer_process(file_name)
        
        
            # Assert column names are standardized as expected
            self.assertEqual(product_raw_df.count(), 1851, "raw dataframe not correctly created")
        
    def test_raw_layer_process_with_in_valid_filename(self):
            
            invalid_file_name = "Product1.csv"
        
            # Apply the function
        	with self.assertRaises(FileNotFoundError):
                 product_raw_df=self.sales_data_load_obj.raw_layer_process(invalid_file_name)

    def test_valid_raw_orders(self):
        # Create test data
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("ship_date", StringType(), True)
        ])
        data = [
            ("CA-2016-119165", "BD-11320", "FUR-CH-10000988", "31/10/2016", "06/11/2016")
        ]
        raw_df = spark.createDataFrame(data, schema)

        # Run the function
        processed_df = self.sales_data_load_obj.processed_layer_load(raw_df, "raw_orders")

        # Assertions
        self.assertEqual(processed_df.count(), 1)
        self.assertTrue("order_date" in processed_df.columns)
        self.assertTrue("ship_date" in processed_df.columns)

    def test_invalid_raw_orders(self):
        # Create test data with invalid order_id
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("ship_date", StringType(), True)
        ])
        data = [
            ("INVALID_ORDER_ID", "BD-11320", "FUR-CH-10000988", "31/10/2016", "06/11/2016")
        ]
        raw_df = self.spark.createDataFrame(data, schema)

        # Run the function
        processed_df = self.sales_data_load_obj.processed_layer_load(raw_df, "raw_orders")

        # Assertions
        self.assertEqual(processed_df.count(), 0)

    def test_valid_raw_customers(self):
        # Create test data
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("postal_code", StringType(), True)
        ])
        data = [
            ("BD-11320", "John Doe", "john.doe@gmail.com", "+1 123-456-7890", "12345")
        ]
        raw_df = self.spark.createDataFrame(data, schema)

        # Run the function
        processed_df = self.sales_data_load_obj.processed_layer_load(raw_df, "raw_customers")

        # Assertions
        self.assertEqual(processed_df.count(), 1)
        self.assertTrue("email" in processed_df.columns)
        self.assertTrue("phone" in processed_df.columns)

    def test_invalid_raw_customers(self):
        # Create test data with invalid customer data
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("postal_code", StringType(), True)
        ])
        data = [
            ("INVALID_ID", "Invalid_Name123", "invalid_email@", "12345", "ABCDE"),  # All invalid
            ("BD-11320", "John Doe", "john.doe@gmail.com", "123-456-7890", "12345"),  # Valid row
        ]
        raw_df = spark.createDataFrame(data, schema)
    
        # Run the function
        processed_df = self.sales_data_load_obj.processed_layer_load(raw_df, "raw_customers")
    
        # Collect results
        processed_data = processed_df.collect()
    
        # Assertions
        self.assertEqual(len(processed_data), 1)  # Only 1 valid row should remain
        self.assertEqual(processed_data[0]["customer_id"], "BD-11320")
        self.assertEqual(processed_data[0]["customer_name"], "John Doe")
        self.assertEqual(processed_data[0]["email"], "john.doe@gmail.com")
        self.assertEqual(processed_data[0]["phone"], "123-456-7890")
        self.assertEqual(processed_data[0]["postal_code"], "12345")

    def test_valid_raw_products(self):
        # Sample valid raw products data
        valid_raw_products_data = [
            {"product_id": "FUR-CH-10000988", "product_name": "Furniture Chair"},
            {"product_id": "OFF-SU-10000454", "product_name": "Office Supplies"},
        ]
    
        valid_raw_products_df = spark.createDataFrame(valid_raw_products_data)

    
        # Process the raw products
        processed_df = self.sales_data_load_obj.processed_layer_load(
            raw_df=valid_raw_products_df,
            raw_table_name="raw_products",
        )
    
        # Assertions for valid records
        processed_product_ids = [row["product_id"] for row in processed_df.collect()]
        expected_product_ids = [row["product_id"] for row in valid_raw_products_data]
        self.assertEqual(processed_product_ids, expected_product_ids)
    
        processed_product_names = [row["product_name"] for row in processed_df.collect()]
        expected_product_names = [row["product_name"] for row in valid_raw_products_data]
        self.assertEqual(processed_product_names, expected_product_names)

    def test_valid_raw_products(self):
        # Sample valid raw products data
        valid_raw_products_data = [
            {"product_id": "FUR-CH-10000988", "product_name": "Furniture Chair"},
            {"product_id": "OFF-SU-10000454", "product_name": "Office Supplies"},
        ]
    
        valid_raw_products_df = spark.createDataFrame(valid_raw_products_data)
    
    
        # Process the raw products
        processed_df = self.sales_data_load_obj.processed_layer_load(
            raw_df=valid_raw_products_df,
            raw_table_name="raw_products",
        )
    
        # Assertions for valid records
        processed_product_ids = [row["product_id"] for row in processed_df.collect()]
        expected_product_ids = [row["product_id"] for row in valid_raw_products_data]
        self.assertEqual(processed_product_ids, expected_product_ids)
    
        processed_product_names = [row["product_name"] for row in processed_df.collect()]
        expected_product_names = [row["product_name"] for row in valid_raw_products_data]
        self.assertEqual(processed_product_names, expected_product_names)


if __name__ == '__main__':
    unittest.main()