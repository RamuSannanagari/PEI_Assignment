import os
import json
import shutil
import pyspark.sql.functions as F
from databricks.sdk.runtime import *

import python_lib.config as config

import os

class DataLoad:
    def __init__(self, raw_layer_path: str):
        """
        Initialize the DataLoad class with raw layer path and database name.
        
        Args:
            raw_layer_path (str): Path to the raw data layer.
            database_name (str): Name of the target database.
        """
        self.raw_layer_path = raw_layer_path
        self.database_name = config.conf["db_name"]
        self._validate_path()

    def _validate_path(self):
        """Ensure the raw layer path exists."""
        if not os.path.exists(self.raw_layer_path):
            raise FileNotFoundError(f"Raw layer path '{self.raw_layer_path}' does not exist.")

    def standardize_column_names(df: DataFrame) -> DataFrame:
       """
       Standardizes column names in a PySpark DataFrame.
       
       - Converts all column names to lowercase.
       - Replaces spaces and special characters with underscores.
       - Removes leading and trailing whitespace.
       
       Args:
           df (DataFrame): Input PySpark DataFrame.
       
       Returns:
           DataFrame: DataFrame with standardized column names.
       """
	   
       standardized_columns = [
           col.lower().strip().replace(" ", "_").replace("-", "")
           for col in df.columns
       ]
	   
       return df.toDF(*standardized_columns)

    def raw_layer_process(self, file_name: str):
        """
        Ingest a file into the raw data layer and register it in a database table.
        
        Args:
            file_name (str): Name of the file to ingest.
            table_name (str): Name of the target table.
        
        Returns:
            dataframe: returns the DataFrame with the raw data.
        """
        # Full path to the file
        file_path = os.path.join(self.raw_layer_path, file_name)
        
        # Validate file existence
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_name}' does not exist in the raw layer path '{self.raw_layer_path}'.")
        
        file_type=file_name.split(".")[-1]

        df=None
        if file_type == "csv":
            file_header=[file_dtls[0]["file_header"] for file_dtls in config.conf["source_files"] if file_dtls["file_header"] == file_name][0]
            df = self.spark.read.format(file_type).option("header", file_header).load(file_path)
        elif file_type == "xlsx":
            file_header=[file_dtls[0]["file_header"] for file_dtls in config.conf["source_files"] if file_dtls["file_header"] == file_name][0]
            df = (
                  spark.read
                       .format("com.crealytics.spark.excel")
                       .option("header", file_header)  # Treat the first row as a header
                       .option("inferSchema", "true")  # Infer schema automatically
                       .load(file_path)
                 )

        elif file_type == "json":
            schema= config.conf["schemas"]["raw_orders"]
            df = spark.read.schema(schema).json(file_path)

        if not df.isEmpty():
            # Write the DataFrame to the specified database table
           df=standardize_column_names(df)
           df.withColumn("created_dt", current_date()) \
              .withColumn("created_by", lit("system_user")) 
                  
        return df

    def processed_layer_load(self, raw_df ,raw_table_name):
        """
        Ingest a file into the raw data layer and register it in a database table.
        
        Args:
            raw_df (str): Name of the Raw Table
            raw_table_name (str): Name of the Raw Table
  
        
        Returns:
            dataframe : dataframe with processed column names for quality checks.
        """
        processed_df=None

        if  raw_table_name == "raw_orders":
            order_date_format=config["data_quality"][raw_table_name]["data_format"]["order_date"] 
            ship_date_format=config["data_quality"][raw_table_name]["data_format"]["ship_date"]  
            order_id_regex=config["data_quality"][raw_table_name]["data_format"]["order_id"] 
            customer_id_regex=config["data_quality"][raw_table_name]["data_format"]["customer_id"] 
            product_id_regex=config["data_quality"][raw_table_name]["data_format"]["product_id"] 
          
            # filter  out the invalid records based on ID regex pattern and impute invalid data
            processed_df = raw_df \
                           .filter(
                                   F.col("order_id").rlike(order_id_regex) &
                                   F.col("customer_id").rlike(customer_id_regex) &
                                   F.col("product_id").rlike(product_id_regex) &
                                  ) \
                           .withColumn("order_date",when(raw_df.order_date.rlike(r"^\d{2}/\d{2}/\d{4}$"), to_date(raw_df.order_date,order_date_format)).otherwise(None)) \
                           .withColumn("ship_date",when(raw_df.ship_date.rlike(r"^\d{2}/\d{2}/\d{4}$"), to_date(raw_df.ship_date,ship_date_format)).otherwise(None))   
           
        elif  raw_table_name == "raw_customers":
            customer_id_regex=config["data_quality"][raw_table_name]["data_format"]["customer_id"]
            customer_name_regex=config["data_quality"][raw_table_name]["data_format"]["customer_name"]
            email_regex=config["data_quality"][raw_table_name]["data_format"]["email"]
            phone_regex=config["data_quality"][raw_table_name]["data_format"]["phone"]
            portal_regex=config["data_quality"][raw_table_name]["data_format"]["postal_code"]

            # filter  out the invalid records based on ID regex pattern and impute invalid data
            processed_df = raw_df \
                           .filter(
                                   F.col("customer_id").rlike(customer_id_regex) 
                                  ) \
                           .withColumn("customer_name", F.regexp_replace("customer_name", customer_name_regex, "")) \
                           .withColumn(
                                       "email", F.when(F.col("email").rlike(email_regex), F.col("email")).otherwise(None)
                                      ) \
                           .withColumn(
                                       "phone", F.when(F.col("phone").rlike(phone_regex), F.col("phone")).otherwise(None)
                                    ) \
                           .withColumn(
                                      "postal_code", F.when(F.col("postal_code").rlike(portal_regex), F.col("postal_code")).otherwise(None)
                                     ) 


        elif  raw_table_name == "raw_products":
            product_id_regex=config["data_quality"][raw_table_name]["data_format"]["product_id"]
            product_name_regex=config["data_quality"][raw_table_name]["data_format"]["product_name"]

            processed_df = raw_df \
                           .filter(
                                   F.col("product_id").rlike(product_id_regex) 
                                  ) \
                           .withColumn("product_name", F.regexp_replace("product_name", product_name_regex, "")) 

        return processed_df
    
    def load_table(self, df, table_name):    
       # Write the output DataFrame to the managed transactions Delta table
       df.write.format("delta").mode("append").saveAsTable(f"{self.database_name}.{table_name}")           

                   
  
