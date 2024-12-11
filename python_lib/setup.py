import os
import json
import shutil
from pyspark.sql import SparkSession
from databricks.sdk.runtime import *

import python_lib.config as config

class Setup:
    def __init__(self, root_dir):
        """Initialize the setup class with the root directory."""
        # Root directory of the project
        self.root_dir = root_dir

        # Load configuration 
        self.config = config.conf

        # Extract configuration details
        self.source_dir = os.path.join(self.root_dir, "source_files")
        self.sql_ddl_dir = os.path.join(self.root_dir, "sql_ddl")
        self.source_files = self.config["source_files"]
        self.ddl_files = self.config["ddl_files"]
        self.db_name = self.config["db_name"]

        # Spark session

        print("Setup initialized successfully!")

    def create_raw_layer(self):
        """Creates directories for the raw layer."""
        raw_path = os.path.join(self.root_dir, "raw")
        #dbutils.fs.mkdirs(f"/mnt/{raw_path}")
        os.mkdir(raw_path)
        print(f"Raw layer directory created at: {raw_path}")

    def copy_source_files(self):
        """Copies source files to the raw layer."""
        raw_path = os.path.join(self.root_dir, "raw")
        for file in self.source_files:
            source_file_path = os.path.join(self.source_dir, file["file_name"])
            destination_path = f"{raw_path}/{file["file_name"]}"
            #dbutils.fs.cp(source_file_path, destination_path)
            shutil.copy(source_file_path, destination_path)
            print(f"Copied file: {source_file_path} to {destination_path}")

    def create_database(self):
        """Creates a database in Databricks."""
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        print(f"Database created: {self.db_name}")

    def create_table(self):
        """Creates tables using SQL DDL files."""
        for ddl_file in self.ddl_files:
            ddl_file_path = os.path.join(self.sql_ddl_dir, ddl_file)
            sql_command = None
            spark.sql(f"use {self.db_name}")
            with open(ddl_file_path) as fp:
                sql_command=fp.read()

            if sql_command:    
               spark.sql(sql_command)
               print(f"Executed DDL file: {ddl_file}")
        

    def setup(self):
        """Main setup method to create directories, copy files, and initialize database and tables."""
        self.create_raw_layer()
        self.copy_source_files()
        self.create_database()
        self.create_table()

    def clean_up(self):
        """Cleans up the setup by removing directories and database."""
        raw_path = os.path.join(self.root_dir, "raw")
   
        # Remove directories
        shutil.rmtree(raw_path)

        print("Directories removed.")

        # Drop database
        spark.sql(f"DROP DATABASE IF EXISTS {self.db_name} CASCADE")
        print(f"Database dropped: {self.db_name}")
