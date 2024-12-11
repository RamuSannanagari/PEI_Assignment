from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
)

conf={
  "source_dir": "source_files",
  "source_files":[{"file_name":"Customer.xlsx","file_type":"xlsx","file_header":"true"},
                  {"file_name":"Product.csv","file_type":"csv","file_header":"true"},
                  {"file_name":"Order.json","file_type":"json"}],
  "db_name": "sales",
  "ddl_path": "sql_ddl",
  "ddl_files":["raw_customer.sql","raw_product.sql","raw_orders.sql","enriched_customer.sql","enriched_product.sql","enriched_transactions.sql",'sales_summary.sql'],
  "schemas":{'raw_orders':StructType([
                                      StructField("Row ID", IntegerType(), True),
                                      StructField("Order ID", StringType(), True),
                                      StructField("Order Date", StringType(), True),
                                      StructField("Ship Date", StringType(), True),
                                      StructField("Ship Mode", StringType(), True),
                                      StructField("Customer ID", StringType(), True),
                                      StructField("Product ID", StringType(), True),
                                      StructField("Quantity", IntegerType(), True),
                                      StructField("Price", DecimalType(10, 2), True),
                                      StructField("Discount", DecimalType(5, 2), True),
                                      StructField("Profit", DecimalType(10, 2), True)])
             },
  "data_quality":{"raw_orders":{"data_format":{"order_date": "MM/dd/yyyy",
                                                "ship_date": "MM/dd/yyyy",
                                                "order_id":r"^[A-Z]{2}-\\d{4}-\\d{6}$",
                                                "customer_id":r"^[A-Z]{2}-\\d{5}$",
                                                "product_id":r"^[A-Z]{3}-[A-Z]{2}-\\d{8}$"},
                                },
                  "raw_customer": {"data_format":{ "customer_id":"^[A-Z]{2}-\\d{5}$",
                                                   "email":r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
                                                   "customer_name":r"[^a-zA-Z\s]",
                                                   "phone" :r"^(?:\\+1\\s?)?(\\(?\\d{3}\\)?[\\s.-]?)\\d{3}[\\s.-]?\\d{4}$",
                                                   "postal_code":r"^\d{5}(-\d{4})?$"
                                                   },
                                   },
                  "raw_product": { "data_format":{"product_id":r"^[A-Z]{3}-[A-Z]{2}-\\d{8}$",
                                                  "product_name": r"[^\x00-\x7F]+"
                                                  },
                                },
                  }
}