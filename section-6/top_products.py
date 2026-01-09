import argparse

from pyspark.sql import SparkSession

def calculate_top_products(data_source, output_uri):
    """
    Processes sample orders from the Northwind dataset

    :param data_source: The URI of the northwind dataset folder, such as 's3://DEA-COURSE-BUCKET/northwind-dataset'.
    :param output_uri: The URI where output is written, such as 's3://DEA-COURSE-BUCKET/top_products_results'.
    """
    with SparkSession.builder.appName("Calculate Top Selling Products").getOrCreate() as spark:
        # Load the sales CSV data
        if data_source is not None:
            products_df = spark.read.option("header", "true").csv(data_source + "/products.csv")
            order_details_df = spark.read.option("header", "true").csv(data_source + "/orders_details.csv")

        # Create an in-memory DataFrame to query
        products_df.createOrReplaceTempView("products")
        order_details_df.createOrReplaceTempView("orders_details")

        # Create a DataFrame of the top 10 products
        top_selling_products = spark.sql("""SELECT products.productid, productname, sum(orders_details.quantity) AS total_units_sold 
          FROM products 
          INNER JOIN orders_details
          ON products.productid = orders_details.productid                                         
          GROUP BY products.productid, productname 
          ORDER BY total_units_sold DESC LIMIT 10""")

        # Write the results to the specified output URI
        top_selling_products.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for the northwind dataset, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_top_products(args.data_source, args.output_uri)
			