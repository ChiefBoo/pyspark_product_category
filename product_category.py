from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("ProductCategoryExample") \
    .getOrCreate()

products_data = [
    (1, "Product1"),
    (2, "Product2"),
    (3, "Product3"),
    (4, "Product4")
]

categories_data = [
    (1, "Category1"),
    (2, "Category2"),
    (3, "Category3")
]

product_category_relations_data = [
    (1, 1),
    (2, 2),
    (3, 1),
    (3, 3),
]

products = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_relations = spark.createDataFrame(product_category_relations_data, ["product_id", "category_id"])

product_category_pairs = product_category_relations.join(products, "product_id") \
    .join(categories, "category_id") \
    .select("product_name", "category_name")

products_without_categories = products.join(product_category_relations, "product_id", "left_anti")

product_category_pairs.show()
products_without_categories.show()

spark.stop()