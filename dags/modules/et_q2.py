from airflow.models import Connection
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = (
    SparkSession.builder.config(
        "spark.jars.packages", "org.postgresql:postgresql:42.7.0"
    )
    .master("local")
    .appName("PySpark_Postgres")
    .getOrCreate()
)


def extract_transform_total_films_by_category(conn_data):
    df_city = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.film")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_city.createOrReplaceTempView("film")

    df_country = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.film_category")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_country.createOrReplaceTempView("film_category")

    df_customer = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.category")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_customer.createOrReplaceTempView("category")

    df_result = spark.sql(
        """
    SELECT
        name as category_name,
        COUNT(film_id) as total_film,
        current_date() as date,
        'jack_airflow' as data_owner
    FROM category
    JOIN film_category ON category.category_id = film_category.category_id
    GROUP BY name
    ORDER BY total_film DESC
    """
    )

    print(f"Result: {df_result.count()}")

    df_result.show()

    df_result.write.mode("overwrite").partitionBy("date").option(
        "compression", "snappy"
    ).option("partitionOverwriteMode", "dynamic").save(
        os.getcwd() + "/output/data_result_2"
    )
