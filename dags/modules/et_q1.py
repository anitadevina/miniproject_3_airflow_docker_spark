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


def extract_transform_top_countries(conn_data):
    df_city = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.city")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_city.createOrReplaceTempView("city")

    df_country = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.country")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_country.createOrReplaceTempView("country")

    df_customer = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.customer")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_customer.createOrReplaceTempView("customer")

    df_address = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.address")
        .option("user", conn_data.login)
        .option("password", conn_data.password)
        .load()
    )
    df_address.createOrReplaceTempView("address")

    df_result = spark.sql(
        """
    SELECT
        country,
        COUNT(country) as total,
        current_date() as date,
        'jack_airflow' as data_owner
    FROM customer
    JOIN address ON customer.address_id = address.address_id
    JOIN city ON address.city_id = city.city_id
    JOIN country ON city.country_id = country.country_id
    GROUP BY country
    ORDER BY total DESC
    """
    )

    print(f"Result: {df_result.count()}")

    df_result.show()

    df_result.write.mode("overwrite").partitionBy("date").option(
        "compression", "snappy"
    ).option("partitionOverwriteMode", "dynamic").save(
        os.getcwd() + "/output/data_result_1"
    )
