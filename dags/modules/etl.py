import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd


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

    df_result.write.mode("overwrite").partitionBy("date").option(
        "compression", "snappy"
    ).option("partitionOverwriteMode", "dynamic").save("output/data_result_1")


def load_top_countries():
    df = pd.read_parquet("output/data_result_1")

    engine = create_engine(
        "mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/project3",
        echo=False,
    )

    df.to_sql(name="top_country", con=engine, if_exists="append")
