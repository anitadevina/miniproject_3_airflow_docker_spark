from sqlalchemy import create_engine
import pandas as pd
import os


engine = create_engine(
    "mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/project3",
    echo=False,
)


def ingest_top_countries():
    df_country = pd.read_parquet(os.getcwd() + "/output/data_result_1")

    df_country.to_sql(name="top_country", con=engine, if_exists="replace")


def ingest_total_films_by_category():
    df_category = pd.read_parquet(os.getcwd() + "/output/data_result_2")

    df_category.to_sql(name="total_film_by_category", con=engine, if_exists="replace")
