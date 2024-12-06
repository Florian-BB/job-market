from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine, MetaData
import os
api = FastAPI()

username = "ff"
password = os.environ.get("mysql_ff_jobmarket")
hostname = "job-mysql"
port = "3306"
database_name = "jobmarket"

db_url = f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database_name}"
metadata = MetaData()
engine = create_engine(db_url)



@api.get('/located_jobs')
def get_greetings():
    query = "SELECT * FROM POSTES WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    df = pd.read_sql(query, engine)

    # Conversion du DataFrame en liste de dictionnaires
    result = df.to_dict(orient='records')

    return result