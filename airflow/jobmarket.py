from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pandas as pd
import requests
from datetime import datetime
import os
import json
from sqlalchemy import create_engine, MetaData, text
from pymongo import MongoClient
from flatten_json import flatten



###Connexion aux bases de données
### MYSQL
username = "ff"
password = Variable.get("mysql_ff_jobmarket")
hostname = Variable.get("ip_jobmarket")
port = "3306"
database_name = "jobmarket"

db_url = f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database_name}"
metadata = MetaData()
engine = create_engine(db_url)

### MONGODB
client = MongoClient(
    host= Variable.get("ip_jobmarket"),
    port = 27017,
    username = "ff",
    password = Variable.get("mysql_ff_jobmarket")
    )

with DAG(
    dag_id='job-adzuna',
    description='JOBMARKET ADZUNA',
    tags=['jobmarket', 'ETL'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    }
) as my_dag:


    def adzuna_to_json(countries, first_page, last_page):
        """
        Fonction qui récupère les données d'ADZUNA et qui les enregistre vers un registre
        """

        appid = "a5b42571"
        key = Variable.get("api_adzuna")
        # This function take a list of different countries and the first and last page we wan to extract.
        results, success, failed = [], [], [] # Initialize 3 lists, one for extraction, two others for log.
        for country in countries:
            for page in range(first_page, last_page):
                url = "https://api.adzuna.com/v1/api/jobs/{3}/search/{0}?app_id={1}&app_key={2}".format(page, appid, key, country)
                req = requests.get(url)
                if req.status_code == 200: # If we get a positive answer we store data to result
                    try:
                        data = req.json()
                        results.extend(data["results"])
                    except:
                        failed.append("{} : {}".format(country, page))
                        print(req.status_code, " pour la page ", page, "du pays ", country)
                    else:
                        success.append("{} : {}".format(country, page))

                else :
                    print(req.status_code, " pour la page ", page, "du pays ", country)

        # Get the current date and write into log file
        current_date = datetime.now().strftime("%Y-%m-%d")
        #append_log_file(f"{current_date};{len(success)};{len(failed)}","adzuna_to_json.csv")
        print(f"ADZUNA TO JSON \nSuccess : {len(success)}\nFailed : {len(failed)}")
        print(len(results))


        # write results to JSON for audit
        now = datetime.now().strftime("%d-%m-%Y")
        path = "/app/raw_files/" + now + ".json"
        with open(path, 'w') as f:
            json.dump(results, f)

    
    def json_to_database():
        now = datetime.now().strftime("%d-%m-%Y")
        path = "/app/raw_files/" + now + ".json"
        with open(path, 'r') as file:
            json_data = json.load(file)
        
        success_insert, duplicate_insert, failed_insert = 0, 0, 0
        columns = ["id", "title", "created", "location_display_name", "location_area_0", "redirect_url", "company_display_name", "salary_is_predicted", "contract_type", "category_label", "latitude", "longitude"]

        job = client["job"] #Connexion to MongoDB
        with engine.connect() as connection: # Connexion to SQL
            for annonce in json_data:
                annonce_flatten = flatten(annonce)
                
            
                date_creation = str(annonce['created']).replace("T", " ").replace("Z", "")


                text_columns = "("
                text_values = "("
                for column in columns:
                    if column in annonce_flatten.keys():
                        text_columns = f"{text_columns}{column}, "
                        if column == "created": #Si c'est une date je formate
                            text_values = f'{text_values}"{annonce_flatten[column].replace("T", " ").replace("Z", "")}", '
                        elif column == "salary_is_predicted" or column == "latitude" or column == "longitude": # Si c'est un chiffre je récupère seulement
                            text_values = f'{text_values}{annonce_flatten[column]}, '
                        else: # Si c'est du texte, j'ajoute des double quotes
                            text_values = f'{text_values}"{annonce_flatten[column]}", '

                text_columns = text_columns[:-2] + ")"
                text_values = text_values[:-2] + ")"

                replacement = {"title" : "nom_poste", "created" : "date_creation", "location_display_name" : "ville", "location_area_0" : "pays", "redirect_url" : "url", "company_display_name" : "nom_entreprise", "salary_is_predicted" : "salaire", "contract_type" : "type_contrat", "category_label" : "categorie"}
                for key, value in replacement.items():
                    text_columns = text_columns.replace(key, value)

                request_text = f"INSERT INTO POSTES {text_columns} VALUES {text_values}"

                clean_description = annonce['description'].replace('"', " ")
                mongodb_value = r'{"id":' + str(annonce['id']) + ', "description":"' + clean_description + '"}'
                mongodb_json = json.loads(mongodb_value)
                


                # On ajoute la ligne à la base de données
                try:
                    connection.execute(text(request_text))
                except:
                    duplicate_insert += 1
                else:
                    success_insert += 1
                    job.descriptions.insert_one(mongodb_json) # Si ce n'est pas un doublon, alors on importe l'id et la description dans MONGODB

                


            # Get the current date and write into log file
            current_date = datetime.now().strftime("%Y-%m-%d")
            #append_log_file(f"{current_date};{success_insert};{duplicate_insert};{failed_insert}","json_to_database.csv")     
            print(f"\nJSON TO DATABASE \nSuccess : {success_insert}\nDuplucate : {duplicate_insert}\nFailed : {failed_insert}")



    extract = PythonOperator(
        task_id='extract_data',
        python_callable=adzuna_to_json,
        op_kwargs={'countries': ['fr'],'first_page': 1,'last_page': 6},
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=json_to_database,
    )

    extract >> load