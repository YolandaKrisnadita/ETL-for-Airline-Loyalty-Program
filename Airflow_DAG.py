
import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch



#Fetch from Postgresql

def queryPostgresql():
    ''' 
    Fungsi ini ditujukan untuk mengambil data dari postgresSQL.
    Parameter:
        Database: airflow
        Hostname: postgres
        Username: airflow
        Password: airflow
        Table: table_m3
    Mengambil seluruh isi data dengan perintah SELECT *
    Lalu menyimpannnya dalam bentuk file .csv
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select * from table_m3",conn) 
    df.to_csv('/opt/airflow/dags/airline_loyalty_data_raw.csv',index=False)
    print("-------Data Saved------")


#Data Cleaning
def cleandata():
    ''' 
    Fungsi ini ditujukan untuk melakukan cleaning dataset yang telah diambil dari postgresSQL.
    Parameter:
        1. Mengubah nama column menjadi lowercase
        2. Mengganti (spasi) dengan (_)
        3. Mengubah NaN Value menjadi 0 karena dalam hal ini kolom yang diubah valuenya adalah kolom tahun pembatalan keanggotaan, 
        karena hal ini sangat mempengaruhui analisa engagement pelanggan mangkanya tidak diisi dengan tahun tapi dengan angka 0
    Lalu menyimpannnya dalam bentuk file .csv
    '''
    df=pd.read_csv('/opt/airflow/dags/airline_loyalty_data_raw.csv')
    df.columns=[x.lower() for x in df.columns]
    df.columns = df.columns.str.replace(' ', '_') 
    df.fillna(0,inplace=True)
    df.to_csv('/opt/airflow/dags/airline_loyalty_data_clean.csv',index=False)

#Post to Elasticsearch

def insertElasticsearch():
    ''' 
    Fungsi ini ditujukan untuk melakukan Upload data yang sudah rapi kedalam Elastic search untuk selanjutnya dilakukan visualisasi dengan kibana.
    Parameter:
        1. Membaca file csv yang sudah clean,
        2. memberikan nama index untuk elasticsearch dengan nama dataclean_2
    '''
    es = Elasticsearch("http://Elasticsearch:9200") 
    df=pd.read_csv('/opt/airflow/dags/airline_loyalty_data_clean.csv')
    es.ping()

    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="dataclean_2",doc_type="doc",body=doc)
        print(res)	 


default_args = {
    'owner': 'Yoland',
    'start_date': dt.datetime(2024, 3, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('P2M3_Yolanda-Krisnadita',
         default_args=default_args,
         schedule_interval='30 6 * * *',      # '0 * * * *',
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=queryPostgresql)
    
    cleanData = PythonOperator(task_id='CleanTheData',
                                 python_callable=cleandata)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch)



getData >> cleanData >> insertData