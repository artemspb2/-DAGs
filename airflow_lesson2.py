#!/usr/bin/env python
# coding: utf-8

# In[10]: some changes


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[11]:



TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[12]:

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_zone():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['domain_zone'] = top_doms['domain'].apply(lambda x: x.split('.')[-1])
    top_10_size_domain = top_doms.groupby('domain_zone', as_index=False)\
                             .agg({'rank':'count'})\
                             .sort_values(by = 'rank', ascending = False)\
                             .rename(columns={'rank':'amount'})\
                             .head(10)
    with open('top_10_size_domain.csv', 'w') as f:
        f.write(top_10_size_domain.to_csv(index=False, header=False))

def domain_lenght():
    lenght_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    lenght_df['domain_lenght'] = lenght_df['domain'].apply(lambda x: len(str(x)))
    top_lenght_domain = lenght_df.sort_values(by = ['domain_lenght','domain'], ascending = [False, True]).head(1).domain
    with open('top_lenght_domain.csv', 'w') as f:
        f.write(top_lenght_domain.to_csv(index=False, header=False))   
    
def airflow_rank():
    airflow_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = airflow_df.query('domain == "airflow.com"')['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))
        
def print_data(ds):
    with open('top_10_size_domain.csv', 'r') as f:
        top_10_size_domain = f.read()
        
    with open('top_lenght_domain.csv', 'r') as f:
        top_lenght_domain = f.read()
    
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
   
    date = ds
    
    print(f'Top-10 domain zone by amounth of domains for date {date}')
    print(top_10_size_domain)
    
    print(f'Dommain with highest lenght name for date {date}')
    print(top_lenght_domain)
    
    print(f'Airflow_rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'ar-davydov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 15),
}
schedule_interval = '0 12 * * *'

dag = DAG('ar-davydov_top_domains', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_zone',
                    python_callable=top_zone,
                    dag=dag)

t3 = PythonOperator(task_id='domain_lenght',
                        python_callable=domain_lenght,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=domain_lenght,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5



