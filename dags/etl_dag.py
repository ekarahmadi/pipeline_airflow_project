from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
# Impor fungsi-fungsi scraping dan lainnya dari skrip utama
from script import scrape_detik, scrape_pikiran_rakyat
import clickhouse_connect

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedult_interval': '@daily',
    'start_date': datetime(2024, 11, 10),  # Sesuaikan dengan tanggal mulai DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisialisasi DAG dengan using 'with' notation
with DAG(
    'ETL_DAG',
    default_args=default_args,
    description='Scraping Detik and Pikiran Rakyat and inserting to PostgreSQL',
    schedule_interval=timedelta(days=1),  # Menjadwalkan setiap hari
) as dag:

    # Define task to scrape Detik
    def scrape_detik_task(task_name, name, url, **kwargs):
        # Call the scraping function and get the DataFrame
        df = scrape_detik(1, name, url)
        # Push the DataFrame as an XCom
        kwargs['ti'].xcom_push(key=task_name, value=df)
        return df

    def scrape_pikiran_rakyat_task(task_name, name, url, **kwargs):
        # Call the scraping function and get the DataFrame
        df = scrape_pikiran_rakyat(1, name, url)
        # Push the DataFrame as an XCom
        kwargs['ti'].xcom_push(key=task_name, value=df)
        return df

    scrape_detik_task_1 = PythonOperator(
        task_id='scrape_detik_1',
        python_callable=scrape_detik_task,
        op_args=['scrape_detik_1', 'Andika Perkasa', 'https://www.detik.com/search/searchall?query=andika%20perkasa&page={page}&result_type=latest'],
        provide_context=True,  # Enable context to access XCom
        dag=dag
    )

    scrape_detik_task_2 = PythonOperator(
        task_id='scrape_detik_2',
        python_callable=scrape_detik_task,
        op_args=['scrape_detik_2', 'Ahmad Luthfi', 'https://www.detik.com/search/searchall?query=ahmad%20luthfi&page={page}&result_type=latest'],
        provide_context=True,
        dag=dag
    )

    scrape_pikiran_rakyat_task_1 = PythonOperator(
        task_id='scrape_pikiran_rakyat_1',
        python_callable=scrape_pikiran_rakyat_task,
        op_args=['scrape_pikiran_rakyat_1', 'Andika Perkasa', 'https://www.pikiran-rakyat.com/search/?q=andika+perkasa&page={page}#gsc.tab=0&gsc.q=andika%20perkasa&gsc.page={page}'],
        provide_context=True,
        dag=dag
    )

    scrape_pikiran_rakyat_task_2 = PythonOperator(
        task_id='scrape_pikiran_rakyat_2',
        python_callable=scrape_pikiran_rakyat_task,
        op_args=['scrape_pikiran_rakyat_2', 'Ahmad Luthfi', 'https://www.pikiran-rakyat.com/search/?q=ahmad+luthfi&page={page}#gsc.tab=0&gsc.q=ahmad%20luthfi&gsc.page={page}'],
        provide_context=True,
        dag=dag
    )

    # Pull the DataFrame from XCom and pass it to the next task
    def process_data_from_xcom(task_name, **kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids=task_name)
        # Process the DataFrame (e.g., merge, clean, or save to DB)
        logging.info(f"Processed DataFrame from {task_name}: {df.head()}")
        return df
    
    def combine_all(task_name_1, task_name_2, task_name_3, task_name_4, **kwargs):
        ti = kwargs['ti']
        df_1 = ti.xcom_pull(task_ids=task_name_1)
        df_2 = ti.xcom_pull(task_ids=task_name_2)
        df_3 = ti.xcom_pull(task_ids=task_name_3)
        df_4 = ti.xcom_pull(task_ids=task_name_4)
        combined_df = pd.concat([df_1, df_2, df_3, df_4], ignore_index=True)
        logging.info(combined_df.info())

        client = clickhouse_connect.get_client(
            host='wwjgh4iil1.ap-southeast-1.aws.clickhouse.cloud',
            user='default',
            password='Ss606bB0N_i~G',
            secure=True
        )

        client.insert_df("scraped_articles", combined_df)

        return combined_df


    process_detik_1 = PythonOperator(
        task_id='process_detik_1',
        python_callable=process_data_from_xcom,
        op_args=['scrape_detik_1'],
        provide_context=True,
        dag=dag
    )

    process_detik_2 = PythonOperator(
        task_id='process_detik_2',
        python_callable=process_data_from_xcom,
        op_args=['scrape_detik_2'],
        provide_context=True,
        dag=dag
    )

    process_pikiran_rakyat_1 = PythonOperator(
        task_id='process_pikiran_rakyat_1',
        python_callable=process_data_from_xcom,
        op_args=['scrape_pikiran_rakyat_1'],
        provide_context=True,
        dag=dag
    )

    process_pikiran_rakyat_2 = PythonOperator(
        task_id='process_pikiran_rakyat_2',
        python_callable=process_data_from_xcom,
        op_args=['scrape_pikiran_rakyat_2'],
        provide_context=True,
        dag=dag
    )

    combine_all_task = PythonOperator(
        task_id='combined_task_id',
        python_callable=combine_all,
        op_args=['scrape_detik_1', 'scrape_detik_2', 'scrape_pikiran_rakyat_1', 'scrape_pikiran_rakyat_2'],
        provide_context=True,
        dag=dag
    )

    # Set task dependencies
    scrape_detik_task_1 >> scrape_detik_task_2
    scrape_detik_task_2 >> scrape_pikiran_rakyat_task_1
    scrape_pikiran_rakyat_task_1 >> scrape_pikiran_rakyat_task_2
    scrape_detik_task_1 >> process_detik_1
    scrape_detik_task_2 >> process_detik_2
    scrape_pikiran_rakyat_task_1 >> process_pikiran_rakyat_1
    scrape_pikiran_rakyat_task_2 >> process_pikiran_rakyat_2
    process_pikiran_rakyat_2 >> combine_all_task
