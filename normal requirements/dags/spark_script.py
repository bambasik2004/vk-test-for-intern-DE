import os
import datetime
import shutil

from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col, sum, round


def recommended():
    spark = SparkSession.builder.appName("VK, take me to work pls.").getOrCreate()

    date_range = (datetime.datetime.now() - datetime.timedelta(days=delta) for delta in range(1, 8))
    csv_files = [os.path.join('input', f'{date.strftime("%Y-%m-%d")}.csv') for date in date_range]
    df = spark.read.csv(csv_files)

    res = df.groupBy('_c0').agg(
        count(when(col('_c1') == 'CREATE', True)).alias('create_count'),
        count(when(col('_c1') == 'READ', True)).alias('read_count'),
        count(when(col('_c1') == 'UPDATE', True)).alias('update_count'),
        count(when(col('_c1') == 'DELETE', True)).alias('delete_count')
    )
    res = res.withColumnRenamed('_c0', 'email')
    
    output_path = os.path.join('output', f'{datetime.datetime.now().strftime("%Y-%m-%d")}.csv')
    if os.path.exists(output_path):
        os.remove(output_path)
    res.toPandas().to_csv(output_path, index=False)


def advanced():
    spark = SparkSession.builder.appName("-_-").getOrCreate()

    date_range = [(datetime.datetime.now() - datetime.timedelta(days=delta)).strftime("%Y-%m-%d") for delta in range(1, 8)]

    if not os.path.exists(os.path.join('output', 'temp')):
        os.makedirs(os.path.join('output', 'temp'))
    
    csv_files = [
        os.path.join('input', f'{date}.csv')
        for date in date_range
        if not os.path.exists(os.path.join('output', 'temp', f'{date}'))
    ]

    # Intermediate calculations
    for file in csv_files:
        df = spark.read.csv(file)
        res = df.groupBy('_c0').agg(
            count(when(col('_c1') == 'CREATE', True)).alias('create_count'),
            count(when(col('_c1') == 'READ', True)).alias('read_count'),
            count(when(col('_c1') == 'UPDATE', True)).alias('update_count'),
            count(when(col('_c1') == 'DELETE', True)).alias('delete_count')
        )
        res = res.withColumnRenamed('_c0', 'email')

        output_path = os.path.join('output', 'temp', file.split('/')[-1].replace('.csv', ''))
        res.write.csv(output_path, header=True)
    
    output_csv_files = [
        os.path.join('output', 'temp', date.strftime("%Y-%m-%d"), filename)
        for date in date_range
        for filename in os.listdir(os.path.join('output', 'temp', date.strftime("%Y-%m-%d")))
        if filename.endswith(".csv")
    ]

    # Resulting calculations
    df = spark.read.csv(output_csv_files, header=True)
    res = df.groupBy("email").agg(
        sum("create_count").cast('int').alias("create_count"),
        sum("read_count").cast('int').alias("read_count"),
        sum("update_count").cast('int').alias("update_count"),
        sum("delete_count").cast('int').alias("delete_count")
    )    

    temp_output_path = os.path.join('output', '~')
    res.coalesce(1).write.csv(temp_output_path)

    final_output_file = os.path.join('output', f'{datetime.datetime.now().strftime("%Y-%m-%d")}.csv')
    if os.path.exists(final_output_file):
        os.remove(final_output_file)

    for filename in os.listdir(temp_output_path):
        if filename.endswith(".csv"):
            shutil.copy(os.path.join(temp_output_path, filename), final_output_file)
            break
    shutil.rmtree(temp_output_path)


with DAG(
    dag_id='recommended',
    start_date=datetime.datetime(2024, 9, 24),
    schedule_interval='0 7 * * *',
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='pls',
        python_callable=recommended,
    )


with DAG(
    dag_id='advanced',
    start_date=datetime.datetime(2024, 9, 24),
    schedule_interval='0 7 * * *',
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='pls_pls',
        python_callable=advanced,
    )