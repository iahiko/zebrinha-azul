# if __name__ == 'main': 
#     traffic_data = ClimateData()
#     df_trafego = traffic_data.pipeline()
# #ROdar a bronze

# #rodar a silver

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from features.feat_bronze_clima import ClimateData


def e_valida(ti):
    qtd = ti.xcom_pull(task_ids = 'ClimateData')
    if (qtd > 5):
        return 'valida'
    return 'nvalida'

with DAG('pipeline.dag', start_date = datetime(2024,5,25), schedule_interval=timedelta(minutes=5), catchup= False) as dag:

    ClimateData = PythonOperator(
            task_id = 'ClimateData',
            python_callable = ClimateData
        )
    
    e_valida = BranchPythonOperator(
        task_id = 'e_valida',
        python_callable= e_valida
    )
    
    valido = BashOperator(
        task_id = 'valido',
        bash_command= "echo 'Quantidade Ok'"
    )

    nvalido = BashOperator(
        task_id = 'nvalido',
        bash_command= "echo 'Quantidade Ok'"
    )


ClimateData >> e_valida >> [valido,nvalido]
        
        

