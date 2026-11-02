import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="stefany_data_warehouse",
    description="Orquestacion de S3, Redshift y dbt en EC2",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="America/Bogota"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["stefany", "data-warehouse", "aws"],
) as dag:

    verificar_s3 = BashOperator(
        task_id="verificar_archivos_s3",
        bash_command=(
            "aws s3api head-object "
            "--bucket stefany-redshift-taller-2026 "
            "--key data-warehouse/redshift-input/"
            "batch_id=20261102T013140Z-c5e61618/"
            "mysql_customers.csv >/dev/null"
        ),
    )

    verificar_redshift = BashOperator(
        task_id="verificar_conexion_redshift",
        bash_command=(
            "cd /home/ec2-user/dw-proyecto/dbt && "
            "python3 /home/ec2-user/dw-proyecto/"
            "run_dbt_ec2.py debug"
        ),
    )

    ejecutar_dbt = BashOperator(
        task_id="ejecutar_modelos_y_pruebas",
        bash_command=(
            "cd /home/ec2-user/dw-proyecto/dbt && "
            "python3 /home/ec2-user/dw-proyecto/"
            "run_dbt_ec2.py build"
        ),
    )

    verificar_s3 >> verificar_redshift >> ejecutar_dbt
