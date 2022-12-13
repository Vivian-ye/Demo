import boto3
from utils.postgressql_connector import DbConnector

def get_parameter_value(parameter_name):
    ssm_client = boto3.client("ssm")
    parameter = ssm_client.get_parameter(Name = f'{parameter_name}')
    parameter_value = parameter['Parameter']['Value']
    return parameter_value
    # print(parameter_value['Parameter']['Value'])

def update_parameter_value(parameter_name, redshift_secret_name, destination_schema_name, destination_table_name):
    ssm_client = boto3.client("ssm")
    destination_db_connection = DbConnector(redshift_secret_name)
    redshift_db_connection = destination_db_connection.connect_to_redshift()
    destination_db_cursor = redshift_db_connection.cursor()
    destination_db_cursor.execute(f"""select max(updated_at) from {destination_schema_name}.{destination_table_name}""")
    last_parameter_time = destination_db_cursor.fetchall()
    # print(last_parameter_time[0][0])
    ssm_client.put_parameter(Name = f'{parameter_name}',Value = f'{last_parameter_time[0][0]}',Overwrite = True)

