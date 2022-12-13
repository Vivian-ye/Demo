import boto3
import json
import psycopg2
import csv

def get_aws_secret(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId= secret_name)["SecretString"]
    return json.loads(response)


def connect_to_rds(rds_secret_name):
    connection_credential = get_aws_secret(rds_secret_name)
    engine = psycopg2.connect(
    database= connection_credential["database"],
    user= connection_credential["username"],
    password= connection_credential["password"],
    host= connection_credential["host"],
    port= connection_credential["port"]
    )
    return engine

def connect_to_redshift(redshift_secret_name):
    connection_credential = get_aws_secret(redshift_secret_name)
    redshift_engine = psycopg2.connect(
    database= connection_credential["database"],
    user= connection_credential["username"],
    password= connection_credential["password"],
    host= connection_credential["host"],
    port= connection_credential["port"]
    )
    return redshift_engine

def write_to_csv(rds_secret_name, rds_schema_name, table_name,parameter_name):
    ssm_client = boto3.client("ssm")
    parameter_value = ssm_client.get_parameter(Name = f'{parameter_name}')
    source_db_connection = connect_to_rds(rds_secret_name)
    cursor = source_db_connection.cursor()
    cursor.execute(f"""select * from {rds_schema_name}.{table_name}
    where updated_at >= '{parameter_value['Parameter']['Value']}'""")
    with open(f'{table_name}.csv', 'w', newline='') as f:
        csv_writer = csv.writer(f, quoting=csv.QUOTE_ALL, delimiter=",")
        csv_writer.writerow(col[0] for col in cursor.description)
        csv_writer.writerows(cursor.fetchall())


def upload_to_s3(table_name, bucket, object_name):
    s3_client = boto3.client('s3')
    s3_upload = s3_client.upload_file(f"{table_name}.csv", bucket, f"{object_name}/{table_name}.csv")
    
def s3_to_stagging(staging_schema_name, table_name, bucket, object_name, redshift_secret_name):
    copy_cmd = f"""
    truncate {staging_schema_name}.{table_name};
    copy {staging_schema_name}.{table_name}
    from 's3://{bucket}/{object_name}/{table_name}.csv'
    credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
    format csv
    delimiter ','
    ignoreheader 1"""
    redshift_connection = connect_to_redshift(redshift_secret_name)
    cursor = redshift_connection.cursor()
    cursor.execute(copy_cmd)
    cursor.execute("COMMIT")

def staging_to_redshift(staging_schema_name,destination_table_name,table_name,destination_schema_name,primary_key_column,redshift_secret_name):
    upsert_sql_cmd = f"""
    delete from {destination_schema_name}.{destination_table_name}
    where {primary_key_column} in (select {primary_key_column} from {staging_schema_name}.{table_name});
    insert into {destination_schema_name}.{destination_table_name}
    select * from {staging_schema_name}.{table_name}
    """
    redshift_connection = connect_to_redshift(redshift_secret_name)
    cursor = redshift_connection.cursor()
    cursor.execute(upsert_sql_cmd)
    cursor.execute("COMMIT")

def get_parameter_value():
    ssm_client = boto3.client("ssm")
    parameter_value = ssm_client.get_parameter(
        Name='de_daily_load_ac_shopping_crm_customer')
    # print(parameter_value['Parameter']['Value'])

def update_parameter_value(parameter_name, redshift_secret_name, destination_schema_name, destination_table_name):
    ssm_client = boto3.client("ssm")
    destination_db_connection = connect_to_redshift(redshift_secret_name)
    destination_db_cursor = destination_db_connection.cursor()
    destination_db_cursor.execute(f"""select max(updated_at) from {destination_schema_name}.{destination_table_name}""")
    last_parameter_time = destination_db_cursor.fetchall()
    # print(last_parameter_time[0][0])
    ssm_client.put_parameter(Name = f'{parameter_name}',Value = f'{last_parameter_time[0][0]}',Overwrite = True)


def main():
    rds_secret_name = "postgres_ac_master"
    redshift_secret_name = "redshift_ac_master"
    rds_schema_name = "ac_shopping"
    staging_schema_name = "staging_vivian"
    table_name = "customer"
    destination_table_name = "customer_vivian"
    bucket = "ac-shopping-datalake"
    object_name = "ac_shopping_crm_vivian"
    primary_key_column ="customer_id"
    destination_schema_name ="ac_shopping_crm"
    parameter_name = 'de_daily_load_ac_shopping_crm_customer'
    write_to_csv(rds_secret_name, rds_schema_name, table_name,parameter_name)
    upload_to_s3(table_name, bucket, object_name)
    s3_to_stagging(staging_schema_name, table_name, bucket, object_name, redshift_secret_name)
    staging_to_redshift(staging_schema_name,destination_table_name,table_name,destination_schema_name,primary_key_column,redshift_secret_name)
    update_parameter_value(parameter_name, redshift_secret_name, destination_schema_name, destination_table_name)

main()

