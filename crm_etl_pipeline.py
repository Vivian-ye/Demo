from utils.ssm import * 
from utils.s3_connector import S3Connector
from utils.postgressql_connector import DbConnector
import yaml


class DataIngestion():
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        etl_config = self.read_yaml()
        self.rds_secret_name = etl_config["rds_secret_name"]
        self.redshift_secret_name = etl_config["redshift_secret_name"]
        self.rds_schema_name = etl_config["rds_schema_name"]
        self.staging_schema_name = etl_config["staging_schema_name"]
        self.table_name = etl_config["table_name"]
        self.destination_table_name = etl_config["destination_table_name"]
        self.bucket = etl_config["bucket"]
        self.object_name = etl_config["object_name"]
        self.primary_key_column = etl_config["primary_key_column"]
        self.destination_schema_name = etl_config["destination_schema_name"]
        self.parameter_name = etl_config["parameter_name"]

    def read_yaml(self):
        with open(self.config_file_path) as config_file:
            return yaml.full_load(config_file)

    def write_to_csv(self):
        source_db_connection = DbConnector(self.rds_secret_name)
        last_parameter_value = get_parameter_value(self.parameter_name)
        sql = f"""select * from {self.rds_schema_name}.{self.table_name}
        where updated_at >= '{last_parameter_value}'"""
        source_db_connection.write_to_csv(sql, self.table_name)

    def upload_to_s3(self):
        s3_conn = S3Connector(self.bucket)
        s3_conn.upload_to_s3(self.table_name, self.object_name)

    def s3_to_stagging(self):
        copy_cmd = f"""
        truncate {self.staging_schema_name}.{self.table_name};
        copy {self.staging_schema_name}.{self.table_name}
        from 's3://{self.bucket}/{self.object_name}/{self.table_name}.csv'
        credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
        format csv
        delimiter ','
        ignoreheader 1"""
        destination_db_connection = DbConnector(self.redshift_secret_name)
        redshift_connection = destination_db_connection.connect_to_db()
        cursor = redshift_connection.cursor()
        cursor.execute(copy_cmd)
        cursor.execute("COMMIT")

    def staging_to_redshift(self):
        upsert_sql_cmd = f"""
        delete from {self.destination_schema_name}.{self.destination_table_name}
        where {self.primary_key_column} in (select {self.primary_key_column} from {self.staging_schema_name}.{self.table_name});
        insert into {self.destination_schema_name}.{self.destination_table_name}
        select * from {self.staging_schema_name}.{self.table_name}
        """
        destination_db_connection = DbConnector(self.redshift_secret_name)
        redshift_connection = destination_db_connection.connect_to_db()
        cursor = redshift_connection.cursor()
        cursor.execute(upsert_sql_cmd)
        cursor.execute("COMMIT")



# result = read_yaml("config/ac_shopping_crm.yml")
# print(result)

crm_ingestion_class = DataIngestion(config_file_path = "projects/data_ingestion/config/ac_shopping_crm.yml")

crm_ingestion_class.write_to_csv()
# crm_ingestion_class.upload_to_s3()
# crm_ingestion_class.s3_to_stagging()
# crm_ingestion_class.staging_to_redshift()
# upload_to_s3(bucket, table_name, object_name)
# s3_to_stagging(staging_schema_name, table_name, bucket, object_name, redshift_secret_name)
# staging_to_redshift(staging_schema_name,destination_table_name,table_name,destination_schema_name,primary_key_column,redshift_secret_name)
# # update_parameter_value(parameter_name, redshift_secret_name, destination_schema_name, destination_table_name)



