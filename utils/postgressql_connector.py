import psycopg2
import csv
from utils.aws_secret import get_aws_secret
from utils.s3_connector import S3Connector

class DbConnector():
    def __init__(self,secret_name):
        self.secret_name = secret_name
        self.credential = get_aws_secret(self.secret_name)

    def connect_to_db(self):
        engine = psycopg2.connect(
        database= self.credential.get("database"),
        user= self.credential.get("username"),
        password= self.credential.get("password"),
        host= self.credential.get("host"),
        port= self.credential.get("port")
        )
        return engine
    
    def execute_sql(self, sql, params=None, connection=None):
        if connection is None:
            connection = self.connect_to_db()
            connection.autocommit = True
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            ##cursor.execute("COMMIT")
            print("Successfully executed {}".format(sql))
        except Exception as e:
            raise e
    
    def get_column_metadata(self, table_name, schema_name, column_list=[]):
        column_filter = ""
        if column_list:
            col_string = "'{0}'".format("', '".join(column_list))
            column_filter = f"and column_name in ({col_string.strip()}) "

        sql = f"""
            select
                c.column_name,
                c.data_type,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                null as column_type
            from information_schema.tables t
                left join information_schema.columns c
                    on t.table_schema = c.table_schema
                    and t.table_name = c.table_name
            where t.table_name = '{table_name}'
            and t.table_schema = '{schema_name}'
            {column_filter}
            order by c.ordinal_position;
        """

        df = self.query_sql(query=sql)

        return df
        
    def write_to_csv(self, sql, table_name):
        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute(sql)
        with open(f'{table_name}.csv', 'w', newline='') as f:
            csv_writer = csv.writer(f, quoting=csv.QUOTE_ALL, delimiter=",")
            csv_writer.writerow(col[0] for col in cursor.description)
            csv_writer.writerows(cursor.fetchall())
    
    def upload_to_s3(self, table_name, object_name):
        s3_conn = S3Connector()
        s3_conn.upload_to_s3(table_name, object_name)

    # def connect_to_redshift(self):
    #     redshift_engine = psycopg2.connect(
    #     database= self.credential.get("database"),
    #     user= self.credential.get("username"),
    #     password= self.credential.get("password"),
    #     host= self.credential.get("host"),
    #     port= self.credential.get("port")
    #     )
    #     return redshift_engine
