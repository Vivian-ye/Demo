import yaml
import time
import os
from utils.postgressql_connector import DbConnector
from utils.ssm import (
    get_parameter_value,
    update_parameter_value,
)

class DataIngestionConfig:
    """
    Class for dealing with data ingestion config. Reads the given YAML file and
    resolves it to a PipelineConfig class and TableConfig class
    :param yaml_file: The YAML filename and path (without .yml) to process in
        data_ingestion/config/
    :type yaml_file: str
    :param table_id: (Optional) The specific table name to process.
        If not set, all tables will be processed
    :type table_id: str
    :param config_group: (Optional) Currently not in use. If needed, we can use
        it to specify a config_group to process.
        If not set, all config_groups will be processed
    :type config_group: str
    """

    def __init__(
        self, yaml_file, table_id=None, config_group=None,
    ):
        self.yaml_file = yaml_file
        self.table_id = table_id
        self.config_group = config_group
        self.yaml_string = self._get_yaml()

    def _get_yaml(self):
        with open(self.yaml_file) as config_file:
            return yaml.full_load(config_file)

    def get_yaml_attr(self, attribute_name):
        return self.yaml_string.get(attribute_name)

    def get_pipeline_config(self):
        return PipelineConfig(self)

    def get_table_config(self):
        pipe_config = self.get_pipeline_config()
        table_config_list = []
        if self.table_id is None:
            for table in self.yaml_string.get("tables"):
                for table_name, table_config_attr in table.items():
                    table_config_list.append(
                        TableConfig(table_name, pipe_config, table_config_attr)
                    )
            return table_config_list
        else:
            for table in self.yaml_string.get("tables"):
                for table_name, table_config_attr in table.items():
                    if table_name == self.table_id:
                        table_config_list.append(
                            TableConfig(table_name, pipe_config, table_config_attr)
                        )
            return table_config_list

    def get_table_attr(self):
            for table in self.yaml_string.get("tables"):
                for table_name, table_config_attr in table.items():
                    print(table_name)
                    print(table_config_attr)

class PipelineConfig:
    def __init__(self, config):
        # resolve pipeline level config
        self.dag_name = config.get_yaml_attr("dag_name")
        self.bucket = config.get_yaml_attr("bucket")
        self.destination_platform = (
            config.get_yaml_attr("destination_platform") or "Redshift"
        )
        self.destination_schema_name = config.get_yaml_attr("destination_schema_name")
        self.redshift_secret_name = (
            config.get_yaml_attr("redshift_secret_name") or "redshift_ac_master"
        )
        self.source_platform = config.get_yaml_attr("source_platform") or "postgres"
        self.rds_secret_name = config.get_yaml_attr("rds_secret_name")
        self.rds_schema_name = config.get_yaml_attr("rds_schema_name")
        self.staging_schema = config.get_yaml_attr("staging_schema_name")
        self.source_conn = self._get_connection()

    def _get_connection(self):

        return DbConnector(self.rds_secret_name)


class TableConfig:
    def __init__(self, table, pipe_config, table_attr):
        self._resolve_parameters(table, pipe_config, table_attr)

    def _resolve_parameters(self, table, pipe_config, table_attr):
        # resolve table level config
        print(f"Resolve table level config")
        self.load_columns = None
        self.source_table = table
        self.update_method = table_attr.get("update_method")
        self.destination_table = table_attr.get("destination_table_name") or table
        # self.staging_schema = table_attr.get("staging_schema") or "staging"
        self.staging_table = (
            table_attr.get("staging_table") or f"{self.destination_table_name}"
        )
        self.filename = f"{table}.csv.gz"
        self.parameter_name = f"{pipe_config.dag_name}_{table}"  ##parameter store
        # create_parameter_value(self.parameter_name)

        self.incremental_load_parameter = get_parameter_value(self.parameter_name)

        # self.s3_object = table_attr.get("s3_object")

        self.specify_copy_columns = (
            True
            # ifexplicitly specifying column inclusions
            if table_attr.get("column_inclusions")
            else False
        )

        # set DB specific attributes

        # generate column inclusions from source metadata if not provided in config
        self.column_inclusions = table_attr.get("column_inclusions")

        # self.update_keys = table_attr.get("update_keys") or self._resolve_update_keys(
        #     pipe_config
        # )

        self.incremental_load_columns = table_attr.get("incremental_load_columns")

        # self.distkey = table_attr.get("distkey")

        # self.sort_key = table_attr.get("sortkey")

        self.export_file_name = table_attr.get("export_file_name")

    # def _resolve_sort_key(self, sort_key):
    #     # if sort key not provided use first load_column or update key
    #     if sort_key:
    #         return sort_key
    #     elif self.incremental_load_columns:
    #         return self.incremental_load_columns[1]
    #     elif self.update_keys:
    #         return self.update_keys[0]
    #     else:
    #         return None

    # def _resolve_column_inclusions(self, pipe_config):
    #     # get columns from source system
    #     df = pipe_config.source_conn.get_column_metadata(
    #         table_name=self.source_table, schema_name=pipe_config.rds_schema_name
    #     )

    #     return df["column_name"].tolist()

    # def _resolve_update_keys(self, pipe_config):
    #     # get primary key(s) from source system
    #     return pipe_config.source_conn.get_table_primary_keys(
    #         table_name=self.source_table, schema_name=pipe_config.rds_schema_name
    #     )