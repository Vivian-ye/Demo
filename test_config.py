from projects.data_ingestion.data_ingestion_config import DataIngestionConfig

test_config = DataIngestionConfig(yaml_file= "projects/data_ingestion/config/ac_shopping_crm.yml")

# table_config_attr = test_config.get_table_attr()
# print(table_config_attr)
table_config = test_config.get_pipeline_config()

print(table_config.staging_schema_name)

# for list in table_config:
#     print(list.update_method)