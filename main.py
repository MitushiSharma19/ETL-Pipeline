from mysql import import_data_to_hdfs
from transform import read_and_transform_parquet
from test_load import create_and_load_hive_table
from pyspark.sql import SparkSession

def main():
    # ETL Parameters
    db_type = "mysql"
    host_ip = "insert your ip address"
    port = "insert your port"
    db_name = "insert db name"
    user_name = "insert username"
    password = "insert password"
    table = "insert table name"
    target_path = "insert target path"
    
    # Step 1: Import data from MySQL to HDFS
    import_data_to_hdfs(db_type, host_ip, port, db_name, user_name, password, table, target_path)
    
    # Step 2: Transform data
    hdfs_path = "insert path where database is saved"
    transformed_df = read_and_transform_parquet(
        hdfs_path,
        drop_duplicates=['id', 'name', 'age', 'salary', 'department'],  
        sort_rows=['age'],            
        remove_null_values=True,      
        remove_outliers=['salary'],  
        keep_columns=['id', 'name', 'age', 'salary', 'department'],  
        remove_columns=[],
    )
    
    # Step 3: Load transformed data into Hive
    create_and_load_hive_table(
        transformed_df,
        hive_username="hive username",
        hive_password="hive password",
        hive_db="hive db name",
        hive_table_name="hive table name"
    )

if __name__ == "__main__":
    main()


