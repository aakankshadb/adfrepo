# Databricks notebook source
#creating the mount point for the storage account strgrestapi
def creating_mount_pt(src,mntpt,config):
    dbutils.fs.mount(
    source=src,
    mount_point=mntpt,
    extra_configs=config
                )
    

# COMMAND ----------

src="wasbs://containerrestapi@strgrestapi.blob.core.windows.net"
mntpt="/mnt/mountingstrgrestapiaakanksha"
config={"fs.azure.account.key.strgrestapi.blob.core.windows.net":
        "AVAiykR2XouS+8g/asG2tWPXc10nmf2hSIi/WMLSXXPQieLjSQ+bOP0NJ5J0+O3dE/hxhn+EP0Ia+AStny4gZQ=="}
creating_mount_pt(src,mntpt,config)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,ArrayType,StringType, IntegerType
from pyspark.sql.functions import explode
schema=StructType([
                StructField("data",ArrayType(StructType([
                    StructField("id",IntegerType()),
                    StructField("employee_name", StringType()),
                    StructField("employee_salary", IntegerType()),
                    StructField("employee_age", IntegerType()),
                    StructField("profile_image", StringType())
                    ]))),
                StructField("message",StringType()),
                StructField("status",StringType())
                ])

# COMMAND ----------

def json_read(schma,multiline,path):
    df=spark.read.format('json').schema(schma).option(multiline,multiline).load(path)
    return df
   

# COMMAND ----------

multiline="true"
json_df=json_read(schema,multiline,'/mnt/mountingstrgrestapi/Bronze/')
json_df.printSchema()
display(json_df)

# COMMAND ----------

#flattening the data from nested json
def flatten_json(json_df):
    exploded_df=json_df.withColumn("exploded_data",explode("data"))
    df=exploded_df.select("exploded_data.id",
                              "exploded_data.employee_name",
                              "exploded_data.employee_age",
                              "exploded_data.employee_salary",
                              "exploded_data.profile_image",
                              "message",
                              "status"
                            )
    return df


# COMMAND ----------

flatten_df=flatten_json(json_df)
display(flatten_df)
flatten_df.printSchema()

# COMMAND ----------

from delta import *

def save_to_delta_with_overwrite(resultDf, path, db_name, table_name, mergeCol):

    base_path = path + f"/{db_name}/{table_name}"

    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):    

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        resultDf.write\
                .mode("overwrite") \
                .format("delta") \
                .option("path", base_path) \
                .saveAsTable(f"{db_name}.{table_name}")

    else:

        deltaTable = DeltaTable.forPath(spark, f"{base_path}")

        matchKeys = " AND ".join("old." + col + " = new." + col for col in mergeCol)

        deltaTable.alias("old") \
                  .merge(resultDf.alias("new"), matchKeys) \
                  .whenMatchedUpdateAll() \
                  .whenNotMatchedInsertAll() \
                  .execute()
save_to_delta_with_overwrite(flatten_df, '/mnt/mountingstrgrestapi/Silver','emp_database', 'employee_table',['id'] )

# COMMAND ----------

silver_df=spark.read.format('delta').load("/mnt/mountingstrgrestapi/Silver/emp_database/employee_table/")
display(silver_df)

# COMMAND ----------

table1_df=silver_df.select("id",
                           "employee_name",
                           "profile_image"
                           )
table2_df=silver_df.select("id",
                           "employee_age",
                           "employee_salary",
                           "message",
                           "status"
                           )

# COMMAND ----------

display(table1_df)
display(table2_df)

# COMMAND ----------

def innerjoin_df(df1,df2,colname):
    df = table1_df.join(table2_df,colname,'inner')
    return df

# COMMAND ----------

colname="id"
join_df=innerjoin_df(table1_df,table2_df,colname)
display(join_df)

# COMMAND ----------

def write(df,frmt,mode,path):
    df.write\
      .format(frmt)\
      .mode(mode)\
      .save(path)
    

# COMMAND ----------

frmt='parquet'
mode='append'
path="/mnt/mountingstrgrestapiaakanksha/Gold"
write(join_df,frmt,mode,path)

# COMMAND ----------

