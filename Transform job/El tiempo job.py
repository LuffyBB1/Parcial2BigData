import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Table Jobs
TableJobs_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="periodicos",
    table_name="periodico_eltiempo",
    transformation_ctx="TableJobs_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = SelectFields.apply(
    frame=TableJobs_node1,
    paths=["col0", "col1", "col2", "year", "month"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node SQL Table
SQLTable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="salida-db",
    table_name="db-eltiempo",
    transformation_ctx="SQLTable_node3",
)

job.commit()
