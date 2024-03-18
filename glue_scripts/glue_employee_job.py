import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from awsglue.dynamicframe import DynamicFrame

logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)

# create a handler for cloudwatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("My log message")

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node = glueContext.create_dynamic_frame.from_catalog(
    database="sampledb",
    table_name="employee_data",
    transformation_ctx="AmazonS3_node",
)

logger.info("print schema of node Amazon s3 for dynamic frame")
AmazonS3_node.printSchema()

count = AmazonS3_node.count()
print("Number of rows in dynamic frame: ", count)
logger.info("count for frame is {}".format(count))

# Script generated for node Change Schema
ChangeSchema_node = ApplyMapping.apply(
    frame=AmazonS3_node,
    mappings=[
        ("EmpID", "string", "emp_id", "string"),
        ("FirstName", "string", "first_name", "string"),
        ("LastName", "string", "last_name", "string"),
    ],
    transformation_ctx="ChangeSchema_node",
)

# convert dynamic frame to spark dataframe
logger.info("convert dynamic frame to spark dataframe")

spark_dataframe = ChangeSchema_node.toDF()

spark_dataframe.show()

logger.info("convert spark dataframe into a table view so that we can run SQL query on it")
spark_dataframe.createOrReplaceTempView("employee_view")

logger.info("create dataframe by spark sql")
employee_sql_df = spark.sql("SELECT first_name, count(emp_id) AS emp_cnt_first_name FROM employee_view GROUP BY first_name")

logger.info("Display results after aggregation")
employee_sql_df.show()

# convert spark dataframe back to glue dynamic dataframe
logger.info("convert the dataframe back to dynamic dataframe")
dynamic_frame = DynamicFrame.fromDF(employee_sql_df, glueContext, "dynamic_frame")

# Script generated for node Amazon S3
AmazonS3_node1 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://s3-glue-bucket-ar/output/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1",
)

job.commit()
