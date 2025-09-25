import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1758775556899 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="cutomer_trusted", transformation_ctx="customer_trusted_node1758775556899")

# Script generated for node acceleometer_landing
acceleometer_landing_node1758775529316 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="acceleometer_landing", transformation_ctx="acceleometer_landing_node1758775529316")

# Script generated for node Join
Join_node1758775589882 = Join.apply(frame1=customer_trusted_node1758775556899, frame2=acceleometer_landing_node1758775529316, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1758775589882")

# Script generated for node SQL Query
SqlQuery3499 = '''
select user,x,y,z,timestamp from myDataSource

'''
SQLQuery_node1758775634347 = sparkSqlQuery(glueContext, query = SqlQuery3499, mapping = {"myDataSource":Join_node1758775589882}, transformation_ctx = "SQLQuery_node1758775634347")

# Script generated for node Drop Duplicates
DropDuplicates_node1758775737362 =  DynamicFrame.fromDF(SQLQuery_node1758775634347.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1758775737362")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1758775737362, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774401559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758775772026 = glueContext.getSink(path="s3://stedi-datalakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758775772026")
AmazonS3_node1758775772026.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="acceleometer_trusted")
AmazonS3_node1758775772026.setFormat("json")
AmazonS3_node1758775772026.writeFrame(DropDuplicates_node1758775737362)
job.commit()