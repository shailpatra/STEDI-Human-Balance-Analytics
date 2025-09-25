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

# Script generated for node accelerometer_landing
accelerometer_landing_node1758787658543 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalakehouse/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1758787658543")

# Script generated for node customer_trusted
customer_trusted_node1758787709010 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalakehouse/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1758787709010")

# Script generated for node Join
Join_node1758787740171 = Join.apply(frame1=customer_trusted_node1758787709010, frame2=accelerometer_landing_node1758787658543, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1758787740171")

# Script generated for node SQL Query
SqlQuery3745 = '''
select user,x,y,z,timestamp from myDataSource
'''
SQLQuery_node1758788063307 = sparkSqlQuery(glueContext, query = SqlQuery3745, mapping = {"myDataSource":Join_node1758787740171}, transformation_ctx = "SQLQuery_node1758788063307")

# Script generated for node Drop Duplicates
DropDuplicates_node1758788172609 =  DynamicFrame.fromDF(SQLQuery_node1758788063307.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1758788172609")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1758788172609, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758786225613", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758788178441 = glueContext.getSink(path="s3://stedi-datalakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758788178441")
AmazonS3_node1758788178441.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="accelerometer_trusted")
AmazonS3_node1758788178441.setFormat("json")
AmazonS3_node1758788178441.writeFrame(DropDuplicates_node1758788172609)
job.commit()