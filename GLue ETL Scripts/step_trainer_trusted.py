import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node customer_curated
customer_curated_node1758790311499 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalakehouse/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1758790311499")

# Script generated for node step_trainer_landing
step_trainer_landing_node1758790399933 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1758790399933")

# Script generated for node SQL Query
SqlQuery3485 = '''
select l.* from c join l on c.serialnumber = l.serialnumber

'''
SQLQuery_node1758790503171 = sparkSqlQuery(glueContext, query = SqlQuery3485, mapping = {"c":customer_curated_node1758790311499, "l":step_trainer_landing_node1758790399933}, transformation_ctx = "SQLQuery_node1758790503171")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758790503171, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758790298161", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758790902002 = glueContext.getSink(path="s3://stedi-datalakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758790902002")
AmazonS3_node1758790902002.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="step_trainer_trusted")
AmazonS3_node1758790902002.setFormat("json")
AmazonS3_node1758790902002.writeFrame(SQLQuery_node1758790503171)
job.commit()