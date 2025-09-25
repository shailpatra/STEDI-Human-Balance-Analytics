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

# Script generated for node step_trainer_landing
step_trainer_landing_node1758776641311 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1758776641311")

# Script generated for node customer_curated
customer_curated_node1758776614800 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="customer_curated", transformation_ctx="customer_curated_node1758776614800")

# Script generated for node SQL Query
SqlQuery3610 = '''
select l.* from c join l on c.serialnumber = l.serialnumber
'''
SQLQuery_node1758776671103 = sparkSqlQuery(glueContext, query = SqlQuery3610, mapping = {"l":step_trainer_landing_node1758776641311, "c":customer_curated_node1758776614800}, transformation_ctx = "SQLQuery_node1758776671103")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776671103, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774401559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758776759586 = glueContext.getSink(path="s3://stedi-datalakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758776759586")
AmazonS3_node1758776759586.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="step_trainer_trusted")
AmazonS3_node1758776759586.setFormat("json")
AmazonS3_node1758776759586.writeFrame(SQLQuery_node1758776671103)
job.commit()