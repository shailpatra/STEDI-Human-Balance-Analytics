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

# Script generated for node acceleometer_trusted
acceleometer_trusted_node1758777023573 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="acceleometer_trusted", transformation_ctx="acceleometer_trusted_node1758777023573")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1758777059324 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1758777059324")

# Script generated for node SQL Query
SqlQuery3511 = '''
select serialnumber,x,y,z,a.timestamp from s join a on s.sensorreadingtime = a.timestamp

'''
SQLQuery_node1758777095462 = sparkSqlQuery(glueContext, query = SqlQuery3511, mapping = {"s":step_trainer_trusted_node1758777059324, "a":acceleometer_trusted_node1758777023573}, transformation_ctx = "SQLQuery_node1758777095462")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758777095462, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774401559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758777209366 = glueContext.getSink(path="s3://stedi-datalakehouse/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758777209366")
AmazonS3_node1758777209366.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="machine_learning_curated")
AmazonS3_node1758777209366.setFormat("glueparquet", compression="snappy")
AmazonS3_node1758777209366.writeFrame(SQLQuery_node1758777095462)
job.commit()