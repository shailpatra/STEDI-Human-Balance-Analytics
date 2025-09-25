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

# Script generated for node customer_trusted
customer_trusted_node1758776101057 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="cutomer_trusted", transformation_ctx="customer_trusted_node1758776101057")

# Script generated for node acceleometer_landing
acceleometer_landing_node1758776066833 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="acceleometer_landing", transformation_ctx="acceleometer_landing_node1758776066833")

# Script generated for node SQL Query
SqlQuery3967 = '''
select c.* from c join (select distinct user from myDataSource) as 
a  on c.email = a.user
'''
SQLQuery_node1758776135790 = sparkSqlQuery(glueContext, query = SqlQuery3967, mapping = {"c":customer_trusted_node1758776101057, "myDataSource":acceleometer_landing_node1758776066833}, transformation_ctx = "SQLQuery_node1758776135790")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776135790, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774401559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758776287040 = glueContext.getSink(path="s3://stedi-datalakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758776287040")
AmazonS3_node1758776287040.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="customer_curated")
AmazonS3_node1758776287040.setFormat("json")
AmazonS3_node1758776287040.writeFrame(SQLQuery_node1758776135790)
job.commit()