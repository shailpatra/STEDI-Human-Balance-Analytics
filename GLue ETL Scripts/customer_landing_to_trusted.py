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

# Script generated for node Customer_landing
Customer_landing_node1758785729646 = glueContext.create_dynamic_frame.from_catalog(database="stedi_datalakehouse", table_name="customer_landing", transformation_ctx="Customer_landing_node1758785729646")

# Script generated for node SQL Query
SqlQuery3752 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SQLQuery_node1758786706862 = sparkSqlQuery(glueContext, query = SqlQuery3752, mapping = {"myDataSource":Customer_landing_node1758785729646}, transformation_ctx = "SQLQuery_node1758786706862")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758786706862, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758785540957", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758786782686 = glueContext.getSink(path="s3://stedi-datalakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758786782686")
AmazonS3_node1758786782686.setCatalogInfo(catalogDatabase="stedi_datalakehouse",catalogTableName="customer_trusted")
AmazonS3_node1758786782686.setFormat("json")
AmazonS3_node1758786782686.writeFrame(SQLQuery_node1758786706862)
job.commit()