import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 customer_landing
S3customer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://mig-buck1/customer/landing/"]},
    transformation_ctx="S3customer_landing_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1688563417150 = DynamicFrame.fromDF(
    S3customer_landing_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1688563417150",
)

# Script generated for node Consenting only
Consentingonly_node1687965478890 = Filter.apply(
    frame=DropDuplicates_node1688563417150,
    f=lambda row: (row["shareWithResearchAsOfDate"] > 55555555),
    transformation_ctx="Consentingonly_node1687965478890",
)

# Script generated for node customer_trusted
customer_trusted_node1687965595470 = glueContext.write_dynamic_frame.from_options(
    frame=Consentingonly_node1687965478890,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://mig-buck1/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_node1687965595470",
)

job.commit()
