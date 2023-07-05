import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accel. trusted
Acceltrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Acceltrusted_node1",
)

# Script generated for node s3 customer_trusted
s3customer_trusted_node1688468521236 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://mig-buck1/customer/trusted/"]},
    transformation_ctx="s3customer_trusted_node1688468521236",
)

# Script generated for node Join Accel /\ Customer
JoinAccelCustomer_node1688204377603 = Join.apply(
    frame1=s3customer_trusted_node1688468521236,
    frame2=Acceltrusted_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinAccelCustomer_node1688204377603",
)

# Script generated for node Consenting only
Consentingonly_node1688480090650 = Filter.apply(
    frame=JoinAccelCustomer_node1688204377603,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Consentingonly_node1688480090650",
)

# Script generated for node Drop accel
Dropaccel_node1688286238783 = DropFields.apply(
    frame=Consentingonly_node1688480090650,
    paths=["z", "timestamp", "user", "y", "x"],
    transformation_ctx="Dropaccel_node1688286238783",
)

# Script generated for node DeDup on all Customer keys for 200x compression
DeDuponallCustomerkeysfor200xcompression_node1688480972568 = DynamicFrame.fromDF(
    Dropaccel_node1688286238783.toDF().dropDuplicates(
        [
            "serialNumber",
            "shareWithPublicAsOfDate",
            "birthDay",
            "registrationDate",
            "shareWithResearchAsOfDate",
            "customerName",
            "email",
            "lastUpdateDate",
            "phone",
            "shareWithFriendsAsOfDate",
        ]
    ),
    glueContext,
    "DeDuponallCustomerkeysfor200xcompression_node1688480972568",
)

# Script generated for node Customer curated
Customercurated_node3 = glueContext.getSink(
    path="s3://mig-buck1/customer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Customercurated_node3",
)
Customercurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated1"
)
Customercurated_node3.setFormat("json")
Customercurated_node3.writeFrame(
    DeDuponallCustomerkeysfor200xcompression_node1688480972568
)
job.commit()
