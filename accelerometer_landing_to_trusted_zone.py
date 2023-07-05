import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node table customer_trusted
tablecustomer_trusted_node1688204699700 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="tablecustomer_trusted_node1688204699700",
)

# Script generated for node s3 accel raw
s3accelraw_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mig-buck1/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="s3accelraw_node1",
)

# Script generated for node Join Accel /\ Customer
s3accelraw_node1DF = s3accelraw_node1.toDF()
tablecustomer_trusted_node1688204699700DF = (
    tablecustomer_trusted_node1688204699700.toDF()
)
JoinAccelCustomer_node1688204377603 = DynamicFrame.fromDF(
    s3accelraw_node1DF.join(
        tablecustomer_trusted_node1688204699700DF,
        (
            s3accelraw_node1DF["user"]
            == tablecustomer_trusted_node1688204699700DF["email"]
        ),
        "right",
    ),
    glueContext,
    "JoinAccelCustomer_node1688204377603",
)

# Script generated for node Consented already?
SqlQuery0 = """
select * from myDataSource
where myDataSource.timeStamp > myDataSource.sharewithresearchasofdate
"""
Consentedalready_node1688369407940 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": JoinAccelCustomer_node1688204377603},
    transformation_ctx="Consentedalready_node1688369407940",
)

# Script generated for node Accel. Fields-only
AccelFieldsonly_node1688286238783 = DropFields.apply(
    frame=Consentedalready_node1688369407940,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="AccelFieldsonly_node1688286238783",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://mig-buck1/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted1"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(AccelFieldsonly_node1688286238783)
job.commit()
