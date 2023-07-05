import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node S3 customer_cured
S3customer_cured_node1688400820475 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated2",
    transformation_ctx="S3customer_cured_node1688400820475",
)

# Script generated for node step_trainer raw
step_trainerraw_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://mig-buck1/step_trainer/landing/"]},
    transformation_ctx="step_trainerraw_node1",
)

# Script generated for node Customer_cured /\  Step_trainer
Customer_curedStep_trainer_node1688204377603 = Join.apply(
    frame1=step_trainerraw_node1,
    frame2=S3customer_cured_node1688400820475,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Customer_curedStep_trainer_node1688204377603",
)

# Script generated for node Filter out prior2consent
SqlQuery0 = """
select * from myDataSource
where myDataSource.sensorReadingTime > myDataSource.sensorReadingTime
"""
Filteroutprior2consent_node1688552940241 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Customer_curedStep_trainer_node1688204377603},
    transformation_ctx="Filteroutprior2consent_node1688552940241",
)

# Script generated for node Drop Fields Customer
DropFieldsCustomer_node1688286238783 = DropFields.apply(
    frame=Filteroutprior2consent_node1688552940241,
    paths=[
        "birthday",
        "timestamp",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
        "serialnumber",
    ],
    transformation_ctx="DropFieldsCustomer_node1688286238783",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.getSink(
    path="s3://mig-buck1/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node3",
)
step_trainer_trusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted4"
)
step_trainer_trusted_node3.setFormat("json")
step_trainer_trusted_node3.writeFrame(DropFieldsCustomer_node1688286238783)
job.commit()
