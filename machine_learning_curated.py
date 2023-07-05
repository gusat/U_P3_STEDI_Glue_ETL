import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accel_trust
accel_trust_node1688404585418 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted1",
    transformation_ctx="accel_trust_node1688404585418",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node Renamed keys for accel /\ V steps
RenamedkeysforaccelVsteps_node1688555442329 = ApplyMapping.apply(
    frame=accel_trust_node1688404585418,
    mappings=[
        ("user", "string", "R_user", "string"),
        ("timestamp", "long", "R_timestamp", "long"),
        ("x", "double", "R_x", "double"),
        ("y", "double", "R_y", "double"),
        ("z", "double", "R_z", "double"),
        ("serialnumber", "string", "R_serialnumber", "string"),
        ("sharewithpublicasofdate", "long", "R_sharewithpublicasofdate", "long"),
        ("birthday", "string", "R_birthday", "string"),
        ("registrationdate", "long", "R_registrationdate", "long"),
        ("sharewithresearchasofdate", "long", "R_sharewithresearchasofdate", "long"),
        ("customername", "string", "R_customername", "string"),
        ("lastupdatedate", "long", "R_lastupdatedate", "long"),
        ("sharewithfriendsasofdate", "long", "R_sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforaccelVsteps_node1688555442329",
)

# Script generated for node accel /\ V steps
step_trainer_trusted_node1DF = step_trainer_trusted_node1.toDF()
RenamedkeysforaccelVsteps_node1688555442329DF = (
    RenamedkeysforaccelVsteps_node1688555442329.toDF()
)
accelVsteps_node1688404565646 = DynamicFrame.fromDF(
    step_trainer_trusted_node1DF.join(
        RenamedkeysforaccelVsteps_node1688555442329DF,
        (
            step_trainer_trusted_node1DF["sensorreadingtime"]
            == RenamedkeysforaccelVsteps_node1688555442329DF["R_timestamp"]
        ),
        "left",
    ),
    glueContext,
    "accelVsteps_node1688404565646",
)

# Script generated for node S3 ML_cured
S3ML_cured_node3 = glueContext.getSink(
    path="s3://mig-buck1/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3ML_cured_node3",
)
S3ML_cured_node3.setCatalogInfo(catalogDatabase="stedi", catalogTableName="ML_cured1")
S3ML_cured_node3.setFormat("json")
S3ML_cured_node3.writeFrame(accelVsteps_node1688404565646)
job.commit()
