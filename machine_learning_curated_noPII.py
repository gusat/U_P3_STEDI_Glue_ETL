import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import hashlib

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

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    accelVsteps_node1688404565646,
    [
        "PERSON_NAME",
        "EMAIL",
        "CREDIT_CARD",
        "IP_ADDRESS",
        "MAC_ADDRESS",
        "PHONE_NUMBER",
        "USA_ATIN",
        "USA_PASSPORT_NUMBER",
        "USA_PTIN",
        "USA_SSN",
        "USA_ITIN",
        "BANK_ACCOUNT",
        "USA_DRIVING_LICENSE",
        "USA_HCPCS_CODE",
        "USA_NATIONAL_DRUG_CODE",
        "USA_NATIONAL_PROVIDER_IDENTIFIER",
        "USA_DEA_NUMBER",
        "USA_HEALTH_INSURANCE_CLAIM_NUMBER",
        "USA_MEDICARE_BENEFICIARY_IDENTIFIER",
        "JAPAN_BANK_ACCOUNT",
        "JAPAN_DRIVING_LICENSE",
        "JAPAN_MY_NUMBER",
        "JAPAN_PASSPORT_NUMBER",
        "UK_BANK_ACCOUNT",
        "UK_BANK_SORT_CODE",
        "UK_DRIVING_LICENSE",
        "UK_ELECTORAL_ROLL_NUMBER",
        "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
        "UK_NATIONAL_INSURANCE_NUMBER",
        "UK_PASSPORT_NUMBER",
        "UK_PHONE_NUMBER",
        "UK_UNIQUE_TAXPAYER_REFERENCE_NUMBER",
        "UK_VALUE_ADDED_TAX",
        "ARGENTINA_TAX_IDENTIFICATION_NUMBER",
        "AUSTRALIA_BUSINESS_NUMBER",
        "AUSTRALIA_COMPANY_NUMBER",
        "AUSTRALIA_DRIVING_LICENSE",
        "AUSTRALIA_MEDICARE_NUMBER",
        "AUSTRALIA_PASSPORT_NUMBER",
        "AUSTRALIA_TAX_FILE_NUMBER",
        "AUSTRIA_DRIVING_LICENSE",
        "AUSTRIA_PASSPORT_NUMBER",
        "AUSTRIA_SSN",
        "AUSTRIA_TAX_IDENTIFICATION_NUMBER",
        "AUSTRIA_VALUE_ADDED_TAX",
        "BOSNIA_UNIQUE_MASTER_CITIZEN_NUMBER",
        "KOSOVO_UNIQUE_MASTER_CITIZEN_NUMBER",
        "MACEDONIA_UNIQUE_MASTER_CITIZEN_NUMBER",
        "MONTENEGRO_UNIQUE_MASTER_CITIZEN_NUMBER",
        "VOJVODINA_UNIQUE_MASTER_CITIZEN_NUMBER",
        "SERBIA_UNIQUE_MASTER_CITIZEN_NUMBER",
        "SERBIA_VALUE_ADDED_TAX",
        "BELGIUM_DRIVING_LICENSE",
        "BELGIUM_NATIONAL_IDENTIFICATION_NUMBER",
        "BELGIUM_PASSPORT_NUMBER",
        "BELGIUM_TAX_IDENTIFICATION_NUMBER",
        "BELGIUM_VALUE_ADDED_TAX",
        "BRAZIL_BANK_ACCOUNT",
        "BRAZIL_NATIONAL_IDENTIFICATION_NUMBER",
        "BRAZIL_NATIONAL_REGISTRY_OF_LEGAL_ENTITIES_NUMBER",
        "BRAZIL_NATURAL_PERSON_REGISTRY_NUMBER",
        "BULGARIA_DRIVING_LICENSE",
        "BULGARIA_UNIFORM_CIVIL_NUMBER",
        "BULGARIA_VALUE_ADDED_TAX",
        "CANADA_DRIVING_LICENSE",
        "CANADA_GOVERNMENT_IDENTIFICATION_CARD_NUMBER",
        "CANADA_PASSPORT_NUMBER",
        "CANADA_PERMANENT_RESIDENCE_NUMBER",
        "CANADA_PERSONAL_HEALTH_NUMBER",
        "CANADA_SOCIAL_INSURANCE_NUMBER",
        "CHILE_DRIVING_LICENSE",
        "CHILE_NATIONAL_IDENTIFICATION_NUMBER",
        "CHINA_IDENTIFICATION",
        "CHINA_LICENSE_PLATE_NUMBER",
        "CHINA_MAINLAND_TRAVEL_PERMIT_ID_HONG_KONG_MACAU",
        "CHINA_MAINLAND_TRAVEL_PERMIT_ID_TAIWAN",
        "CHINA_PASSPORT_NUMBER",
        "CHINA_PHONE_NUMBER",
        "HONG_KONG_IDENTITY_CARD",
        "MACAU_RESIDENT_IDENTITY_CARD",
        "TAIWAN_NATIONAL_IDENTIFICATION_NUMBER",
        "TAIWAN_PASSPORT_NUMBER",
        "COLOMBIA_PERSONAL_IDENTIFICATION_NUMBER",
        "COLOMBIA_TAX_IDENTIFICATION_NUMBER",
        "CROATIA_DRIVING_LICENSE",
        "CROATIA_IDENTITY_NUMBER",
        "CROATIA_PASSPORT_NUMBER",
        "CROATIA_PERSONAL_IDENTIFICATION_NUMBER",
        "CYPRUS_DRIVING_LICENSE",
        "CYPRUS_NATIONAL_IDENTIFICATION_NUMBER",
        "CYPRUS_PASSPORT_NUMBER",
        "CYPRUS_TAX_IDENTIFICATION_NUMBER",
        "CYPRUS_VALUE_ADDED_TAX",
        "CZECHIA_DRIVING_LICENSE",
        "CZECHIA_PERSONAL_IDENTIFICATION_NUMBER",
        "CZECHIA_VALUE_ADDED_TAX",
        "DENMARK_DRIVING_LICENSE",
        "DENMARK_PERSONAL_IDENTIFICATION_NUMBER",
        "DENMARK_TAX_IDENTIFICATION_NUMBER",
        "DENMARK_VALUE_ADDED_TAX",
        "ESTONIA_DRIVING_LICENSE",
        "ESTONIA_PASSPORT_NUMBER",
        "ESTONIA_PERSONAL_IDENTIFICATION_CODE",
        "ESTONIA_VALUE_ADDED_TAX",
        "FINLAND_DRIVING_LICENSE",
        "FINLAND_HEALTH_INSURANCE_NUMBER",
        "FINLAND_NATIONAL_IDENTIFICATION_NUMBER",
        "FINLAND_PASSPORT_NUMBER",
        "FINLAND_VALUE_ADDED_TAX",
        "FRANCE_BANK_ACCOUNT",
        "FRANCE_DRIVING_LICENSE",
        "FRANCE_HEALTH_INSURANCE_NUMBER",
        "FRANCE_INSEE_CODE",
        "FRANCE_NATIONAL_IDENTIFICATION_NUMBER",
        "FRANCE_PASSPORT_NUMBER",
        "FRANCE_TAX_IDENTIFICATION_NUMBER",
        "FRANCE_VALUE_ADDED_TAX",
        "GERMANY_BANK_ACCOUNT",
        "GERMANY_DRIVING_LICENSE",
        "GERMANY_PASSPORT_NUMBER",
        "GERMANY_PERSONAL_IDENTIFICATION_NUMBER",
        "GERMANY_TAX_IDENTIFICATION_NUMBER",
        "GERMANY_VALUE_ADDED_TAX",
        "GREECE_DRIVING_LICENSE",
        "GREECE_PASSPORT_NUMBER",
        "GREECE_SSN",
        "GREECE_TAX_IDENTIFICATION_NUMBER",
        "GREECE_VALUE_ADDED_TAX",
        "HUNGARY_DRIVING_LICENSE",
        "HUNGARY_PASSPORT_NUMBER",
        "HUNGARY_SSN",
        "HUNGARY_TAX_IDENTIFICATION_NUMBER",
        "HUNGARY_VALUE_ADDED_TAX",
        "ICELAND_NATIONAL_IDENTIFICATION_NUMBER",
        "ICELAND_PASSPORT_NUMBER",
        "ICELAND_VALUE_ADDED_TAX",
        "INDIA_AADHAAR_NUMBER",
        "INDIA_PERMANENT_ACCOUNT_NUMBER",
        "INDONESIA_IDENTITY_CARD_NUMBER",
        "IRELAND_DRIVING_LICENSE",
        "IRELAND_PASSPORT_NUMBER",
        "IRELAND_PERSONAL_PUBLIC_SERVICE_NUMBER",
        "IRELAND_TAX_IDENTIFICATION_NUMBER",
        "IRELAND_VALUE_ADDED_TAX",
        "ISRAEL_IDENTIFICATION_NUMBER",
        "ITALY_BANK_ACCOUNT",
        "ITALY_DRIVING_LICENSE",
        "ITALY_FISCAL_CODE",
        "ITALY_PASSPORT_NUMBER",
        "ITALY_VALUE_ADDED_TAX",
        "KOREA_PASSPORT_NUMBER",
        "KOREA_RESIDENCE_REGISTRATION_NUMBER_FOR_CITIZENS",
        "KOREA_RESIDENCE_REGISTRATION_NUMBER_FOR_FOREIGNERS",
        "LATVIA_DRIVING_LICENSE",
        "LATVIA_PASSPORT_NUMBER",
        "LATVIA_PERSONAL_IDENTIFICATION_NUMBER",
        "LATVIA_VALUE_ADDED_TAX",
        "LIECHTENSTEIN_NATIONAL_IDENTIFICATION_NUMBER",
        "LIECHTENSTEIN_PASSPORT_NUMBER",
        "LIECHTENSTEIN_TAX_IDENTIFICATION_NUMBER",
        "LITHUANIA_DRIVING_LICENSE",
        "LITHUANIA_PERSONAL_IDENTIFICATION_NUMBER",
        "LITHUANIA_TAX_IDENTIFICATION_NUMBER",
        "LITHUANIA_VALUE_ADDED_TAX",
        "LUXEMBOURG_DRIVING_LICENSE",
        "LUXEMBOURG_NATIONAL_INDIVIDUAL_NUMBER",
        "LUXEMBOURG_PASSPORT_NUMBER",
        "LUXEMBOURG_TAX_IDENTIFICATION_NUMBER",
        "LUXEMBOURG_VALUE_ADDED_TAX",
        "MALAYSIA_MYKAD_NUMBER",
        "MALAYSIA_PASSPORT_NUMBER",
        "MALTA_DRIVING_LICENSE",
        "MALTA_NATIONAL_IDENTIFICATION_NUMBER",
        "MALTA_TAX_IDENTIFICATION_NUMBER",
        "MALTA_VALUE_ADDED_TAX",
        "MEXICO_CLABE_NUMBER",
        "MEXICO_DRIVING_LICENSE",
        "MEXICO_PASSPORT_NUMBER",
        "MEXICO_TAX_IDENTIFICATION_NUMBER",
        "MEXICO_UNIQUE_POPULATION_REGISTRY_CODE",
        "NETHERLANDS_BANK_ACCOUNT",
        "NETHERLANDS_CITIZEN_SERVICE_NUMBER",
        "NETHERLANDS_DRIVING_LICENSE",
        "NETHERLANDS_PASSPORT_NUMBER",
        "NETHERLANDS_TAX_IDENTIFICATION_NUMBER",
        "NETHERLANDS_VALUE_ADDED_TAX",
        "NEW_ZEALAND_DRIVING_LICENSE",
        "NEW_ZEALAND_NATIONAL_HEALTH_INDEX_NUMBER",
        "NEW_ZEALAND_TAX_IDENTIFICATION_NUMBER",
        "NORWAY_BIRTH_NUMBER",
        "NORWAY_DRIVING_LICENSE",
        "NORWAY_HEALTH_INSURANCE_NUMBER",
        "NORWAY_NATIONAL_IDENTIFICATION_NUMBER",
        "NORWAY_VALUE_ADDED_TAX",
        "PHILIPPINES_DRIVING_LICENSE",
        "PHILIPPINES_PASSPORT_NUMBER",
        "POLAND_DRIVING_LICENSE",
        "POLAND_IDENTIFICATION_NUMBER",
        "POLAND_PASSPORT_NUMBER",
        "POLAND_REGON_NUMBER",
        "POLAND_SSN",
        "POLAND_TAX_IDENTIFICATION_NUMBER",
        "POLAND_VALUE_ADDED_TAX",
        "PORTUGAL_DRIVING_LICENSE",
        "PORTUGAL_NATIONAL_IDENTIFICATION_NUMBER",
        "PORTUGAL_PASSPORT_NUMBER",
        "PORTUGAL_TAX_IDENTIFICATION_NUMBER",
        "PORTUGAL_VALUE_ADDED_TAX",
        "ROMANIA_DRIVING_LICENSE",
        "ROMANIA_NUMERICAL_PERSONAL_CODE",
        "ROMANIA_PASSPORT_NUMBER",
        "ROMANIA_VALUE_ADDED_TAX",
        "SINGAPORE_DRIVING_LICENSE",
        "SINGAPORE_NATIONAL_REGISTRY_IDENTIFICATION_NUMBER",
        "SINGAPORE_PASSPORT_NUMBER",
        "SINGAPORE_UNIQUE_ENTITY_NUMBER",
        "SLOVAKIA_DRIVING_LICENSE",
        "SLOVAKIA_NATIONAL_IDENTIFICATION_NUMBER",
        "SLOVAKIA_PASSPORT_NUMBER",
        "SLOVAKIA_VALUE_ADDED_TAX",
        "SLOVENIA_DRIVING_LICENSE",
        "SLOVENIA_PASSPORT_NUMBER",
        "SLOVENIA_TAX_IDENTIFICATION_NUMBER",
        "SLOVENIA_UNIQUE_MASTER_CITIZEN_NUMBER",
        "SLOVENIA_VALUE_ADDED_TAX",
        "SOUTH_AFRICA_PERSONAL_IDENTIFICATION_NUMBER",
        "SPAIN_BANK_ACCOUNT",
        "SPAIN_DNI",
        "SPAIN_DRIVING_LICENSE",
        "SPAIN_NIE",
        "SPAIN_NIF",
        "SPAIN_PASSPORT_NUMBER",
        "SPAIN_SSN",
        "SPAIN_VALUE_ADDED_TAX",
        "SRI_LANKA_NATIONAL_IDENTIFICATION_NUMBER",
        "SWEDEN_DRIVING_LICENSE",
        "SWEDEN_PASSPORT_NUMBER",
        "SWEDEN_PERSONAL_IDENTIFICATION_NUMBER",
        "SWEDEN_TAX_IDENTIFICATION_NUMBER",
        "SWEDEN_VALUE_ADDED_TAX",
        "SWITZERLAND_AHV",
        "SWITZERLAND_HEALTH_INSURANCE_NUMBER",
        "SWITZERLAND_PASSPORT_NUMBER",
        "SWITZERLAND_VALUE_ADDED_TAX",
        "THAILAND_PASSPORT_NUMBER",
        "THAILAND_PERSONAL_IDENTIFICATION_NUMBER",
        "TURKEY_NATIONAL_IDENTIFICATION_NUMBER",
        "TURKEY_PASSPORT_NUMBER",
        "TURKEY_VALUE_ADDED_TAX",
        "UKRAINE_INDIVIDUAL_IDENTIFICATION_NUMBER",
        "UKRAINE_PASSPORT_NUMBER_DOMESTIC",
        "UKRAINE_PASSPORT_NUMBER_INTERNATIONAL",
        "UNITED_ARAB_EMIRATES_PERSONAL_NUMBER",
        "VENEZUELA_DRIVING_LICENSE",
        "VENEZUELA_NATIONAL_IDENTIFICATION_NUMBER",
        "VENEZUELA_VALUE_ADDED_TAX",
    ],
    1.0,
    0.1,
)


def pii_column_hash(original_cell_value):
    return hashlib.sha256(original_cell_value.encode()).hexdigest()


pii_column_hash_udf = udf(pii_column_hash, StringType())


def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")


DetectSensitiveData_node1688560976702 = hashDf(
    accelVsteps_node1688404565646, list(classified_map.keys())
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
S3ML_cured_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated_noPII"
)
S3ML_cured_node3.setFormat("json")
S3ML_cured_node3.writeFrame(DetectSensitiveData_node1688560976702)
job.commit()
