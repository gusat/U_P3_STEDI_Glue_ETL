CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  serialnumber STRING,
  sharewithpublicasofdate BIGINT,
  birthday STRING,
  registrationdate BIGINT,
  sharewithresearchasofdate BIGINT,
  customername STRING,
  email STRING,
  lastupdatedate BIGINT,
  phone STRING,
  sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mig-buck1/customer/landing/'
TBLPROPERTIES (
  'classification' = 'json',
  'data_source_name' = 'AwsDataCatalog',
  'data_source_type' = 'AWS Glue Data Catalog',
  'has_encrypted_data' = 'false',
  'create_time' = '2023-06-30T11:13:15.000+02:00'
  'description' = 'raw cust. data',
  'last_updated' = 'June 30, 2023 at 09:23:40'
);