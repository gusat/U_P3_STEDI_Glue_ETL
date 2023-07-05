CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  user STRING,
  timeStamp BIGINT,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mig-buck1/accelerometer/landing/'
TBLPROPERTIES (
  'classification' = 'json',
  'data_source_name' = 'AwsDataCatalog',
  'data_source_type' = 'AWS Glue Data Catalog',
  'has_encrypted_data' = 'false',
  'create_time' = '2023-06-30T11:13:18.000+02:00'
  'description' = 'raw accel. data',
  'last_updated' = 'June 30, 2023 at 09:25:40'
);
