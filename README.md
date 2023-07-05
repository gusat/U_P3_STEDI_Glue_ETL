# U_P3_STEDI_Glue_ETL
Udacity P3: STEDI Human Balance Analytics

## Intro
The first 80% of P3 took ca. 4 hrs., while the last 20% took 9+ FT days, owing mostly to AWS' glitches and (too) many Glue4.0 idiosyncrasies; these were documented for our (IBM cohort23) instructors.

**Initial trials:** For testing purposes, I've selected a smaller data subset for ingestion. After visual inspection, I've hand-picked these 3 files that do have common emails and SN's:
- `customer/landing/customer-keep-1655293787679.json`
- `accelerometer/landing/accelerometer-1655471583651.json`
- `step_trainer/landing/step_trainer-1655296678763.json`

## Workflow
All the AWS Glue jobs use only visual nodes, e.g., JOINs and Filters, with very few custom filters (also erratic in Glue 3/4); see the ETL jobs below.

### Glue Job 1 - Sanitize Customer Data and Create `customer_trusted` Table:
- Read the Customer data from the Landing Zone (`customer_landing` table).
- Filter out the Customer Records where the `shareWithResearchAsOfDate` field is not equal to 0.
- Store the filtered data in the Trusted Zone as a Glue Table called `customer_trusted`.

### Glue Job 2 - Sanitize Accelerometer Data and Create `accelerometer_trusted` Table:
- Read the Accelerometer data from the Landing Zone (`accelerometer_landing` table).
- Filter the Accelerometer Readings for customers who have agreed to share their data (based on the `customer_trusted` table).
- Store the filtered data in the Trusted Zone as a Glue Table called `accelerometer_trusted`.

### Glue Job 3 - Sanitize Customer Data (Curated) and Create `customer_curated` Table:
- Read the Customer data from the Trusted Zone (`customer_trusted` table).
- Join the Customer data with the Accelerometer data (`accelerometer_trusted` table) based on a common key (e.g., email).
- Drop the unnecessary fields from the joined data (such as x, y, z, user, timestamp).
- Store the curated data in the Curated Zone as a Glue Table called `customer_curated`.

### Glue Job 4 - Read Step Trainer IoT Data and Create `step_trainer_trusted` Table:
- Read the Step Trainer IoT data stream from S3.
- Filter out the Step Trainer Records for customers who have accelerometer data and have agreed to share their data (based on the `customer_curated` table).
- Store the filtered data in the Trusted Zone as a Glue Table called `step_trainer_trusted`.

### Glue Job 5 - Create `machine_learning_curated` Table:
- Join the Step Trainer Records (`step_trainer_trusted` table) with the associated accelerometer reading data (from `accelerometer_trusted` table) based on the timestamp.
- Only include records for customers who have agreed to share their data (based on the `customer_curated` table).
- Store the resulting data in the Curated Zone as a Glue Table called `machine_learning_curated`.
