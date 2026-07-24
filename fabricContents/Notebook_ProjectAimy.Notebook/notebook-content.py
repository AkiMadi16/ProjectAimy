# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fe0976b8-40a1-42dd-a9a5-56a4ae7cde8c",
# META       "default_lakehouse_name": "PA_DB",
# META       "default_lakehouse_workspace_id": "fe998802-535a-48c0-a157-36409c78eeaf",
# META       "known_lakehouses": [
# META         {
# META           "id": "fe0976b8-40a1-42dd-a9a5-56a4ae7cde8c"
# META         },
# META         {
# META           "id": "11e3bb97-e105-4bb2-acb9-cf893bdf9662"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Project Aimy Notebook**
# 
# This Notebook contains Cleaning, validation and transformation of the Project Aimy Dataset.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **List all tables in the current Database - Python**

# CELL ********************

"""
spark.sql("SHOW TABLES").show()
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Truncate all tables and reload from the copyjob activity to lakehouse

# CELL ********************

"""
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_Enrollment")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_Attendance")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_HeadCount")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_BusinessUnit")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_Org")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_Attendee")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_Program")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_ProgramCategory")
spark.sql("TRUNCATE TABLE "PA_SBD.dbo.stg_PublicHoliday")


spark.sql("DROP TABLE IF EXISTS PA_SDB.dbo.cln_businessunit")
spark.sql("DROP TABLE IF EXISTS PA_SDB.dbo.cln_organisation")
spark.sql("DROP TABLE IF EXISTS PA_SDB.dbo.cln_attendee")
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************


staging_tables = [
    "PA_DB.dbo.Enrollment",
    "PA_DB.dbo.Attendance",
    "PA_DB.dbo.Attendee",
    "PA_DB.dbo.Org",
    "PA_DB.dbo.BusinessUnit",
    "PA_DB.dbo.Program",
    "PA_DB.dbo.ProgramCategory",
    "PA_DB.dbo.HeadCount"
]

for table in staging_tables:
    count = spark.sql(f"SELECT COUNT(*) AS Rows FROM {table}").collect()[0]["Rows"]
    print(f"{table}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. READ STAGING TABLES

stg_enrollment = spark.table("PA_DB.dbo.Enrollment")
stg_attendance = spark.table("PA_DB.dbo.Attendance")
stg_attendee = spark.table("PA_DB.dbo.Attendee")
stg_org = spark.table("PA_DB.dbo.Org")
stg_business_unit = spark.table("PA_DB.dbo.BusinessUnit")
stg_headcount = spark.table("PA_DB.dbo.HeadCount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Business Unit source row count:", stg_business_unit.count())

stg_business_unit.printSchema()

stg_business_unit.select(
    F.count("*").alias("TotalRows"),
    F.countDistinct("Id").alias("DistinctBusinessUnitIds"),
    F.sum(
        F.when(F.col("Id").isNull(), 1).otherwise(0)
    ).alias("MissingIds")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean the BusinessUnit Data Source**

# CELL ********************

business_unit_source = (
    stg_business_unit
    .select(
        F.col("Id").cast("long").alias("BusinessUnitId"),
        F.col("Code").cast("string").alias("BusinessUnitCode"),
        F.col("Name").cast("string").alias("BusinessUnitName"),
        F.col("EnterpriseId").cast("long").alias("EnterpriseId"),
        F.col("TypeId").cast("long").alias("TypeId"),
        F.col("IsActive").cast("boolean").alias("IsActive"),
        F.to_timestamp(
            F.col("CreatedOn").cast("string")
        ).alias("CreatedOn"),
        F.to_timestamp(
            F.col("UpdatedOn").cast("string")
        ).alias("UpdatedOn")
    )
    .filter(F.col("BusinessUnitId").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Keep the latest record for each Business unit**
# This prefers -
# 1. Active record
# 2. Latest UpdatedOn
# 3. Latest CreatedOn

# CELL ********************

business_unit_window = (
    Window
    .partitionBy("BusinessUnitId")
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

business_unit_clean = (
    business_unit_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(business_unit_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)

#Save after transforming data

(
    business_unit_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("PA_SDB.dbo.cln_BusinessUnit")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Check the Source table for Organisation**

# CELL ********************

print("Organisation source row count:", stg_org.count())

stg_org.printSchema()

stg_org.select(
    F.count("*").alias("TotalRows"),
    F.countDistinct("Id").alias("DistinctOrganisationIds"),
    F.sum(
        F.when(F.col("Id").isNull(), 1).otherwise(0)
    ).alias("MissingOrganisationIds"),
    F.sum(
        F.when(F.col("BusinessUnitId").isNull(), 1).otherwise(0)
    ).alias("MissingBusinessUnitIds")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean the Source Table**

# CELL ********************

organisation_source = (
    stg_org
    .select(
        F.col("Id").cast("long").alias("OrganisationId"),
        F.col("BusinessUnitId").cast("long").alias("BusinessUnitId"),
        F.col("Code").cast("string").alias("OrganisationCode"),
        F.col("Name").cast("string").alias("OrganisationName"),
        F.col("EnterpriseId").cast("long").alias("EnterpriseId"),
        F.col("TypeId").cast("long").alias("TypeId"),
        F.col("IsActive").cast("boolean").alias("IsActive"),
        F.to_timestamp(
            F.col("CreatedOn").cast("string")
        ).alias("CreatedOn"),
        F.to_timestamp(
            F.col("UpdatedOn").cast("string")
        ).alias("UpdatedOn")
    )
    .filter(
        F.col("OrganisationId").isNotNull()
        & F.col("BusinessUnitId").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Keep the latest Organisation record**
# 
# The Organisation business key is : **BusinessUnitId + OrganisationId**

# CELL ********************

organisation_window = (
    Window
    .partitionBy(
        "BusinessUnitId",
        "OrganisationId"
    )
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

organisation_clean = (
    organisation_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(organisation_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)
#Save after transformation
(
    organisation_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("PA_SDB.dbo.cln_Organisation")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean the table for Attendee**

# CELL ********************

attendee_source = (
    stg_attendee
    .select(
        F.col("Id").cast("long").alias("AttendeeId"),
        F.col("BusinessUnitId").cast("long").alias("BusinessUnitId"),
        F.col("AccountId").cast("long").alias("AccountId"),
        F.col("EnterpriseId").cast("long").alias("EnterpriseId"),
        F.col("GenderId").cast("long").alias("GenderId"),
        F.col("StatusId").cast("long").alias("StatusId"),
        F.col("IsActive").cast("boolean").alias("IsActive"),

        F.to_date(
            F.col("DateOfBirth").cast("string")
        ).alias("RawDateOfBirth"),

        F.to_timestamp(
            F.col("CreatedOn").cast("string")
        ).alias("CreatedOn"),

        F.to_timestamp(
            F.col("UpdatedOn").cast("string")
        ).alias("UpdatedOn")
    )
    .filter(
        F.col("AttendeeId").isNotNull()
        & F.col("BusinessUnitId").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean unreasonable dates of birth**

# CELL ********************

attendee_src = (
    attendee_source
    .withColumn(
        "DateOfBirth",
        F.when(
             F.col("RawDateOfBirth").rlike(r"^\d{4}-\d{2}-\d{2}$") &
            (F.to_date("RawDateOfBirth") >= F.to_date(F.lit("1900-01-01")))
            & (F.to_date("RawDateOfBirth") <= F.current_date()),
            F.to_date("RawDateOfBirth")
        ).otherwise(
            F.lit(None).cast("date")
        )
    )
    .drop("RawDateOfBirth")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Keep the latest attendee version with latest records**

# CELL ********************

attendee_window = (
    Window
    .partitionBy(
        "BusinessUnitId",
        "AttendeeId"
    )
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

attendee_clean = (
    attendee_src
    .withColumn(
        "_RowNumber",
        F.row_number().over(attendee_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)
(
    attendee_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("PA_SDB.dbo.cln_Attendee")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean the Source table for Enrollments**

# CELL ********************

enrollment_source = (
    stg_enrollment
    .select(
        F.col("Id").cast("long").alias("EnrollmentId"),
        F.col("BusinessUnitId").cast("long").alias("BusinessUnitId"),
        F.col("OrgId").cast("long").alias("OrganisationId"),
        F.col("AttendeeId").cast("long").alias("AttendeeId"),
        F.col("AccountId").cast("long").alias("AccountId"),
        F.col("SubscriptionId").cast("long").alias("SubscriptionId"),
        F.col("StatusId").cast("long").alias("StatusId"),

        F.to_date(
            F.col("EnrolledDate").cast("string")
        ).alias("EnrolledDate"),

        F.to_date(
            F.col("StartDate").cast("string")
        ).alias("StartDate"),

        F.to_date(
            F.col("EndDate").cast("string")
        ).alias("EndDate"),

        F.col("IsActive").cast("boolean").alias("IsActive"),

        F.to_timestamp(
            F.col("CreatedOn").cast("string")
        ).alias("CreatedOn"),

        F.to_timestamp(
            F.col("UpdatedOn").cast("string")
        ).alias("UpdatedOn")
    )
    .filter(
        F.col("EnrollmentId").isNotNull()
        & F.col("BusinessUnitId").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

enrollment_window = (
    Window
    .partitionBy(
        "BusinessUnitId",
        "EnrollmentId"
    )
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

enrollment_clean = (
    enrollment_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(enrollment_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)

(
    enrollment_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("PA_SDB.dbo.cln_Enrollment")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean the Source table Attendance**

# CELL ********************

attendance_source = (
    stg_attendance
    .select(
        F.col("Id").cast("long").alias("AttendanceId"),
        F.col("EnrollmentId").cast("long").alias("EnrollmentId"),
        F.col("OrgId").cast("long").alias("OrganisationId"),
        F.col("AttendeeId").cast("long").alias("AttendeeId"),
        F.col("ProgramId").cast("long").alias("ProgramId"),
        F.col("BookingId").cast("long").alias("BookingId"),
        F.col("TypeId").cast("long").alias("TypeId"),

        F.to_date(
            F.col("Date").cast("string")
        ).alias("AttendanceDate"),

        F.to_timestamp(
            F.col("TimeStart").cast("string")
        ).alias("TimeStart"),

        F.to_timestamp(
            F.col("TimeEnd").cast("string")
        ).alias("TimeEnd"),

        F.col("IsActive").cast("boolean").alias("IsActive"),

        F.to_timestamp(
            F.col("CreatedOn").cast("string")
        ).alias("CreatedOn"),

        F.to_timestamp(
            F.col("UpdatedOn").cast("string")
        ).alias("UpdatedOn")
    )
    .filter(F.col("AttendanceId").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Keep one attendance row per id**

# CELL ********************

attendance_window = (
    Window
    .partitionBy("AttendanceId")
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

attendance_clean = (
    attendance_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(attendance_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)

(
    attendance_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("PA_SDB.dbo.cln_Attendance")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Clean the source table for Headcount**

# CELL ********************

headcount_source = (
    stg_headcount
    .select(
        F.col("Id").cast("long").alias("HeadcountId"),
        F.col("OrgId").cast("long").alias("OrganisationId"),
        F.col("TermProgramSetId").cast("long").alias("TermProgramSetId"),
        F.col("TermProductId").cast("long").alias("TermProductId"),
        F.col("Description").cast("string").alias("Description"),
        F.col("Name").cast("string").alias("HeadcountType"),
        F.col("StatusId").cast("long").alias("StatusId"),
        F.col("TotalCount").cast("int").alias("TotalCount"),
        F.col("IsActive").cast("boolean").alias("IsActive"),

        F.to_timestamp(
            F.col("CreatedOn").cast("string")
        ).alias("CreatedOn"),

        F.to_timestamp(
            F.col("UpdatedOn").cast("string")
        ).alias("UpdatedOn")
    )
    .filter(F.col("HeadcountId").isNotNull())
    .dropDuplicates(["HeadcountId"])
)

headcount_window = (
    Window
    .partitionBy("HeadcountId")
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

headcount_clean = (
    headcount_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(headcount_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)

(
    headcount_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("PA_SDB.dbo.cln_HeadCount")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_tables = [
    "PA_SDB.dbo.cln_Enrollment",
    "PA_SDB.dbo.cln_Attendance",
    "PA_SDB.dbo.cln_Attendee",
    "PA_SDB.dbo.cln_Organisation",
    "PA_SDB.dbo.cln_BusinessUnit",
    "PA_SDB.dbo.cln_HeadCount"
]

for tbl in cleaned_tables:
    count = spark.sql(f"SELECT COUNT(*) AS Rows FROM {tbl}").collect()[0]["Rows"]
    print(f"{tbl}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
