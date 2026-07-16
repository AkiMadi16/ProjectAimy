# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fcd3d422-8fa9-432e-aef1-93e1dbba1f51",
# META       "default_lakehouse_name": "PA_DW",
# META       "default_lakehouse_workspace_id": "fe998802-535a-48c0-a157-36409c78eeaf",
# META       "known_lakehouses": [
# META         {
# META           "id": "fcd3d422-8fa9-432e-aef1-93e1dbba1f51"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Project Aimy Data Model**
# 
# Fabric PySpark Notebook contains the Project Aimy Star Schema Data Model with surrogate keys, dimensions and facts.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. READ Cleanned TABLES
enrollment_source = spark.table("cln_Enrollment")
attendance_source = spark.table("cln_Attendance")
attendee_source = spark.table("cln_Attendee")
organisation_source = spark.table("cln_Organisation")
businessunit_source = spark.table("cln_BusinessUnit")
headcount_source = spark.table("cln_HeadCount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
spark.sql("DROP TABLE IF EXISTS dbo.Dim_Attendee")
spark.sql("DROP TABLE IF EXISTS dbo.fact_attendance")
spark.sql("DROP TABLE IF EXISTS dbo.Dim_Date")
spark.sql("DROP TABLE IF EXISTS dbo.Dim_businessunit")
spark.sql("DROP TABLE IF EXISTS dbo.Dim_Organisation")
spark.sql("DROP TABLE IF EXISTS dbo.fact_enrolment")
spark.sql("DROP TABLE IF EXISTS dbo.fact_headcount")
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. HELPERS
def add_key(df, key_name, order_columns):
    w = Window.orderBy(*[F.col(c).asc_nulls_last() for c in order_columns])
    return df.withColumn(key_name, F.row_number().over(w).cast("long"))


def save_table(df, table_name):
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))
    print(f"Created {table_name}: {df.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# 4. DIM_DATE
dim_date = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit("2000-01-01")),
                F.to_date(F.lit("2035-12-31")),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("FullDate")
    )
    .select(
        F.date_format("FullDate", "yyyyMMdd").cast("int").alias("DateKey"),
        "FullDate",
        F.dayofmonth("FullDate").alias("DayNumber"),
        F.date_format("FullDate", "EEEE").alias("DayName"),
        F.weekofyear("FullDate").alias("WeekNumber"),
        F.month("FullDate").alias("MonthNumber"),
        F.date_format("FullDate", "MMMM").alias("MonthName"),
        F.quarter("FullDate").alias("Quarter"),
        F.year("FullDate").alias("Year"),
        F.date_format("FullDate", "yyyy-MM").alias("YearMonth"),
        F.date_format("FullDate", "E").isin("Sat", "Sun").alias("IsWeekend")
    )
)
save_table(dim_date, "Dim_Date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. DIM_BUSINESSUNIT 

business_unit_key_window = Window.orderBy(
    F.col("BusinessUnitId")
)

dim_businessunit = (
    businessunit_source
    .withColumn(
        "BusinessUnitKey",
        F.row_number()
        .over(business_unit_key_window)
        .cast("long")
    )
)

#Validate the final Business Unit dimension

dim_businessunit.select(
    F.count("*").alias("FinalRows"),
    F.countDistinct("BusinessUnitKey").alias("DistinctKeys"),
    F.countDistinct("BusinessUnitId").alias("DistinctBusinessUnitIds"),
    F.sum(
        F.when(F.col("BusinessUnitId").isNull(), 1).otherwise(0)
    ).alias("MissingBusinessUnitIds")
).show()

 ##Check for duplicates

dim_businessunit.groupBy(
    "BusinessUnitId"
).count().filter(
    F.col("count") > 1
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dbo.Dim_BusinessUnit")

(
    dim_businessunit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.Dim_BusinessUnit")
)

print(
    "Dim_BusinessUnit saved:",
    spark.table("dbo.Dim_BusinessUnit").count(),
    "rows"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the saved Business Unit dimension
dim_businessunit = spark.table("dbo.Dim_BusinessUnit")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. DIM_ORGANISATION

# BusinessUnitId + OrganisationId

#Create the surrogate key
organisation_key_window = Window.orderBy(
    F.col("BusinessUnitId"),
    F.col("OrganisationId")
)

dim_organisation = (
    organisation_source
    .withColumn(
        "OrganisationKey",
        F.row_number()
        .over(organisation_key_window)
        .cast("long")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Connect Organisation to businessunit**

# CELL ********************

dim_organisation = (
    dim_organisation
    .join(
        dim_businessunit.select(
            "BusinessUnitId",
            "BusinessUnitKey"
        ),
        on="BusinessUnitId",
        how="left"
    )
    .fillna({
        "BusinessUnitKey": 0
    })
)


#organising columns to keep orgKey in first place as we added it to the end
dim_organisation = dim_organisation.select(
    "OrganisationKey",
    "OrganisationId",
    "BusinessUnitKey",
    "BusinessUnitId",
    "OrganisationCode",
    "OrganisationName",
    "EnterpriseId",
    "TypeId",
    "IsActive",
    "CreatedOn",
    "UpdatedOn"
)

dim_organisation.select(
    F.count("*").alias("FinalRows"),
    F.countDistinct("OrganisationKey").alias("DistinctKeys"),
    F.countDistinct(
        "BusinessUnitId",
        "OrganisationId"
    ).alias("DistinctOrganisationBusinessKeys"),
    F.sum(
        F.when(F.col("BusinessUnitKey") == 0, 1).otherwise(0)
    ).alias("UnmatchedBusinessUnits")
).show()

dim_organisation.filter(
    (F.col("OrganisationId") != -1)
    & (F.col("BusinessUnitKey") == 0)
).select(
    "OrganisationId",
    "BusinessUnitId",
    "OrganisationCode",
    "OrganisationName"
).show(truncate=False)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check duplicate Organisation business keys

dim_organisation.groupBy(
    "BusinessUnitId",
    "OrganisationId"
).count().filter(
    F.col("count") > 1
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dbo.Dim_Organisation")

(
    dim_organisation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.Dim_Organisation")
)

print(
    "Dim_Organisation saved:",
    spark.table("dbo.Dim_Organisation").count(),
    "rows"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Dim attendee Table**

# CELL ********************

dim_attendee = add_key(
    attendee_source,
    "AttendeeKey",
    ["BusinessUnitId", "AttendeeId"]
)

#join the business unit

dim_attendee = (
    dim_attendee
    .join(
        dim_businessunit.select(
            "BusinessUnitId",
            "BusinessUnitKey"
        ),
        on="BusinessUnitId",
        how="left"
    )
    .fillna({
        "BusinessUnitKey": 0
    })
)

dim_attendee = dim_attendee.select(
    "AttendeeKey",
    "AttendeeId",
    "BusinessUnitKey",
    "BusinessUnitId",
    "AccountId",
    "EnterpriseId",
    "GenderId",
    "DateOfBirth",
    "StatusId",
    "IsActive",
    "CreatedOn",
    "UpdatedOn"
)

dim_attendee.printSchema()

dim_attendee.select(
    F.count("*").alias("TotalRows"),
    F.sum(
        F.when(
            F.col("DateOfBirth").isNull(),
            1
        ).otherwise(0)
    ).alias("MissingOrInvalidDateOfBirth"),
    F.sum(
        F.when(
            F.col("BusinessUnitKey") == 0,
            1
        ).otherwise(0)
    ).alias("UnmatchedBusinessUnits")
).show()

save_table(
    dim_attendee,
    "dbo.Dim_Attendee"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build Fact_Enrollment table**

# CELL ********************

# Read the saved Organisation dimension
dim_organisation = spark.table("dbo.Dim_Organisation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_enrollment = add_key(
    enrollment_source,
    "EnrollmentFactKey",
    ["BusinessUnitId", "EnrollmentId"]
)

fact_enrollment = (
    fact_enrollment
    .join(
        dim_businessunit.select(
            "BusinessUnitId",
            "BusinessUnitKey"
        ),
        on="BusinessUnitId",
        how="left"
    )
    .join(
        dim_organisation.select(
            "BusinessUnitId",
            "OrganisationId",
            "OrganisationKey"
        ),
        on=[
            "BusinessUnitId",
            "OrganisationId"
        ],
        how="left"
    )
    .join(
        dim_attendee.select(
            "BusinessUnitId",
            "AttendeeId",
            "AttendeeKey"
        ),
        on=[
            "BusinessUnitId",
            "AttendeeId"
        ],
        how="left"
    )
    .fillna({
        "BusinessUnitKey": 0,
        "OrganisationKey": 0,
        "AttendeeKey": 0
    })
    .withColumn(
        "EnrolledDateKey",
        F.date_format("EnrolledDate", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "StartDateKey",
        F.date_format("StartDate", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "EndDateKey",
        F.date_format("EndDate", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "EnrollmentCount",
        F.lit(1)
    )
)

fact_enrollment = fact_enrollment.select(
    "EnrollmentFactKey",
    "EnrollmentId",
    "BusinessUnitKey",
    "OrganisationKey",
    "AttendeeKey",
    "BusinessUnitId",
    "OrganisationId",
    "AttendeeId",
    "AccountId",
    "SubscriptionId",
    "StatusId",
    "EnrolledDateKey",
    "StartDateKey",
    "EndDateKey",
    "EnrolledDate",
    "StartDate",
    "EndDate",
    "IsActive",
    "EnrollmentCount",
    "CreatedOn",
    "UpdatedOn"
)

save_table(
    fact_enrollment,
    "dbo.Fact_Enrollment"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the saved Fact Enrollment 
fact_enrollment_enrollment = spark.table("dbo.Fact_Enrollment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

enrollment_lookup = (
    fact_enrollment
    .select(
        "EnrollmentId",
        "BusinessUnitId",
        "EnrollmentFactKey"
    )
    .dropDuplicates(["EnrollmentId"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_attendance = (
    attendance_source.alias("attendance")
    .join(
        enrollment_lookup.alias("enrollment"),
        F.col("attendance.EnrollmentId")
        == F.col("enrollment.EnrollmentId"),
        how="left"
    )
    .select(
        "attendance.*",
        F.col("enrollment.BusinessUnitId").alias("BusinessUnitId"),
        F.col("enrollment.EnrollmentFactKey").alias("EnrollmentFactKey")
    )
)
fact_attendance = add_key(
    fact_attendance,
    "AttendanceFactKey",
    ["AttendanceId"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_attendance = (
    fact_attendance
    .join(
        dim_businessunit.select(
            "BusinessUnitId",
            "BusinessUnitKey"
        ),
        on="BusinessUnitId",
        how="left"
    )
    .join(
        dim_organisation.select(
            "BusinessUnitId",
            "OrganisationId",
            "OrganisationKey"
        ),
        on=[
            "BusinessUnitId",
            "OrganisationId"
        ],
        how="left"
    )
    .join(
        dim_attendee.select(
            "BusinessUnitId",
            "AttendeeId",
            "AttendeeKey"
        ),
        on=[
            "BusinessUnitId",
            "AttendeeId"
        ],
        how="left"
    )
    .fillna({
        "BusinessUnitKey": 0,
        "OrganisationKey": 0,
        "AttendeeKey": 0
    })
    .withColumn(
        "AttendanceDateKey",
        F.date_format("AttendanceDate", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "AttendanceMinutes",
        F.when(
            F.col("TimeStart").isNotNull()
            & F.col("TimeEnd").isNotNull()
            & (F.col("TimeEnd") >= F.col("TimeStart")),
            (
                F.unix_timestamp("TimeEnd")
                - F.unix_timestamp("TimeStart")
            ) / 60
        ).cast("int")
    )
    .withColumn(
        "AttendanceCount",
        F.lit(1)
    )
)

fact_attendance = fact_attendance.select(
    "AttendanceFactKey",
    "AttendanceId",
    "EnrollmentFactKey",
    "BusinessUnitKey",
    "OrganisationKey",
    "AttendeeKey",
    "EnrollmentId",
    "BusinessUnitId",
    "OrganisationId",
    "AttendeeId",
    "ProgramId",
    "BookingId",
    "TypeId",
    "AttendanceDateKey",
    "AttendanceDate",
    "TimeStart",
    "TimeEnd",
    "AttendanceMinutes",
    "AttendanceCount",
    "IsActive",
    "CreatedOn",
    "UpdatedOn"
)

save_table(
    fact_attendance,
    "dbo.Fact_Attendance"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_headcount = add_key(
    headcount_source,
    "HeadcountFactKey",
    ["HeadcountId"]
)

fact_headcount = (
    fact_headcount
    .join(
        dim_organisation.select(
            "OrganisationId",
            "BusinessUnitId",
            "OrganisationKey"
        ),
        on="OrganisationId",
        how="left"
    )
    .join(
        dim_businessunit.select(
            "BusinessUnitId",
            "BusinessUnitKey"
        ),
        on="BusinessUnitId",
        how="left"
    )
    .fillna({
        "BusinessUnitKey": 0,
        "OrganisationKey": 0
    })
    .withColumn(
        "HeadcountDate",
        F.to_date("CreatedOn")
    )
    .withColumn(
        "HeadcountDateKey",
        F.date_format("HeadcountDate", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "HeadcountRecordCount",
        F.lit(1)
    )
)

fact_headcount = fact_headcount.select(
    "HeadcountFactKey",
    "HeadcountId",
    "BusinessUnitKey",
    "OrganisationKey",
    "BusinessUnitId",
    "OrganisationId",
    "TermProgramSetId",
    "TermProductId",
    "StatusId",
    "HeadcountDateKey",
    "HeadcountDate",
    "Description",
    "HeadcountType",
    "TotalCount",
    "HeadcountRecordCount",
    "IsActive",
    "CreatedOn",
    "UpdatedOn"
)

save_table(
    fact_headcount,
    "dbo.Fact_Headcount"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Final Validation**

# CELL ********************

model_tables = [
    "dbo.Dim_Attendee",
    "dbo.Fact_Enrollment",
    "dbo.Fact_Attendance",
    "dbo.Fact_Headcount",
    "dbo.Dim_Date",
    "dbo.Dim_BusinessUnit",
    "dbo.Dim_Organisation"
]

for table_name in model_tables:
    print(
        f"{table_name}: {spark.table(table_name).count()} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Check Unmatched Relationships**

# CELL ********************

print("Enrollments without matched attendees:")

spark.table("dbo.Fact_Enrollment").filter(
    F.col("AttendeeKey") == 0
).select(
    "EnrollmentId",
    "BusinessUnitId",
    "AttendeeId",
    "AccountId",
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Attendance without matched enrollments:")

spark.table("dbo.Fact_Attendance").filter(
    F.col("EnrollmentFactKey").isNull()
).select(
    "AttendanceId",
    "EnrollmentId",
    "AttendeeId",
    "OrganisationId"
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
