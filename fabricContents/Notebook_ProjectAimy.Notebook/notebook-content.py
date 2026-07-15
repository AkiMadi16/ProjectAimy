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

# # **Project Aimy Notebook**

# MARKDOWN ********************

# **Center Management Star Schema**

# MARKDOWN ********************

# #### List all tables in the current Database - Python

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CELL ********************

spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Describe stg_PublicHoliday

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. READ STAGING TABLES
stg_enrolment = spark.table("stg_Enrollment")
stg_attendance = spark.table("stg_Attendance")
stg_attendee = spark.table("stg_Attendee")
stg_org = spark.table("stg_Org")
stg_business_unit = spark.table("stg_BusinessUnit")
stg_headcount = spark.table("stg_HeadCount")

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

# 4. DIM_DATE
dim_date = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit("2020-01-01")),
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
dim_business_unit = (
    stg_business_unit
    .select(
        F.col("Id").cast("long").alias("BusinessUnitId"),
        F.col("Code").cast("string").alias("BusinessUnitCode"),
        F.col("Name").cast("string").alias("BusinessUnitName"),
        F.col("EnterpriseId").cast("long").alias("EnterpriseId"),
        F.col("TypeId").cast("long").alias("TypeId"),
        F.col("IsActive").cast("boolean").alias("IsActive"),
        F.col("CreatedOn").cast("timestamp").alias("CreatedOn"),
        F.col("UpdatedOn").cast("timestamp").alias("UpdatedOn")
    )
    .filter(F.col("BusinessUnitId").isNotNull())
    .dropDuplicates(["BusinessUnitId"])
)
dim_business_unit = add_key(dim_business_unit, "BusinessUnitKey", ["BusinessUnitId"])
unknown_bu = spark.createDataFrame(
    [(0, -1, "UNKNOWN", "Unknown Business Unit", None, None, False, None, None)],
    "BusinessUnitKey long, BusinessUnitId long, BusinessUnitCode string, BusinessUnitName string, EnterpriseId long, TypeId long, IsActive boolean, CreatedOn timestamp, UpdatedOn timestamp"
)
dim_business_unit = unknown_bu.unionByName(dim_business_unit.select(unknown_bu.columns))
save_table(dim_business_unit, "Dim_BusinessUnit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. DIM_ORGANISATION
dim_organisation = (
    stg_org
    .select(
        F.col("Id").cast("long").alias("OrganisationId"),
        F.col("BusinessUnitId").cast("long").alias("BusinessUnitId"),
        F.col("Code").cast("string").alias("OrganisationCode"),
        F.col("Name").cast("string").alias("OrganisationName"),
        F.col("TypeId").cast("long").alias("TypeId"),
        F.col("EnterpriseId").cast("long").alias("EnterpriseId"),
        F.col("IsActive").cast("boolean").alias("IsActive"),
        F.col("CreatedOn").cast("timestamp").alias("CreatedOn"),
        F.col("UpdatedOn").cast("timestamp").alias("UpdatedOn")
    )
    .filter(F.col("OrganisationId").isNotNull())
    .dropDuplicates(["BusinessUnitId", "OrganisationId"])
)
dim_organisation = add_key(dim_organisation, "OrganisationKey", ["BusinessUnitId", "OrganisationId"])
dim_organisation = (
    dim_organisation
    .join(dim_business_unit.select("BusinessUnitId", "BusinessUnitKey"), "BusinessUnitId", "left")
    .fillna({"BusinessUnitKey": 0})
)
unknown_org = spark.createDataFrame(
    [(0, -1, -1, 0, "UNKNOWN", "Unknown Organisation", None, None, False, None, None)],
    "OrganisationKey long, OrganisationId long, BusinessUnitId long, BusinessUnitKey long, OrganisationCode string, OrganisationName string, TypeId long, EnterpriseId long, IsActive boolean, CreatedOn timestamp, UpdatedOn timestamp"
)
dim_organisation = unknown_org.unionByName(dim_organisation.select(unknown_org.columns))
save_table(dim_organisation, "Dim_Organisation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dbo.Dim_Attendee")
spark.sql("DROP TABLE IF EXISTS Dim_Attendee")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

tables_to_drop = [
    "dbo.Fact_Attendance",
    "dbo.Fact_Headcount",
    "dbo.Fact_Enrolment",
    "dbo.Dim_Attendee"
]

for table_name in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropped: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES IN dbo").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_attendee = spark.table("dbo.stg_Attendee")
stg_enrolment = spark.table("dbo.stg_Enrollment")
stg_attendance = spark.table("dbo.stg_Attendance")
stg_headcount = spark.table("dbo.stg_HeadCount")

dim_business_unit = spark.table("dbo.Dim_BusinessUnit")
dim_organisation = spark.table("dbo.Dim_Organisation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_key(df, key_name, order_columns):
    key_window = Window.orderBy(
        *[
            F.col(column_name).asc_nulls_last()
            for column_name in order_columns
        ]
    )

    return df.withColumn(
        key_name,
        F.row_number().over(key_window).cast("long")
    )


def save_table(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Created {table_name}: {df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

attendee_source = (
    attendee_source
    .withColumn(
        "DateOfBirth",
        F.when(
            (F.col("RawDateOfBirth") >= F.to_date(F.lit("1900-01-01")))
            & (F.col("RawDateOfBirth") <= F.current_date()),
            F.col("RawDateOfBirth")
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

attendee_source = (
    attendee_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(attendee_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_attendee = add_key(
    attendee_source,
    "AttendeeKey",
    ["BusinessUnitId", "AttendeeId"]
)

dim_attendee = (
    dim_attendee
    .join(
        dim_business_unit.select(
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

unknown_attendee = spark.createDataFrame(
    [(
        0,
        -1,
        0,
        -1,
        None,
        None,
        None,
        None,
        None,
        False,
        None,
        None
    )],
    """
    AttendeeKey long,
    AttendeeId long,
    BusinessUnitKey long,
    BusinessUnitId long,
    AccountId long,
    EnterpriseId long,
    GenderId long,
    DateOfBirth date,
    StatusId long,
    IsActive boolean,
    CreatedOn timestamp,
    UpdatedOn timestamp
    """
)

dim_attendee = unknown_attendee.unionByName(dim_attendee)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table(
    dim_attendee,
    "dbo.Dim_Attendee"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

enrolment_source = (
    stg_enrolment
    .select(
        F.col("Id").cast("long").alias("EnrolmentId"),
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
        F.col("EnrolmentId").isNotNull()
        & F.col("BusinessUnitId").isNotNull()
    )
)

fact_enrolment = add_key(
    enrolment_source,
    "EnrolmentFactKey",
    ["BusinessUnitId", "EnrolmentId"]
)

fact_enrolment = (
    fact_enrolment
    .join(
        dim_business_unit.select(
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
        "EnrolmentCount",
        F.lit(1)
    )
)

fact_enrolment = fact_enrolment.select(
    "EnrolmentFactKey",
    "EnrolmentId",
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
    "EnrolmentCount",
    "CreatedOn",
    "UpdatedOn"
)

save_table(
    fact_enrolment,
    "dbo.Fact_Enrolment"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

attendance_source = (
    stg_attendance
    .select(
        F.col("Id").cast("long").alias("AttendanceId"),
        F.col("EnrollmentId").cast("long").alias("EnrolmentId"),
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

attendance_window = (
    Window
    .partitionBy("AttendanceId")
    .orderBy(
        F.col("IsActive").desc_nulls_last(),
        F.col("UpdatedOn").desc_nulls_last(),
        F.col("CreatedOn").desc_nulls_last()
    )
)

attendance_source = (
    attendance_source
    .withColumn(
        "_RowNumber",
        F.row_number().over(attendance_window)
    )
    .filter(F.col("_RowNumber") == 1)
    .drop("_RowNumber")
)

enrolment_lookup = (
    fact_enrolment
    .select(
        "EnrolmentId",
        "BusinessUnitId",
        "EnrolmentFactKey"
    )
    .dropDuplicates(["EnrolmentId"])
)

fact_attendance = (
    attendance_source.alias("attendance")
    .join(
        enrolment_lookup.alias("enrolment"),
        F.col("attendance.EnrolmentId")
        == F.col("enrolment.EnrolmentId"),
        how="left"
    )
    .select(
        "attendance.*",
        F.col("enrolment.BusinessUnitId").alias("BusinessUnitId"),
        F.col("enrolment.EnrolmentFactKey").alias("EnrolmentFactKey")
    )
)

fact_attendance = add_key(
    fact_attendance,
    "AttendanceFactKey",
    ["AttendanceId"]
)

fact_attendance = (
    fact_attendance
    .join(
        dim_business_unit.select(
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
    "EnrolmentFactKey",
    "BusinessUnitKey",
    "OrganisationKey",
    "AttendeeKey",
    "EnrolmentId",
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

headcount_source = (
    stg_headcount
    .select(
        F.col("Id").cast("long").alias("HeadcountId"),
        F.col("OrgId").cast("long").alias("OrganisationId"),
        F.col("TermProgramSetId").cast("long").alias("TermProgramSetId"),
        F.col("TermProductId").cast("long").alias("TermProductId"),
        F.col("Description").cast("string").alias("Description"),
        F.col("HeadcountType").cast("string").alias("HeadcountType"),
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
        dim_business_unit.select(
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

# CELL ********************

model_tables = [
    "dbo.Dim_Attendee",
    "dbo.Fact_Enrolment",
    "dbo.Fact_Attendance",
    "dbo.Fact_Headcount"
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

# CELL ********************

print("Enrolments without matched attendees:")

spark.table("dbo.Fact_Enrolment").filter(
    F.col("AttendeeKey") == 0
).select(
    "EnrolmentId",
    "BusinessUnitId",
    "AttendeeId",
    "AccountId"
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Attendance without matched enrolments:")

spark.table("dbo.Fact_Attendance").filter(
    F.col("EnrolmentFactKey").isNull()
).select(
    "AttendanceId",
    "EnrolmentId",
    "AttendeeId",
    "OrganisationId"
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
