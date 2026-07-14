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

# #### List all tables in the current Database - Python

# CELL ********************

spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import count, col

attendance_df = spark.table("stg_Attendance")
enrollment_df = spark.table("stg_Enrollment")

# Join Attendance with Enrollment
result_df = (
    attendance_df.alias("a")
    .join(
        enrollment_df.alias("e"),
        col("a.EnrollmentId") == col("e.Id"),
        "left"
    )
    .agg(
        count("*").alias("AttendanceRows"),
        count(col("e.Id")).alias("MatchedEnrollmentRows")
    )
)

display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

