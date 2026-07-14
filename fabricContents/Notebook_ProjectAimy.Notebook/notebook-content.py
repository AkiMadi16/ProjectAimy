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

# CELL ********************

# MAGIC %%sql
# MAGIC -- Welcome to your new notebook
# MAGIC --Type here in the cell editor to add code!
# MAGIC SELECT
# MAGIC     COUNT(*) AS AttendanceRows,
# MAGIC     COUNT(e.Id) AS MatchedEnrollmentRows
# MAGIC FROM stg_Attendance AS a
# MAGIC LEFT JOIN stg_Enrollment AS e
# MAGIC     ON e.Id = a.EnrollmentId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS AttendanceRows,
# MAGIC     COUNT(atd.Id) AS MatchedAttendeeRows
# MAGIC FROM stg_Attendance a
# MAGIC LEFT JOIN stg_Attendee atd
# MAGIC     ON atd.Id = a.AttendeeId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Enrollment to Attendee
# MAGIC SELECT
# MAGIC     COUNT(*) AS EnrollmentRows,
# MAGIC     SUM(CASE WHEN a.Id IS NOT NULL THEN 1 ELSE 0 END) AS MatchedRows,
# MAGIC     SUM(CASE WHEN a.Id IS NULL THEN 1 ELSE 0 END) AS UnmatchedRows
# MAGIC FROM stg_Enrollment e
# MAGIC LEFT JOIN stg_Attendee a
# MAGIC     ON a.Id = e.AttendeeId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Attendance to Enrollment
# MAGIC SELECT
# MAGIC     COUNT(*) AS AttendanceRows,
# MAGIC     SUM(CASE WHEN e.Id IS NOT NULL THEN 1 ELSE 0 END) AS MatchedRows,
# MAGIC     SUM(CASE WHEN e.Id IS NULL THEN 1 ELSE 0 END) AS UnmatchedRows
# MAGIC FROM stg_Attendance a
# MAGIC LEFT JOIN stg_Enrollment e
# MAGIC     ON e.Id = a.EnrollmentId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Attendance to Attendee
# MAGIC SELECT
# MAGIC     COUNT(*) AS AttendanceRows,
# MAGIC     SUM(CASE WHEN d.Id IS NOT NULL THEN 1 ELSE 0 END) AS MatchedRows,
# MAGIC     SUM(CASE WHEN d.Id IS NULL THEN 1 ELSE 0 END) AS UnmatchedRows
# MAGIC FROM stg_Attendance a
# MAGIC LEFT JOIN stg_Attendee d
# MAGIC     ON d.Id = a.AttendeeId;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
