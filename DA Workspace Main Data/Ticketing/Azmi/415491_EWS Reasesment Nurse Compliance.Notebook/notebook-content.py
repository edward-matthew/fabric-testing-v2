# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e4b85d50-26cd-4a72-8fc7-7baea3cd64bd",
# META       "default_lakehouse_name": "lakehouse_da",
# META       "default_lakehouse_workspace_id": "6a9ddf8c-c2db-4770-b750-e6a34e2c1ef2",
# META       "known_lakehouses": [
# META         {
# META           "id": "e4b85d50-26cd-4a72-8fc7-7baea3cd64bd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import sempy.fabric as fabric
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
from pyspark.sql.functions import col, when, min, concat_ws, lit, current_timestamp
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_name = "lakehouse_da"
workspace_id = fabric.resolve_workspace_id("DEV_DATA_ANALYST")
lakehouse_id = mssparkutils.lakehouse.get(lakehouse_name, workspace_id).id
print(f"workspace_id: {workspace_id}")
print(f"lakehouse_id: {lakehouse_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_string = """
    WITH Admission AS (
    SELECT
        ipd.organization_id, 
        ipd.admission_id, 
        ipd.admission_no, 
        adm.AdmissionDate AS admission_date, 
        ipd.patient_id, 
        ipd.patient_name,
        pat.MrNo AS LocalMrNo, 
        ipd.ward_id, 
        wrd.Name AS WardName, 
        org.Name AS OrganizationName,
        org.Code AS OrganizationCode
    FROM emr_his_ipd_activity_dbo.TR_patient_ipd AS ipd
    INNER JOIN his_reporting_sales.Ward AS wrd 
        ON wrd.WardId = ipd.ward_id
    INNER JOIN his_reporting_system.Organization AS org 
        ON org.OrganizationId = ipd.organization_id 
    INNER JOIN his_reporting_patient.Patient AS pat
        ON pat.PatientId = ipd.patient_id
    INNER JOIN his_reporting_patienttrx.Admission AS adm
        ON adm.AdmissionId = ipd.admission_id
    WHERE admission_date BETWEEN '2025-07-01' AND '2025-10-31'
    -- AND ipd.is_active = 1   -- aktifkan bila ingin hanya pasien aktif
),

ActivityForm AS (
    SELECT 
        adm.organization_id,
        adm.admission_id,
        adm.admission_no,
        adm.admission_date,
        adm.patient_id,
        adm.patient_name,
        adm.ward_id,
        adm.WardName,
        adm.OrganizationName,
        adm.OrganizationCode,
        adm.MrNo,
        taf.activity_id,
        taf.activity_form_id,
        taf.form_item_id,
        taf.form_option_id,
        taf.remarks,
        taf.created_date,
        taf.created_by,
        scd.finished_date,
        usr.Name AS NurseName
    FROM Admission AS adm
    LEFT JOIN emr_his_ipd_activity_dbo.TR_activity_form AS taf
        ON adm.organization_id = taf.organization_id
        AND adm.admission_id = taf.admission_id
        AND adm.patient_id = taf.patient_id
        -- ward_id tidak selalu sama (pasien bisa pindah kamar)
        AND adm.ward_id = taf.ward_id
    LEFT JOIN emr_his_ipd_activity_dbo.TR_activity AS act
        ON act.activity_id = taf.activity_id
    LEFT JOIN emr_his_ipd_activity_dbo.TR_schedule AS scd
        ON scd.schedule_id = act.schedule_id
    INNER JOIN his_reporting_security.User AS usr
        ON usr.UserId = taf.created_by
    WHERE taf.form_item_id IN (34, 133)
),

ActivityFormLatest AS (
    SELECT 
        af.*,
        ROW_NUMBER() OVER (
            PARTITION BY 
                af.organization_id, 
                af.admission_id, 
                af.patient_id, 
                af.form_item_id
            ORDER BY af.created_date DESC
        ) AS rn
    FROM ActivityForm AS af
),

EarlyWarningScore AS (
    SELECT 
        afl.organization_id,
        afl.admission_id,
        afl.patient_id,
        afl.form_item_id AS EWS_form_item_id,
        afl.remarks AS EWS_Score
    FROM ActivityFormLatest AS afl
    WHERE afl.form_item_id = 133
    AND afl.rn = 1
)

SELECT 
    vtl.admission_id,
    vtl.admission_no,
    vtl.admission_date,
    vtl.patient_name,
    vtl.LocalMrNo,
    vtl.WardName,
    vtl.OrganizationName,
    vtl.OrganizationCode,
    -- vtl.form_item_id,
    -- vtl.form_option_id,
    afo.form_option_name, 
    -- ews.EWS_form_item_id,
    ews.EWS_Score,
	dct.LocalName AS DischargeType, 
    vtl.created_date AS VitalSignCreatedDate,
    vtl.finished_date AS TaskCompleteDate,
    vtl.NurseName
FROM ActivityFormLatest AS vtl
INNER JOIN EarlyWarningScore AS ews 
    ON ews.admission_id = vtl.admission_id
INNER JOIN emr_his_ipd_activity_dbo.MS_ATD_form_item AS afi 
    ON afi.form_item_id = vtl.form_item_id
INNER JOIN emr_his_ipd_activity_dbo.MS_ATD_form_option AS afo  
    ON afo.form_option_id = vtl.form_option_id
INNER JOIN his_reporting_patienttrx.Discharge AS dc
	ON dc.AdmissionId = vtl.admission_id
INNER JOIN his_reporting_patient.DischargeType AS dct
	ON dct.DischargeTypeId = dc.DischargeTypeId
WHERE vtl.form_item_id = 34
  AND vtl.rn = 1
  AND vtl.form_option_id IN (37, 38, 39) -- 37 (EWS), 38 (MEWS), 39 (PEWS)
ORDER BY vtl.admission_date DESC
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Intervention Task Created date ? :
# vital sign created datetime ? : tr_cativity_from created_date
# task completed date ? : activity_id ke tr_activity : ada schedule id, bisa ke tr_schedule bisa (tfinish date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_save = spark.sql(query_string)
pandas_df = df_save.toPandas()
pandas_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pandas_df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pandas_df.to_excel(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/download/EWS_Jul_Oct_2025.xlsx", index=False)

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
