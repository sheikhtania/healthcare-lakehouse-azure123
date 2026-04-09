CREATE OR REPLACE TABLE main.default.dim_department AS
SELECT DISTINCT
    monotonically_increasing_id() AS department_key,
    department
FROM main.default.silver_patient_clean;

CREATE OR REPLACE TABLE main.default.dim_date AS
SELECT DISTINCT
    date(admission_time) AS date,
    year(admission_time) AS year,
    month(admission_time) AS month,
    day(admission_time) AS day
FROM main.default.silver_patient_clean;

CREATE OR REPLACE TABLE main.default.dim_triage AS
SELECT DISTINCT
    monotonically_increasing_id() AS triage_key,
    triage_level
FROM main.default.silver_patient_clean;

CREATE OR REPLACE TABLE main.default.fact_patient_visits AS
SELECT
    patient_id,
    department,
    triage_level,
    admission_time,
    discharge_time,
    wait_minutes
FROM main.default.silver_patient_clean;

CREATE OR REPLACE TABLE main.default.fact_patient_visits AS
SELECT
    s.patient_id,
    d.department_key,
    t.triage_key,
    s.admission_time,
    s.discharge_time,
    s.wait_minutes
FROM main.default.silver_patient_clean s
JOIN main.default.dim_department d
    ON s.department = d.department
JOIN main.default.dim_triage t
    ON s.triage_level = t.triage_level;