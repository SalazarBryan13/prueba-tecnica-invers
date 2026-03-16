
-- 1. Insertar datos en las dimensiones

INSERT INTO dim_patient (name, age, gender, blood_type)
SELECT DISTINCT "Name", "Age", "Gender", "Blood Type"
FROM stg_healthcare
WHERE NOT EXISTS (
    SELECT 1 FROM dim_patient dp
    WHERE dp.name = stg_healthcare."Name" 
      AND dp.age = stg_healthcare."Age" 
      AND dp.gender = stg_healthcare."Gender" 
      AND dp.blood_type = stg_healthcare."Blood Type"
);

INSERT INTO dim_doctor (doctor_name)
SELECT DISTINCT "Doctor" FROM stg_healthcare
WHERE "Doctor" NOT IN (SELECT doctor_name FROM dim_doctor);

INSERT INTO dim_hospital (hospital_name)
SELECT DISTINCT "Hospital" FROM stg_healthcare
WHERE "Hospital" NOT IN (SELECT hospital_name FROM dim_hospital);

INSERT INTO dim_insurance (provider_name)
SELECT DISTINCT "Insurance Provider" FROM stg_healthcare
WHERE "Insurance Provider" NOT IN (SELECT provider_name FROM dim_insurance);

INSERT INTO dim_condition (condition_name)
SELECT DISTINCT "Medical Condition" FROM stg_healthcare
WHERE "Medical Condition" NOT IN (SELECT condition_name FROM dim_condition);

INSERT INTO dim_medication (medication_name)
SELECT DISTINCT "Medication" FROM stg_healthcare
WHERE "Medication" NOT IN (SELECT medication_name FROM dim_medication);

INSERT INTO dim_date (date_id, full_date, year, month, day, quarter)
SELECT DISTINCT 
    CAST(TO_CHAR(CAST("Date of Admission" AS DATE), 'YYYYMMDD') AS INT) as date_id,
    CAST("Date of Admission" AS DATE) as full_date,
    EXTRACT(YEAR FROM CAST("Date of Admission" AS DATE)) as year,
    EXTRACT(MONTH FROM CAST("Date of Admission" AS DATE)) as month,
    EXTRACT(DAY FROM CAST("Date of Admission" AS DATE)) as day,
    EXTRACT(QUARTER FROM CAST("Date of Admission" AS DATE)) as quarter
FROM stg_healthcare
WHERE CAST(TO_CHAR(CAST("Date of Admission" AS DATE), 'YYYYMMDD') AS INT) NOT IN (SELECT date_id FROM dim_date)
UNION
SELECT DISTINCT 
    CAST(TO_CHAR(CAST("Discharge Date" AS DATE), 'YYYYMMDD') AS INT) as date_id,
    CAST("Discharge Date" AS DATE) as full_date,
    EXTRACT(YEAR FROM CAST("Discharge Date" AS DATE)) as year,
    EXTRACT(MONTH FROM CAST("Discharge Date" AS DATE)) as month,
    EXTRACT(DAY FROM CAST("Discharge Date" AS DATE)) as day,
    EXTRACT(QUARTER FROM CAST("Discharge Date" AS DATE)) as quarter
FROM stg_healthcare
WHERE CAST(TO_CHAR(CAST("Discharge Date" AS DATE), 'YYYYMMDD') AS INT) NOT IN (SELECT date_id FROM dim_date);


-- 2. Insertar datos en la tabla de hechos
INSERT INTO fact_admission (
    patient_id, doctor_id, hospital_id, insurance_id, condition_id, medication_id,
    admission_date_id, discharge_date_id, room_number, admission_type, test_results, billing_amount
)
SELECT 
    dp.patient_id,
    dd.doctor_id,
    dh.hospital_id,
    di.insurance_id,
    dc.condition_id,
    dm.medication_id,
    CAST(TO_CHAR(CAST(stg."Date of Admission" AS DATE), 'YYYYMMDD') AS INT) AS admission_date_id,
    CAST(TO_CHAR(CAST(stg."Discharge Date" AS DATE), 'YYYYMMDD') AS INT) AS discharge_date_id,
    stg."Room Number" AS room_number,
    stg."Admission Type" AS admission_type,
    stg."Test Results" AS test_results,
    stg."Billing Amount" AS billing_amount
FROM stg_healthcare stg
LEFT JOIN dim_patient dp 
    ON stg."Name" = dp.name AND stg."Age" = dp.age AND stg."Gender" = dp.gender AND stg."Blood Type" = dp.blood_type
LEFT JOIN dim_doctor dd ON stg."Doctor" = dd.doctor_name
LEFT JOIN dim_hospital dh ON stg."Hospital" = dh.hospital_name
LEFT JOIN dim_insurance di ON stg."Insurance Provider" = di.provider_name
LEFT JOIN dim_condition dc ON stg."Medical Condition" = dc.condition_name
LEFT JOIN dim_medication dm ON stg."Medication" = dm.medication_name;


