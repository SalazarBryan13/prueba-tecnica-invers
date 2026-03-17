-- 1. DIMENSIONES
CREATE TABLE IF NOT EXISTS dim_patient (
    patient_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT,
    gender VARCHAR(50),
    blood_type VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_doctor (
    doctor_id SERIAL PRIMARY KEY,
    doctor_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_hospital (
    hospital_id SERIAL PRIMARY KEY,
    hospital_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_insurance (
    insurance_id SERIAL PRIMARY KEY,
    provider_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_condition (
    condition_id SERIAL PRIMARY KEY,
    condition_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_medication (
    medication_id SERIAL PRIMARY KEY,
    medication_name VARCHAR(100) UNIQUE NOT NULL
);

-- 2. TABLA DE HECHOS
CREATE TABLE IF NOT EXISTS fact_admission (
    admission_id SERIAL PRIMARY KEY,
    patient_id INT REFERENCES dim_patient(patient_id),
    doctor_id INT REFERENCES dim_doctor(doctor_id),
    hospital_id INT REFERENCES dim_hospital(hospital_id),
    insurance_id INT REFERENCES dim_insurance(insurance_id),
    condition_id INT REFERENCES dim_condition(condition_id),
    medication_id INT REFERENCES dim_medication(medication_id),
    admission_date_id INT REFERENCES dim_date(date_id),
    discharge_date_id INT REFERENCES dim_date(date_id),
    
    room_number INT,
    admission_type VARCHAR(50),
    test_results VARCHAR(50),
    
    billing_amount NUMERIC(12, 2) NOT NULL
);
