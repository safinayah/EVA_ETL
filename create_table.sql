-- Create database
CREATE DATABASE medical_db;

-- Use the newly created database
USE medical_db;

-- Create the medical_records table
CREATE TABLE medical_records (
    patient_id INT AUTO_INCREMENT PRIMARY KEY,
    attribute_name VARCHAR(50),
    value VARCHAR(50),
    record_timestamp DATETIME
);

-- Insert sample data into the medical_records table
INSERT INTO medical_records (attribute_name, value, record_timestamp) VALUES
('Blood Type', 'O+', '2024-06-01 10:00:00'),
('Weight', '180', '2024-06-01 10:00:00'),
('Name', 'John Doe', '2024-06-01 10:00:00'),
('Age', '30', '2024-06-01 10:00:00'),
('Blood Type', 'A+', '2024-06-02 11:00:00'),
('Weight', '160', '2024-06-02 11:00:00'),
('Name', 'Jane Smith', '2024-06-02 11:00:00'),
('Age', '25', '2024-06-02 11:00:00');

-- Create the test_results table
CREATE TABLE test_results (
    test_id INT AUTO_INCREMENT PRIMARY KEY,
    BloodType VARCHAR(5),
    TestResult INT,
    test_timestamp DATETIME
);

-- Insert sample data into the test_results table
INSERT INTO test_results (BloodType, TestResult, test_timestamp) VALUES
('O+', 10, '2024-06-01 12:00:00'),
('O+', 11, '2024-06-01 12:00:00'),
('A+', 12, '2024-06-02 13:00:00'),
('A+', 13, '2024-06-02 13:00:00');
