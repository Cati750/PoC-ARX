CREATE DATABASE POC_ARX;
USE POC_ARX;

CREATE TABLE Hospitals (
    hospital_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE Doctors (
    cedula_medica INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE InsuranceProviders (
    provider_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE Patients (
    patient_NIF VARCHAR(9) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
   race VARCHAR(255) NOT NULL,
    birth_date DATE NOT NULL,
    gender ENUM('Male', 'Female', 'Other') NOT NULL,
    blood_type ENUM('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-') NOT NULL,
    contact_info TEXT
);

CREATE TABLE Diagnosis (
    diagnosis INT PRIMARY KEY,
    patient_NIF VARCHAR(9),
    medical_condition VARCHAR(255) NOT NULL,
    date_of_admission DATE NOT NULL,
    cedula_medica INT,
    hospital_id INT,
    insurance_provider_id INT,
    billing_amount DECIMAL(10,2) CHECK (billing_amount >= 0),

    FOREIGN KEY (cedula_medica) REFERENCES Doctors(cedula_medica),
    FOREIGN KEY (hospital_id) REFERENCES Hospitals(hospital_id),
    FOREIGN KEY (insurance_provider_id) REFERENCES InsuranceProviders(provider_id),
    FOREIGN KEY (patient_NIF) REFERENCES Patients(patient_NIF)
);


INSERT INTO Hospitals (name) VALUES
('MercyCare Hospital'),
('GlobalMed Hospital'),
('United Health Hospital'),
('Hopewell Hospital'),
('Starlight Hospital');


INSERT INTO Doctors (cedula_medica, name) VALUES
(100001, 'Dr. John Smith'),
(100002, 'Dr. Emily Davis'),
(100003, 'Dr. Michael Brown'),
(100004, 'Dr. Sarah Wilson'),
(100005, 'Dr. David Lee');


INSERT INTO InsuranceProviders (name) VALUES
('CarePlus Insurance'),
('SafeHealth Insurance'),
('MedShield Insurance'),
('LifeSecure Insurance'),
('BrightCare Insurance');

INSERT INTO Patients (patient_NIF, name, birth_date, race, gender, blood_type, contact_info) VALUES
('266944844', 'Michele Williams', '1955-06-10', 'Caucasian', 'Other', 'A-', '001-413-164-7525x53419'),
('853041955', 'Kimberly Sanchez', '1987-05-04', 'African American', 'Male', 'AB+', '(483)503-0564x1395'),
('966647391', 'Jonathan Wilkerson', '2011-12-01', 'Asian', 'Male', 'B-', '242.388.4969x653'),
('506448196', 'Brenda Snyder PhD', '1939-05-07', 'Caucasian', 'Male', 'A-', '(269)166-9784'),
('774996843', 'Shannon Smith', '2022-07-07', 'African American', 'Other', 'A-', '845-146-2704'),
('914763202', 'Pamela Romero', '1938-11-27', 'Asian', 'Other', 'O+', '+1-489-325-2880x9570'),
('203848421', 'Jack Galloway', '1966-04-11', 'Caucasian', 'Male', 'A+', '039.117.1822'),
('692749116', 'Theresa Martin', '1996-03-28', 'African American', 'Male', 'B-', '+1-638-346-5787x133'),
('746412689', 'Alex Spencer', '2004-07-05', 'Asian', 'Male', 'A+', '010.310.5183'),
('685582861', 'John Ryan', '2022-11-15', 'Caucasian', 'Other', 'B-', '001-973-763-1165'),
('199585092', 'John Daniel', '2016-01-26', 'African American', 'Other', 'O+', '+1-106-513-3387x2624'),
('485451171', 'Shannon Jones', '1999-04-28', 'Asian', 'Male', 'O-', '080-132-6773'),
('219540831', 'Stephanie Martin', '1924-09-19', 'Caucasian', 'Other', 'AB+', '(474)687-2343x0980'),
('797808098', 'Amy Silva', '1992-11-10', 'African American', 'Male', 'B+', '208.121.9136x19399'),
('969119330', 'Bradley Sandoval', '2001-05-08', 'Asian', 'Other', 'O+', '543.534.6247x51079'),
('331191390', 'Dr. Angel Lewis MD', '1971-12-04', 'Caucasian', 'Female', 'AB+', '354-278-4980'),
('642621108', 'Kristen Huynh', '1938-07-10', 'African American', 'Male', 'B-', '(411)824-4935'),
('209747451', 'Mark Baker', '1990-11-30', 'Asian', 'Female', 'A-', '016-400-5242x786'),
('313500298', 'Ashley Landry', '2018-06-21', 'Caucasian', 'Male', 'O+', '(805)982-6204'),
('582334538', 'Timothy Stanton', '1929-11-08', 'African American', 'Male', 'AB-', '(331)586-9232x260'),
('990566476', 'Tim Patton', '1980-06-16', 'Asian', 'Female', 'AB+', '+1-342-160-7337x54330'),
('398704996', 'Jennifer Ramirez', '1933-11-14', 'Caucasian', 'Male', 'O-', '001-458-685-0142x94019'),
('398362082', 'Nicholas Edwards', '2006-12-05', 'African American', 'Other', 'A-', '169.340.6088x35615'),
('193349856', 'Mary Martinez', '1965-04-19', 'Asian', 'Female', 'A-', '465.648.2366x29946'),
('249827706', 'April Mitchell', '1982-12-06', 'Caucasian', 'Other', 'AB+', '001-957-773-8721'),
('134126396', 'Matthew Harvey', '1937-02-18', 'African American', 'Other', 'AB-', '001-343-320-0379x17693'),
('339670711', 'Janet Ross', '1944-07-04', 'Asian', 'Other', 'B-', '+1-016-328-7083x1727'),
('334760738', 'Victoria Larson', '1984-08-28', 'Caucasian', 'Other', 'A-', '+1-868-727-7434x873'),
('868820204', 'Billy Mitchell', '1961-05-28', 'African American', 'Male', 'B-', '581-223-6231x66587'),
('146654552', 'Daniel Fisher', '2003-12-01', 'Asian', 'Female', 'A-', '001-096-705-4668x893'),
('849621470', 'Don Tucker MD', '2015-06-16', 'Caucasian', 'Male', 'A-', '+1-627-298-0699x01627'),
('200604502', 'David Murphy', '1986-04-23', 'African American', 'Female', 'AB+', '564-641-7080x53100'),
('106977991', 'Christina Smith', '1945-02-28', 'Asian', 'Female', 'AB-', '271.937.4529'),
('414797776', 'Nicole Wilson', '1946-09-17', 'Caucasian', 'Male', 'AB-', '190-496-6319x3149'),
('488302652', 'Rebecca Knight', '1930-01-05', 'African American', 'Female', 'B-', '(865)185-0671x657'),
('593303705', 'Jessica Gross', '2012-12-13', 'Asian', 'Other', 'AB+', '987-769-4531x473'),
('550455977', 'Laura Barnett', '1970-03-17', 'Caucasian', 'Other', 'A-', '7527354549'),
('786579303', 'Desiree Smith', '1950-06-05', 'African American', 'Other', 'B+', '367-837-7701'),
('210053353', 'Jared Sanchez', '2014-10-15', 'Asian', 'Other', 'B-', '001-578-856-8557'),
('734036506', 'David Martinez', '1950-08-20', 'Caucasian', 'Male', 'O-', '182-337-4989x41343'),
('336696312', 'Eric Erickson', '1941-08-26', 'African American', 'Female', 'AB+', '008-427-1094x777'),
('896233790', 'Vincent King', '1989-06-11', 'Asian', 'Other', 'B-', '167-190-2294'),
('461415646', 'David Grant', '1981-01-24', 'Caucasian', 'Other', 'AB-', '+1-993-867-7496x499'),
('748245888', 'Tyler Garcia', '1952-09-14', 'African American', 'Male', 'B-', '+1-341-232-8120x6797'),
('184611066', 'David Mann', '2020-02-06', 'Asian', 'Male', 'AB-', '134.936.1832x421'),
('965241839', 'Ricky Larson', '2001-11-22', 'Caucasian', 'Female', 'AB+', '717-464-8877x1906'),
('826600539', 'Lindsey Johnson', '1955-07-31', 'African American', 'Male', 'B-', '+1-990-490-2787x42967'),
('126855092', 'Julie Ortiz', '1968-02-13', 'Asian', 'Other', 'AB-', '+1-125-674-6807x15451'),
('469319644', 'Jennifer Robertson', '1924-07-31', 'Caucasian', 'Male', 'O-', '+1-876-038-5977x03482'),
('384027113', 'Jessica Wolfe', '1940-12-13', 'African American', 'Female', 'O-', '9324808613'),
('764130526', 'Susan Murray MD', '1945-05-18', 'Asian', 'Male', 'AB+', '484.677.3782x6398'),
('349817734', 'Breanna Jones', '1980-11-23', 'Caucasian', 'Male', 'B-', '840-449-9727x87558'),
('234031070', 'Jamie Cantu', '1950-02-05', 'African American', 'Other', 'AB+', '001-396-360-5766x270'),
('465341213', 'Vincent Rivera', '1938-01-31', 'Asian', 'Other', 'O+', '187.026.2174x596'),
('919795579', 'Sarah Phelps', '1997-01-18', 'Caucasian', 'Other', 'O+', '(578)091-3431x6117'),
('553035110', 'Elijah Johnson', '1968-07-27', 'African American', 'Female', 'B-', '4556238692'),
('883543540', 'Bob Pitts', '2008-10-06', 'Asian', 'Male', 'O-', '+1-379-237-4740x74821'),
('395310485', 'Gregory Matthews', '2011-05-20', 'Caucasian', 'Male', 'A+', '(474)367-1369x59440'),
('362950628', 'Mary Hill', '2001-06-12', 'African American', 'Male', 'B+', '+1-097-439-5339x42104'),
('675770529', 'Amanda Diaz', '1942-04-23', 'Asian', 'Other', 'B+', '456-232-8588'),
('702632297', 'Sandra Scott', '1990-01-01', 'Caucasian', 'Other', 'O+', '517-123-6851x6048'),
('895285932', 'Jacob Curry', '1960-02-08', 'African American', 'Other', 'A-', '001-651-370-9859'),
('271432881', 'Arthur Colon', '2019-11-03', 'Asian', 'Female', 'O+', '612-004-7113x8267'),
('507943839', 'Travis Horton', '1997-12-11', 'Caucasian', 'Other', 'O-', '(926)179-6405x37735'),
('732719211', 'Michael Stephens', '2012-03-08', 'African American', 'Other', 'AB+', '(064)317-1390x053'),
('128492780', 'Rachel Harris DVM', '1999-11-07', 'Asian', 'Other', 'A+', '933.529.0422'),
('553778756', 'Rebecca Rodriguez', '1927-12-07', 'Caucasian', 'Other', 'A-', '(053)950-2402'),
('131994523', 'Kelly Reese', '1989-03-29', 'African American', 'Other', 'AB+', '589.178.3908x470'),
('685126461', 'John Morales', '2017-10-20', 'Asian', 'Other', 'AB-', '771-159-2124'),
('890779946', 'Lauren Joseph', '1976-04-06', 'Caucasian', 'Male', 'AB+', '+1-847-896-1183x673');

INSERT INTO Diagnosis (diagnosis, patient_NIF, medical_condition, date_of_admission, cedula_medica, hospital_id, insurance_provider_id, billing_amount)
VALUES
(1, '266944844', 'Migraine', '2024-09-28', 100001, 3, 5, 38121.91),
(2, '853041955', 'Hypertension', '2024-09-22', 100003, 5, 5, 10025.66),
(3, '966647391', 'Covid-19', '2024-11-09', 100002, 5, 5, 45932.83),
(4, '506448196', 'Covid-19', '2024-10-15', 100004, 1, 1, 46462.02),
(5, '774996843', 'Cancer', '2024-08-09', 100002, 1, 2, 43912.68),
(6, '914763202', 'Hypertension', '2023-06-24', 100001, 4, 1, 48901.43),
(7, '203848421', 'Asthma', '2024-09-19', 100002, 4, 5, 8339.84),
(8, '692749116', 'Fracture', '2025-05-25', 100002, 5, 2, 35676.15),
(9, '746412689', 'Fracture', '2024-10-26', 100003, 4, 5, 22629.16),
(10, '685582861', 'Heart Disease', '2025-03-09', 100002, 1, 3, 1149.61),
(11, '199585092', 'Heart Disease', '2024-11-23', 100005, 2, 1, 3642.55),
(12, '485451171', 'Diabetes', '2023-12-28', 100002, 1, 1, 42995.81),
(13, '219540831', 'Hypertension', '2023-06-26', 100005, 2, 3, 33481.99),
(14, '797808098', 'Heart Disease', '2023-10-22', 100005, 2, 5, 28852.96),
(15, '969119330', 'Heart Disease', '2024-01-02', 100004, 4, 2, 4806.86),
(16, '331191390', 'Fracture', '2024-11-30', 100003, 4, 4, 23404.53),
(17, '642621108', 'Diabetes', '2025-01-16', 100001, 1, 4, 36437.86),
(18, '209747451', 'Hypertension', '2024-01-29', 100002, 2, 2, 26860.66),
(19, '313500298', 'Asthma', '2025-05-19', 100004, 2, 3, 23185.14),
(20, '582334538', 'Hypertension', '2024-02-22', 100004, 5, 1, 2624.36),
(21, '990566476', 'Diabetes', '2024-03-02', 100001, 2, 2, 20380.06),
(22, '398704996', 'Migraine', '2023-06-08', 100002, 4, 1, 8315.39),
(23, '398362082', 'Diabetes', '2023-10-22', 100004, 3, 4, 14333.94),
(24, '193349856', 'Migraine', '2025-04-09', 100002, 2, 3, 10963.27),
(25, '249827706', 'Diabetes', '2023-09-23', 100005, 5, 1, 37423.98),
(26, '134126396', 'Diabetes', '2024-08-16', 100001, 5, 4, 25192.23),
(27, '339670711', 'Asthma', '2024-10-26', 100001, 5, 1, 42581.99),
(28, '334760738', 'Hypertension', '2024-04-10', 100005, 1, 2, 20248.4),
(29, '868820204', 'Heart Disease', '2023-07-28', 100005, 5, 1, 31007.14),
(30, '146654552', 'Fracture', '2023-09-22', 100005, 5, 5, 15886.86),
(31, '849621470', 'Cancer', '2024-12-30', 100002, 3, 2, 13354.16),
(32, '200604502', 'Asthma', '2024-10-07', 100003, 4, 3, 46458.01),
(33, '106977991', 'Hypertension', '2023-06-27', 100001, 4, 5, 49922.88),
(34, '414797776', 'Hypertension', '2023-08-14', 100001, 5, 2, 25343.65),
(35, '488302652', 'Asthma', '2025-01-03', 100003, 1, 2, 18539.4),
(36, '593303705', 'Asthma', '2024-07-20', 100004, 5, 3, 30622.72),
(37, '550455977', 'Diabetes', '2023-11-18', 100005, 3, 1, 46952.61),
(38, '786579303', 'Asthma', '2024-03-22', 100003, 1, 1, 37146.59),
(39, '210053353', 'Asthma', '2024-04-25', 100003, 3, 5, 10610.42),
(40, '734036506', 'Covid-19', '2024-05-21', 100002, 3, 5, 24477.74),
(41, '336696312', 'Diabetes', '2024-02-23', 100001, 4, 3, 2299.93),
(42, '896233790', 'Covid-19', '2023-10-04', 100002, 3, 2, 37087.42),
(43, '461415646', 'Fracture', '2024-03-17', 100005, 1, 1, 3854.67),
(44, '748245888', 'Asthma', '2024-02-01', 100005, 1, 3, 29167.23),
(45, '184611066', 'Asthma', '2024-12-15', 100004, 2, 1, 15482.09),
(46, '965241839', 'Diabetes', '2024-02-10', 100003, 2, 2, 33379.98),
(47, '826600539', 'Covid-19', '2025-01-27', 100005, 4, 5, 37498.93),
(48, '126855092', 'Heart Disease', '2024-08-17', 100002, 2, 4, 1336.84),
(49, '469319644', 'Covid-19', '2024-09-09', 100004, 2, 3, 8043.97),
(50, '384027113', 'Hypertension', '2023-08-08', 100004, 1, 4, 11199.44),
(51, '764130526', 'Migraine', '2025-04-20', 100003, 3, 2, 11223.89),
(52, '349817734', 'Heart Disease', '2023-07-14', 100004, 3, 3, 43231.21),
(53, '234031070', 'Cancer', '2023-10-02', 100003, 5, 4, 34007.95),
(54, '465341213', 'Covid-19', '2023-10-04', 100001, 1, 3, 9010.53),
(55, '919795579', 'Cancer', '2025-01-09', 100001, 1, 5, 21784.73),
(56, '553035110', 'Covid-19', '2024-09-25', 100004, 5, 5, 5870.72),
(57, '883543540', 'Heart Disease', '2023-07-12', 100003, 1, 4, 184.39),
(58, '395310485', 'Heart Disease', '2024-11-08', 100003, 4, 1, 47448.84),
(59, '362950628', 'Covid-19', '2023-08-06', 100005, 3, 1, 36015.74),
(60, '675770529', 'Cancer', '2024-01-01', 100005, 3, 4, 16376.53),
(61, '702632297', 'Cancer', '2024-05-13', 100005, 2, 2, 21080.27),
(62, '895285932', 'Fracture', '2024-10-30', 100002, 5, 5, 15117.4),
(63, '271432881', 'Diabetes', '2024-04-28', 100003, 3, 2, 21551.42),
(64, '507943839', 'Covid-19', '2024-06-15', 100004, 4, 4, 33813.8),
(65, '732719211', 'Migraine', '2024-09-17', 100002, 1, 3, 25821.06),
(66, '128492780', 'Covid-19', '2024-05-12', 100001, 2, 3, 11309.57),
(67, '553778756', 'Heart Disease', '2024-04-20', 100002, 1, 1, 12317.64),
(68, '131994523', 'Migraine', '2024-01-02', 100005, 1, 4, 20780.61),
(69, '685126461', 'Heart Disease', '2023-11-18', 100004, 4, 4, 12274.82),
(70, '890779946', 'Diabetes', '2025-01-10', 100001, 4, 2, 8876.75);