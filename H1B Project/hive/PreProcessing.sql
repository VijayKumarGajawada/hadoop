CREATE TABLE h1b_1(s_no int, case_status string, employer_name string, soc_name string, job_title string, full_time string, prevailing_wage int, year int, worksite string, lon double, lat double)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"seperatorChar" = ",",
"quoteChar" = "\""
)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/hduser/Downloads/h1b.csv' INTO TABLE h1b_1;
-------------------------------------

CREATE TABLE h1b_2(s_no int, case_status string, employer_name string, soc_name string, job_title string, full_time string, prevailing_wage int, year string, worksite string, lon double, lat double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE h1b_edt(s_no int, case_status string, employer_name string, soc_name string, job_title string, full_time string, prevailing_wage int, year string, worksite string, lon double, lat double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-------------------------------------

INSERT OVERWRITE TABLE h1b_2
SELECT TRIM(s_no), CASE
WHEN TRIM(case_status)='PENDING QUALITY AND COMPLIANCE REVIEW - UNASSIGNED' THEN 'DENIED'
WHEN TRIM(case_status)='REJECTED' THEN 'DENIED'
WHEN TRIM(case_status)='INVALIDATED' THEN 'DENIED' ELSE TRIM(case_status)
END, REGEXP_REPLACE(TRIM(employer_name),"\t",""), REGEXP_REPLACE(TRIM(soc_name),"\t",""), REGEXP_REPLACE(TRIM(job_title),"\t",""), REGEXP_REPLACE(TRIM(full_time),"\t",""), REGEXP_REPLACE(TRIM(prevailing_wage),"\t",""), REGEXP_REPLACE(TRIM(year),"\t",""), REGEXP_REPLACE(TRIM(worksite),"\t",""), REGEXP_REPLACE(TRIM(lon),"\t",""), REGEXP_REPLACE(TRIM(lat),"\t","") FROM h1b_1 WHERE TRIM(case_status)!='NA';


INSERT OVERWRITE TABLE h1b_edt
SELECT s_no,case_status,employer_name,soc_name,job_title,full_time,CASE WHEN prevailing_wage IS NULL THEN 100000 ELSE prevailing_wage END,year,worksite,lon,lat FROM h1b_2; 
