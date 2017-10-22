sqoop export 
--connect jdbc:mysql://localhost/project 
--username root 
--password '' 
--table h1b 
--export-dir /user/hadoop/project10/10op/000000_0 
--input-fields-terminated-by '\t';

-----------------------------------------

INSERT OVERWRITE DIRECTORY '/user/hadoop/project10/10op'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * 
FROM (SELECT a.job_title,COUNT(case_status),COUNT(case_status)/b.total*100 AS sucperc 
      FROM h1b.h1b_edt a 
      JOIN (SELECT job_title,COUNT(case_status) AS total FROM h1b.h1b_edt GROUP BY job_title) b 
      ON a.job_title = b.job_title 
      WHERE case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') AND b.total>=1000
      GROUP BY a.job_title,total) c 
WHERE sucperc>=70.0 
ORDER BY sucperc DESC;
