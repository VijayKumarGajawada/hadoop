--2 a) Which part of the US has the most Data Engineer jobs for each year?
  -- b) find top 5 locations in the US who have got certified visa for each year.[certified]

--a)
SELECT * 
FROM (SELECT *,row_number() OVER (PARTITION BY year ORDER BY cnt DESC) AS rn 
      FROM (SELECT year,job_title,worksite,COUNT(*) AS cnt 
            FROM h1b_edt 
            WHERE job_title = 'DATA ENGINEER' 
            GROUP BY year,job_title,worksite) a) b 
WHERE b.rn=1;

--------------
--b)
SELECT * 
FROM (SELECT *,row_number() OVER (PARTITION BY year ORDER BY cnt DESC) AS rn 
      FROM (SELECT year,case_status,worksite,COUNT(*) AS cnt 
            FROM h1b_edt WHERE case_status = 'CERTIFIED' 
            GROUP BY year,case_status,worksite) a) b 
WHERE b.rn<=5;

-------------------------------------------

--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]
--full time
SELECT *,row_number() OVER (PARTITION BY year ORDER BY avg DESC) AS rn 
FROM (SELECT year,case_status,job_title,SUM(prevailing_wage)/COUNT(job_title) AS avg 
      FROM h1b_edt 
      WHERE full_time='Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') 
      GROUP BY year,job_title,case_status) a;

--part time
SELECT *,row_number() OVER (PARTITION BY year ORDER BY avg DESC) AS rn 
FROM (SELECT year,case_status,job_title,SUM(prevailing_wage)/COUNT(job_title) AS avg 
      FROM h1b_edt 
      WHERE full_time='N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') 
      GROUP BY year,job_title,case_status) a;

--------------------------------------------

--9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?
 
SELECT * 
FROM (SELECT a.employer_name,COUNT(case_status),COUNT(case_status)/b.total*100 AS sucperc 
      FROM h1b_edt a 
      JOIN (SELECT employer_name,COUNT(case_status) AS total FROM h1b_edt GROUP BY employer_name) b 
      ON a.employer_name = b.employer_name 
      WHERE case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') AND b.total>=1000 
      GROUP BY a.employer_name,total) c 
WHERE sucperc>=70.0 
ORDER BY sucperc DESC;

---------------------------------------------

--10) Which are the job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)? 

SELECT * 
FROM (SELECT a.job_title,COUNT(case_status),COUNT(case_status)/b.total*100 AS sucperc 
      FROM h1b_edt a 
      JOIN (SELECT job_title,COUNT(case_status) AS total FROM h1b_edt GROUP BY job_title) b 
      ON a.job_title = b.job_title 
      WHERE case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') AND b.total>=1000
      GROUP BY a.job_title,total) c 
WHERE sucperc>=70.0 
ORDER BY sucperc DESC;
