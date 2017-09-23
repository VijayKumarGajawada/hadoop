--1 a) Is the number of petitions with Data Engineer job title increasing over time?
----b) Find top 5 job titles who are having highest avg growth in applications.[ALL]

--a)
data = LOAD 'hdfs://localhost:54310/user/project' USING PigStorage('\t') AS 
             (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, 
             job_title:chararray, full_time:chararray, prevailing_wage:int, 
             year:chararray, worksite:chararray, lon:double, lat:double);
only_de = FILTER data BY job_title == 'DATA ENGINEER'; 
title_year = FOREACH only_de GENERATE job_title,year;
all_years = GROUP title_year BY year;
year_counts = FOREACH all_years GENERATE group,COUNT(title_year) AS count;
for_2011 = FILTER year_counts BY group=='2011';
for_2012 = FILTER year_counts BY group=='2012';
for_2013 = FILTER year_counts BY group=='2013';
for_2014 = FILTER year_counts BY group=='2014';
for_2015 = FILTER year_counts BY group=='2015';
for_2016 = FILTER year_counts BY group=='2016';
growth_cycle = FOREACH for_2011 GENERATE (double)(for_2012.count-for_2011.count)/for_2011.count*100, 
                                         (double)(for_2013.count-for_2012.count)/for_2012.count*100,
                                         (double)(for_2014.count-for_2013.count)/for_2013.count*100,
                                         (double)(for_2015.count-for_2014.count)/for_2014.count*100,
                                         (double)(for_2016.count-for_2015.count)/for_2015.count*100;
dump growth_cycle;                                         


--b)
data = LOAD 'hdfs://localhost:54310/user/project' USING PigStorage('\t') AS 
             (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, 
             job_title:chararray, full_time:chararray, prevailing_wage:int, 
             year:chararray, worksite:chararray, lon:double, lat:double);
only_de = FILTER data BY job_title == 'DATA ENGINEER'; 
title_year = FOREACH only_de GENERATE job_title,year;
all_years = GROUP title_year BY year;
year_counts = FOREACH all_years GENERATE group,COUNT(title_year) AS count;
for_2011 = FILTER year_counts BY group=='2011';
for_2012 = FILTER year_counts BY group=='2012';
for_2013 = FILTER year_counts BY group=='2013';
for_2014 = FILTER year_counts BY group=='2014';
for_2015 = FILTER year_counts BY group=='2015';
for_2016 = FILTER year_counts BY group=='2016';
join_years = JOIN for_2011 BY job_title, 
                  for_2012 BY job_title, 
                  for_2013 BY job_title,
                  for_2014 BY job_title,
                  for_2015 BY job_title,
                  for_2016 BY job_title; 
only_job_counts = FOREACH join_years GENERATE $1,$2,$5,$8,$11,$14,$17; 
growth_cycle = FOREACH only_job_counts GENERATE $0,(double)($2-$1)/$1*100,
                                                   (double)($3-$2)/$2*100,
                                                   (double)($4-$3)/$3*100,
                                                   (double)($5-$4)/$4*100,
                                                   (double)($6-$5)/$5*100; 
avg_growth_cycle = FOREACH growth_cycle GENERATE $0,($1+$2+$3+$4+$5)/5 AS avg;
top5_jobs = LIMIT (ORDER avg_growth_cycle BY avg DESC) 5;
dump top5_jobs;


--7) Create a bar graph to depict the number of applications for each year [All] 

data = LOAD 'hdfs://localhost:54310/user/project' USING PigStorage('\t') AS 
             (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, 
             job_title:chararray, full_time:chararray, prevailing_wage:int, 
             year:chararray, worksite:chararray, lon:double, lat:double);
year_grp = GROUP data BY year;
year_count = FOREACH year_grp GENERATE $0,COUNT($1);
dump year_count;


--5) Which industry(SOC_NAME) has the most number of Data Scientist positions?[certified]

data = LOAD 'hdfs://localhost:54310/user/project' USING PigStorage('\t') AS 
             (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, 
             job_title:chararray, full_time:chararray, prevailing_wage:int, 
             year:chararray, worksite:chararray, lon:double, lat:double);
only_certi_DataSci = FILTER data BY (case_status == 'CERTIFIED') AND (job_title == 'DATA SCIENTIST');  
soc_ds = FOREACH only_certi_DataSci GENERATE soc_name,job_title;
soc_grp = GROUP soc_ds BY soc_name;
counts = FOREACH soc_grp GENERATE $0,COUNT($1) AS cnt;
top_soc = LIMIT (ORDER counts BY cnt DESC) 1;
dump top_soc;
