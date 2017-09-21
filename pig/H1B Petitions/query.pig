--1 a) Is the number of petitions with Data Engineer job title increasing over time?
----b) Find top 5 job titles who are having highest avg growth in applications.[ALL]

data = LOAD 'hdfs://localhost:54310/user/project' USING PigStorage('\t') AS 
             (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, 
             job_title:chararray, full_time:chararray, prevailing_wage:int, 
             year:chararray, worksite:chararray, lon:double, lat:double);
only_de = FILTER data BY job_title == 'DATA ENGINEER'; 
title_year = FOREACH only_de GENERATE job_title,year;
allyears = GROUP title_year BY year;
year_counts = FOREACH allyears GENERATE group,COUNT(title_year) AS count;
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
