CREATE TABLE data(state string,year string,type_code string,type string,gender string,age_group string,total int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hduser/Documents/datasets/Suicides in India/Suicides in India 2001-2012.csv' INTO TABLE data;

CREATE TABLE info(state string,year string,type_code string,type string,gender string,age_group string,total int);

insert into info select * from data;


--1. Number of Male and Female suicides in each year.

select year,gender,sum(total) from info group by year,gender;

--2. Number of Male and Female suicides in each age group.

select age_group,gender,sum(total) as tot from info group by age_group,gender order by tot desc;


--3. Top 5 States in which most number of suicides took place for male and female.

select state,gender,sum(total) as tot from info where state not like "Total%" and gender="Male" group by state,gender order by tot desc limit 5;

select state,gender,sum(total) as tot from info where state not like "Total%" and gender="Female" group by state,gender order by tot desc limit 5;


--4. Top 10 types of suicides among males and females.

select type_code,type,gender,sum(total) as tot from info where gender="Male" group by type_code,type,gender order by tot desc limit 10;

select type_code,type,gender,sum(total) as tot from info where gender="Female" group by type_code,type,gender order by tot desc limit 10;


--5. Top 5 [causes, means_adopted] types of suicides amoung males and females.

select * from (select *,row_number() over (partition by a.type_code order by a.tot desc) as rn from (select type_code,type,gender,sum(total) as tot from info where type_code in ("Causes","Means_adopted") and LOWER(type) not REGEXP '^other|...other|^causes|^ot(.*)ca' and gender="Male" group by type_code,type,gender) a) b where b.rn<=5;

select * from (select *,row_number() over (partition by a.type_code order by a.tot desc) as rn from (select type_code,type,gender,sum(total) as tot from info where type_code in ("Causes","Means_adopted") and LOWER(type) not REGEXP '^other|...other|^causes|^ot(.*)ca' and gender="Female" group by type_code,type,gender) a) b where b.rn<=5;


--6. Age groups (with gender) who have committed most suicides on the account of physical abuse, love affairs, divorce,Illegitimate Pregnancy,Illness (Aids/STD),By Over Alcoholism,Poverty,drug abuse, dowry dispute, death of dear person.

select * from (select *,row_number() over (partition by a.type order by a.tot desc) as rn from (select type,age_group,gender,sum(total) as tot from info where LOWER(type) REGEXP 'physical|love|drug|illegit|illness|alcohol|poverty|divorce|dowry|death' and age_group != "0-100+" group by type,age_group,gender) a) b where b.rn=1;


--7. Top 10 reasons/types for children under 14 years commit suicide.

select type,age_group,sum(total) as tot from info where age_group = "0-14" and LOWER(type) not REGEXP '^other|...other|^causes|^ot(.*)ca' group by type,age_group,gender order by tot desc limit 10;


--8. Every state’s top 3 types of suicides.

select * from (select *,row_number() over (partition by a.state order by tot desc) as rn from (select state,type,sum(total) as tot from info where LOWER(type) not REGEXP '^other|...other|^causes|^ot(.*)ca|^marr' and state not like "Total%" group by state,type) a) b where rn<=3;


--9. Every year’s top 3 types of suicides.

select * from (select *,row_number() over (partition by a.year order by tot desc) as rn from (select year,type,sum(total) as tot from info where LOWER(type) not REGEXP '^other|...other|^causes|^ot(.*)ca|^marr' and type_code not REGEXP 'Edu|Soc' group by year,type) a) b where rn<=3;

--10. Every age group’s top 3 types of suicides.

select * from (select *,row_number() over (partition by a.age_group order by tot desc) as rn from (select age_group,type,sum(total) as tot from info where LOWER(type) not REGEXP '^other|...other|^causes|^ot(.*)ca|^marr' and type_code not REGEXP 'Edu|Soc' group by age_group,type) a) b where rn<=3;

--11. Top 7 states where farmers committed highest suicides.

select state,type,sum(total) as tot from info where type_code like 'Prof%' and type like 'Farming%' group by state,type order by tot desc limit 7;
