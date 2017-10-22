CREATE TABLE org(sno int, dates string, startupName string, industryVertical string, subVertical string, cityLocation string, investorsName string, investmentType string, amountInUSD string, remarks string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hduser/Documents/datasets/Indian Startups/startup_funding.csv' INTO TABLE org;

CREATE TABLE data (sno int, year string, startupName string, industryVertical string, subVertical string, cityLocation string, investorsName string, investmentType string, amountInUSD bigint, remarks string);

INSERT OVERWRITE TABLE data
SELECT sno,split(dates,"/")[2],startupName,industryVertical,subVertical,cityLocation,investorsName,investmentType,cast(REGEXP_REPLACE(amountInUSD,",","") as bigint),remarks from org; 


--1. Highest and least amount invested in which industry verticals every year.

SELECT * FROM (SELECT *,row_number() OVER (PARTITION BY a.year ORDER BY a.total DESC) AS rn FROM (SELECT industryVertical AS ind,year,sum(amountinUSD) AS total FROM data WHERE amountinUSD is not NULL AND industryVertical != "" AND year is not NULL AND year != "" GROUP BY industryVertical,year) a) b where b.rn=1;

SELECT * FROM (SELECT *,row_number() OVER (PARTITION BY a.year ORDER BY a.total ASC) AS rn FROM (SELECT industryVertical AS ind,year,sum(amountinUSD) AS total FROM data WHERE amountinUSD is not NULL AND industryVertical != "" AND year is not NULL AND year != "" GROUP BY industryVertical,year) a) b where b.rn=1;


--2. Highest and least amount invested in which industry verticals.

--SELECT industryVertical AS ind,sum(amountinUSD) AS total FROM data WHERE amountinUSD is not NULL AND industryVertical != "" GROUP BY industryVertical ORDER BY total desc LIMIT 1;

--SELECT industryVertical AS ind,sum(amountinUSD) AS total FROM data WHERE amountinUSD is not NULL AND industryVertical != "" GROUP BY industryVertical ORDER BY total LIMIT 1;

--3. Percentage & Number of startups location wise.

select citylocation,count(*)/a.total*100 from (select count(*)-179 as total from data) a, data where citylocation is not null group by citylocation,total;

--4. Industry with highest investment.

select industryvertical,sum(amountinusd) as total from data group by industryvertical order by total desc limit 1;


--5. Company and its industry details for which a largest individual investment happened per year.

select year,startupname,industryvertical,amountinusd from data a,(select year,max(amountinusd) as mx from data where year is not NULL AND year != "" group by year) b where a.amountinusd=b.mx and a.year=b.year group by a.year,a.startupname,a.industryvertical,a.amountinusd;


--13. Top 10 investors for funding.

create table investors(name string)
row format delimited
fields terminated by ',';

insert overwrite table investors
select UPPER(investorsname) from data;

select name,count(*) as cnt from investors group by name order by cnt desc limit 10; 

--8. Location wise best startup industry(based on number).

select * from(select *,row_number() over (partition by a.loc order by a.cnt desc) as rn from (select citylocation as loc,industryvertical as ind,count(industryvertical) as cnt from data where industryVertical != "" group by citylocation,industryvertical) a) b where b.rn=1;  

--15. Locations in which big data industries are present.

select citylocation,industryvertical,subvertical from data where UPPER(industryvertical) like '%BIG%' or UPPER(subvertical) like '%BIG%' group by citylocation,industryvertical,subvertical;

--10. What was the most preferred investment type for investors in each industry.

select ind,type,sum(cnt) from (select UPPER(industryvertical) as ind,REGEXP_REPLACE(UPPER(investmenttype),' ','') as type,count(*) as cnt from data where investmenttype != '' group by investmenttype,industryvertical) a group by ind,type;  

select UPPER(industryvertical) as ind,REGEXP_REPLACE(UPPER(investmenttype),' ','') as type,count(*) as cnt from data where investmenttype != '' group by investmenttype,industryvertical

--16. Avg funds raised for startups in each year.

select year,AVG(amountinUSD) from data where year is not NULL AND year != "" AND amountinUSD is not NULL group by year;
