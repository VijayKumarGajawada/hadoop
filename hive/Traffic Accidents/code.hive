create table month
(state string, year int, type string, jan int, feb int, mar int, apr int, may int, june int, july int, aug int, sept int, oct int, nov int, dec int, tot int) row format delimited fields terminated by ",";

load data local inpath '/home/hduser/Documents/datasets/Traffic Accidents/month.csv' into table month;

create table time
(state string, year int, type string, 0to3 int, 3to6 int,6to9 int, 9to12 int, 12to15 int, 15to18 int, 18to21 int, 21to24 int, tot int) row format delimited fields terminated by ",";

load data local inpath '/home/hduser/Documents/datasets/Traffic Accidents/time.csv' into table time;

-------------------------------------

1. State in which highest number of (Road/ Rail-Road/ Other) accidents took place per year.

select b.year,b.type, state, a.tot 
from month a,
(select year,type, max(tot) as tot from month group by year,type) b 
where a.tot=b.tot;

-------------------------------------

2. State in which highest number of (ALL types) accidents took place per year.

select b.year, state, a.tot 
from month a,
(select year, max(tot) as tot from month group by year) b 
where a.tot=b.tot;

------------------------------------

3. Percentage of accidents took place in every state in each decade. (01-10, 11-14).

select state, sum(tot), round(sum(tot)/b.total*100,3) 
from month a,(select sum(tot) as total from month where year<2011) b 
where year<=2010 
group by state,b.total;

select state, sum(tot), round(sum(tot)/b.total*100,3)
from month a,(select sum(tot) as total from month where year>2011) b 
where year>2010 
group by state,b.total;
-------------------------------------

4. Top 3 Accident prone months for each state.

select jan, feb, mar, apr, may, june, july, aug, sept, oct, nov, dec  

create table monthstate(state string,month string,total int);
insert into table monthstate select state,"jan",sum(jan) from month group by state;
insert into table monthstate select state,"feb",sum(feb) from month group by state;
insert into table monthstate select state,"mar",sum(mar) from month group by state;
insert into table monthstate select state,"apr",sum(apr) from month group by state;
insert into table monthstate select state,"may",sum(may) from month group by state;
insert into table monthstate select state,"june",sum(june) from month group by state;
insert into table monthstate select state,"july",sum(july) from month group by state;
insert into table monthstate select state,"aug",sum(aug) from month group by state;
insert into table monthstate select state,"sept",sum(sept) from month group by state;
insert into table monthstate select state,"oct",sum(oct) from month group by state;
insert into table monthstate select state,"nov",sum(nov) from month group by state;
insert into table monthstate select state,"dec",sum(dec) from month group by state;

select * 
from (select state,month,total,row_number() over (partition by state order by total desc) as rn from monthstate) a 
where a.rn<=3;

-----------------------------------------

5. What was the highest & least accident prone year for each state.?

select b.st,c.year,b.mx 
from (select a.state as st,max(a.total) as mx from 
(select year,state,sum(tot) as total from month group by year,state) a group by a.state) b, 
(select year,state,sum(tot) as total from month group by year,state) c 
where b.mx = c.total;

select b.st,c.year,b.mn 
from (select a.state as st,min(a.total) as mn from 
(select year,state,sum(tot) as total from month group by year,state) a group by a.state) b, 
(select year,state,sum(tot) as total from month group by year,state) c 
where b.mn = c.total;

-----------------------------------------

6. Safest and Hazardous state.

select state,sum(tot) as total 
from month 
group by state 
order by total limit 1; 

select state,sum(tot) as total 
from month 
group by state 
order by total desc limit 1;

-----------------------------------------

7. Safest and Hazardous months for different type of accidents.

create table monthtype(name string,type string,total int);  
insert into table monthtype select "jan",type,sum(jan) from month group by type;
insert into table monthtype select "feb",type,sum(feb) from month group by type;
insert into table monthtype select "mar",type,sum(mar) from month group by type;
insert into table monthtype select "apr",type,sum(apr) from month group by type;
insert into table monthtype select "may",type,sum(may) from month group by type;
insert into table monthtype select "june",type,sum(june) from month group by type;
insert into table monthtype select "july",type,sum(july) from month group by type;
insert into table monthtype select "aug",type,sum(aug) from month group by type;
insert into table monthtype select "sept",type,sum(sept) from month group by type;
insert into table monthtype select "oct",type,sum(oct) from month group by type;
insert into table monthtype select "nov",type,sum(nov) from month group by type;
insert into table monthtype select "dec",type,sum(dec) from month group by type;

select name,type,total 
from monthtype a, 
(select type,min(total) as mn from monthtype group by type) b 
where a.total=b.mn 
group by a.name,a.type,a.total;

select name,type,total 
from monthtype a, 
(select type,max(total) as mx from monthtype group by type) b 
where a.total=b.mx 
group by a.name,a.type,a.total;

-------------------------------------------

8. Safest and Hazardous time intervals for types of accidents.

create table timetype(name string,type string,total int);
insert into table timetype select "0to3",type,sum(0to3) from time group by type;
insert into table timetype select "3to6",type,sum(3to6) from time group by type;
insert into table timetype select "6to9",type,sum(6to9) from time group by type;
insert into table timetype select "9to12",type,sum(9to12) from time group by type;
insert into table timetype select "12to15",type,sum(12to15) from time group by type;
insert into table timetype select "15to18",type,sum(15to18) from time group by type;
insert into table timetype select "18to21",type,sum(18to21) from time group by type;
insert into table timetype select "21to24",type,sum(21to24) from time group by type; 

select name,type,total 
from timetype a, 
(select type,min(total) as mn from timetype group by type) b 
where a.total=b.mn 
group by a.name,a.type,a.total;

select name,type,total 
from timetype a, 
(select type,max(total) as mx from timetype group by type) b 
where a.total=b.mx 
group by a.name,a.type,a.total;

---------------------------------------------

9. How Hazardous and Safest times varied each year.?

create table timeyear(year int,time string,total int);
insert into table timeyear select year,"0to3",sum(0to3) from time group by year;
insert into table timeyear select year,"3to6",sum(3to6) from time group by year;
insert into table timeyear select year,"6to9",sum(6to9) from time group by year;
insert into table timeyear select year,"9to12",sum(9to12) from time group by year;
insert into table timeyear select year,"12to15",sum(12to15) from time group by year;
insert into table timeyear select year,"15to18",sum(15to18) from time group by year;
insert into table timeyear select year,"18to21",sum(18to21) from time group by year;
insert into table timeyear select year,"21to24",sum(21to24) from time group by year;

select year,time,total 
from timeyear a, 
(select year,max(total) as mx from timeyear group by year) b 
where a.total=b.mx 
group by a.year,a.time,a.total;

select year,time,total 
from timeyear a, 
(select year,min(total) as mn from timeyear group by year) b 
where a.total=b.mn 
group by a.year,a.time,a.total;

---------------------------------------------

10. How Hazardous months varied each year.?

create table monthyear(year int,name string,total int);  
insert into table monthyear select year,"jan",sum(jan) from month group by year;
insert into table monthyear select year,"feb",sum(feb) from month group by year;
insert into table monthyear select year,"mar",sum(mar) from month group by year;
insert into table monthyear select year,"apr",sum(apr) from month group by year;
insert into table monthyear select year,"may",sum(may) from month group by year;
insert into table monthyear select year,"june",sum(june) from month group by year;
insert into table monthyear select year,"july",sum(july) from month group by year;
insert into table monthyear select year,"aug",sum(aug) from month group by year;
insert into table monthyear select year,"sept",sum(sept) from month group by year;
insert into table monthyear select year,"oct",sum(oct) from month group by year;
insert into table monthyear select year,"nov",sum(nov) from month group by year;
insert into table monthyear select year,"dec",sum(dec) from month group by year;

select year,name,total 
from monthyear a, 
(select year,max(total) as mx from monthyear group by year) b
where a.total=b.mx 
group by a.year,a.name,a.total;

select year,name,total 
from monthyear a, 
(select year,min(total) as mn from monthyear group by year) b 
where a.total=b.mn 
group by a.year,a.name,a.total;

----------------------------------------------

11. How Hazardous states varied each year.?

select b.year,c.state,b.mx 
from (select year,max(a.total) as mx from (select year,state,sum(tot) as total from month group by year,state) a group by year) b,
(select year,state,sum(tot) as total from month group by year,state) c 
where b.mx=c.total 
order by b.year;

----------------------------------------------

12. Safest and Hazardous time intervals for each state.

create table timestate(state string,time string,total int);
insert into table timestate select state,"0to3",sum(0to3) from time group by state;
insert into table timestate select state,"3to6",sum(3to6) from time group by state;
insert into table timestate select state,"6to9",sum(6to9) from time group by state;
insert into table timestate select state,"9to12",sum(9to12) from time group by state;
insert into table timestate select state,"12to15",sum(12to15) from time group by state;
insert into table timestate select state,"15to18",sum(15to18) from time group by state;
insert into table timestate select state,"18to21",sum(18to21) from time group by state;
insert into table timestate select state,"21to24",sum(21to24) from time group by state;

select state,time,total 
from timestate a, 
(select state,max(total) as mx,min(total) as mn from timestate group by state) b 
where (a.total=b.mx or a.total=b.mn) 
group by a.state,a.time,a.total 
order by a.state,a.total;

-----------------------------------------------

13. Percentage of accidents that took place in each month.

create table monthtot(name string,total int);
insert into table monthtot select "jan",sum(jan) from month;
insert into table monthtot select "feb",sum(feb) from month;
insert into table monthtot select "mar",sum(mar) from month;
insert into table monthtot select "apr",sum(apr) from month;
insert into table monthtot select "may",sum(may) from month;
insert into table monthtot select "june",sum(june) from month;
insert into table monthtot select "july",sum(july) from month;
insert into table monthtot select "aug",sum(aug) from month;
insert into table monthtot select "sept",sum(sept) from month;
insert into table monthtot select "oct",sum(oct) from month;
insert into table monthtot select "nov",sum(nov) from month;
insert into table monthtot select "dec",sum(dec) from month;

select name,round(total/b.tl*100,3) 
from monthtot a,
(select sum(tot) as tl from month) b 
group by name,b.tl,total;

------------------------------------------------

14. Percentage of accidents that took place in each time interval.

create table timetot(time string,total int);
insert into table timetot select "0to3",sum(0to3) from time;
insert into table timetot select "3to6",sum(3to6) from time;
insert into table timetot select "6to9",sum(6to9) from time;
insert into table timetot select "9to12",sum(9to12) from time;
insert into table timetot select "12to15",sum(12to15) from time;
insert into table timetot select "15to18",sum(15to18) from time;
insert into table timetot select "18to21",sum(18to21) from time;
insert into table timetot select "21to24",sum(21to24) from time;

select time,round(total/b.tl*100,3) 
from timetot a,
(select sum(tot) as tl from time) b 
group by time,b.tl,total;

-------------------------------------------------

15. What was the safest time of day and night in each state.?

select b.state,b.part,b.time,b.total 
from (select *,row_number() over(partition by state,a.part order by total) as rn from
(select state,total,time,
case 
when time in ("0to3","3to6","18to21","21to24") then "night" else "day" 
end as part 
from timestate group by state,total,time) a
) b 
where b.rn=1;

-------------------------------------------------
