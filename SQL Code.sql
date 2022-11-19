-- Creating the staging area (Temporary table)
insert into all_data select * from "2001"; -- Drop table "2001" purge was done after the transfer.
insert into all_data select * from "2002"; -- Drop table "2002" purge was done after the transfer.
insert into all_data select * from "2003"; -- Drop table "2003" purge was done after the transfer.
insert into all_data select * from "2004"; -- Drop table "2004" purge was done after the transfer.
insert into all_data select * from "2005"; -- Drop table "2005" purge was done after the transfer.
insert into all_data select * from "2006"; -- Drop table "2006" purge was done after the transfer.
insert into all_data select * from "2007"; -- Drop table "2007" purge was done after the transfer.
insert into all_data select * from "2008"; -- Drop table "2008" purge was done after the transfer.
insert into all_data select * from "2009"; -- Drop table "2009" purge was done after the transfer.
insert into all_data select * from "2010"; -- Drop table "2010" purge was done after the transfer.
insert into all_data select * from "2011"; -- Drop table "2011" purge was done after the transfer.
insert into all_data select * from "2012"; -- Drop table "2012" purge was done after the transfer.
insert into all_data select * from "2013"; -- Drop table "2013" purge was done after the transfer.
insert into all_data select * from "2014"; -- Drop table "2014" purge was done after the transfer.
insert into all_data select * from "2015"; -- Drop table "2015" purge was done after the transfer.
insert into all_data select * from "2016"; -- Drop table "2016" purge was done after the transfer.

-- Checking for nulls

select * from all_data where "samplesamplingPointnotation" is null;
select * from all_data where "samplesamplingPointlabel" is null;
select * from all_data where "samplesampleDateTime" is null;
select * from all_data where "determinandlabel" is null;
select * from all_data where "determinanddefinition" is null;
select * from all_data where "determinandnotation" is null;
select * from all_data where "result" is null;
select * from all_data where "determinandunitlabel" is null;


-- Removing the no flow/samp rows
delete from all_data where "determinandlabel" = 'NO FLOW/SAMP';

-- Creating the dimension tables

create table locations (
location_id varchar(15) primary key,
Locations varchar(40));

create table time (
Time_id varchar(30) primary key,
dates varchar(20),
day varchar(3) not null,
month varchar(3) not null,
monthname varchar2(10),
year varchar(5) not null,
weekofyear number(3) not null);

create table Sensors(
sensor_id number(6) primary key,
sensor_type varchar(25) not null,
sensor_def varchar(75),
unit varchar(10));

-- Creating the fact table
create table water_quality( 
Time_id varchar(30),
sensor_id number(6),
location_id varchar(15),
sensor_output float(10) not null,
NB_Measurements number(10),
constraint WQ_FKM foreign key (sensor_id) references sensors(sensor_id),
constraint WQ_FKL foreign key (location_id) references locations(location_id),
constraint WQ_FKT foreign key (Time_id) references time(Time_id),
constraint WQ_PK primary key (Time_id, sensor_id, location_id))
tablespace student2;

-- Populating the dimension tables
    -- Populating the locations and sensors dimensions
insert into locations select distinct "samplesamplingPointnotation" , "samplesamplingPointlabel" from all_data;

insert into sensors select distinct "determinandnotation", "determinandlabel", "determinanddefinition", "determinandunitlabel" from all_data;

    -- Populating the time dimension using a cursor

declare cursor times is 
select distinct "samplesampleDateTime" from all_data;
days varchar(3);
dates varchar(20);
months varchar(3);
monthnames varchar2(10);
years varchar(5);
weekofyears number(3);
begin
    for i in times loop
    days:= to_char(to_date(substr(i."samplesampleDateTime",0,10), 'YY-MM-DD'),'DD');
    dates:= to_char(to_date(substr(i."samplesampleDateTime",0,10), 'YY-MM-DD'),'DD-MM-YYYY'); -- Date format was re-arranged
    months:= to_char(to_date(substr(i."samplesampleDateTime",0,10), 'YY-MM-DD'),'MM');
    monthnames:= to_char(to_date(substr(i."samplesampleDateTime",0,10), 'YY-MM-DD'),'Month');
    years:= to_char(to_date(substr(i."samplesampleDateTime",0,10), 'YY-MM-DD'),'YYYY');
    weekofyears:= to_number(to_char(to_date(substr(i."samplesampleDateTime",0,10), 'YY-MM-DD'),'iw')) ;
    insert into time values (i."samplesampleDateTime" , dates,days, months, monthnames, years, weekofyears);
end loop;
end;
/

-- Creating and populating a temporary table for the nb_measurement column in the fact table
create table temp(
sensorid number(10),
dateid varchar(50),
locationid varchar(50),
measurement number(10))
tablespace student2;

insert into temp 
select f."determinandnotation",  substr(f."samplesampleDateTime",0,10), f."samplesamplingPointnotation", count(f."result") as measurement from all_data f, sensors s, time t, locations l
where s.sensor_id = f."determinandnotation" and f."samplesampleDateTime" = t.time_id and f."samplesamplingPointnotation" = l.location_id
group by  f."determinandnotation" , substr(f."samplesampleDateTime",0,10), f."samplesamplingPointnotation";

-- Populating the fact table using a cursor

declare cursor WQ is 
select t.time_id, s.sensor_id, l.location_id, a."result" from all_data a
join sensors s on (a."determinandnotation" = s.sensor_id)
join locations l on (a."samplesamplingPointnotation" = l.location_id)
join time t on  (a."samplesampleDateTime" = t.time_id);
nb number(10);
begin
for i in WQ loop
select distinct measurement into nb from temp f, all_data a
where sensorid = i.sensor_id
and dateid = substr(i.time_id,0,10)
and locationid = i.location_id;
insert into water_quality values (i.time_id, i.sensor_id, i.location_id, i."result", nb);
end loop;
end;
/

-- Dropping the temporary tables without purging
drop table temp;
drop table all_data;

-- List of water sensors by type by month
select distinct s.sensor_id, s.sensor_type, t.monthname from sensors s, time t, water_quality a
where a.sensor_id = s.sensor_id and t.time_id=a.time_id;

-- Number of measurements by sensor type by week
select s.sensor_type, t.weekofyear, count(w.sensor_output) as number_of_measurements from water_quality w, sensors s, time t
where s.sensor_id = w.sensor_id and t.time_id = w.time_id
group by s.sensor_type, t.weekofyear;

-- Number of measurements by location by month
select t.monthname, l.locations, count(w.sensor_output) as number_of_measurements from water_quality w, time t, locations l
where w.time_id = t.time_id and w.location_id = l.location_id
group by t.monthname, l.locations;

-- Average number of pH measurements per year
select round(avg(number_of_measurements),2) as average_number_of_pH_measurements_per_year from (
select t.year,count(w.sensor_output) as number_of_measurements from water_quality w, time t, sensors s
where t.time_id = w.time_id and s.sensor_id = w.sensor_id and s.sensor_type like 'pH%'
group by t.year);

-- Average value of Nitrate measurements by location by year
select t.year, l.locations, round(avg(w.sensor_output),2) as average_number_of_measurements from water_quality w, time t, sensors s, locations l
where t.time_id = w.time_id and s.sensor_id = w.sensor_id and l.location_id = w.location_id and s.sensor_type like 'Nitrate%'
group by t.year, l.locations;


--- Additional Queries

-- The Create table statement that will Partition the Fact table by year.

create table water_quality2 ( 
Time_id varchar(30),
sensor_id number(6),
location_id varchar(15),
sensor_output float(10) not null,
NB_Measurements number(10),
year varchar(10),
constraint WQ_FKM foreign key (sensor_id) references sensors(sensor_id),
constraint WQ_FKL foreign key (location_id) references locations(location_id),
constraint WQ_FKT foreign key (Time_id) references time(Time_id),
constraint WQ_PK primary key (Time_id, sensor_id, location_id))
PARTITION BY HASH(year)
PARTITIONS 17;

-- An extended TIME table should be created and populated.

create table Time_Extended (
Time_id varchar(30) primary key,
dates varchar(20),
day varchar(10) not null,
dayofweek varchar(20),
dayofyear varchar(5),
timeofday varchar(20),
month varchar(10) not null,
monthname varchar2(10),
year varchar(10) not null,
month_and_year varchar(40),
weekofmonth varchar(10),
weekofyear number(10) not null,
Quarter varchar(10)
);
drop table time_extended purge;

declare cursor timess is 
select distinct time_id from time;
days varchar(10);
dates varchar(25);
dayofweek varchar(20);
timeofday varchar(20);
months varchar(10);
monthnames varchar2(10);
years varchar(10);
weekofyears number(10);
month_and_year varchar(40);
weekofmonth varchar(10);
Quarter varchar(10);
dayofyear varchar(10);

begin
    for i in timess loop
    days:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'DD');
    dates:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'DD-MM-YYYY');
    months:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'MM');
    monthnames:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'Month');
    years:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'YYYY');
    weekofyears:= to_number(to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'ww'));
    dayofweek:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'Day'); 
    timeofday:= substr(i.time_id,12,5);
    dayofyear:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'DDD');
    month_and_year:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'Month YYYY'); 
    weekofmonth:= to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'w');
    Quarter:=to_char(to_date(substr(i.time_id,0,10), 'YY-MM-DD'),'Q');
    
    insert into time_extended values (i.time_id, dates, days, dayofweek, dayofyear, timeofday, months, monthnames, years, month_and_year, weekofmonth, weekofyears, quarter);
end loop;
end;
/

-- The Average of the Nitrate-N sensor value by every two years materialized view

Create materialized view Avg_Nitrate_every_2years as
select '2000-2001' as period, avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2000%')
Union
select '2003-2004' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2004%')
Union
select '2005-2006' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2006%')
Union
select '2007-2008' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2008%')
Union
select '2009-2010' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2010%')
Union
select '2011-2012' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2012%')
Union
select '2013-2014' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2014%')
Union
select '2015-2016' as period,avg(avg_2years) from (
select (year || '-' || (year+1) ) as years, avg(avg_year) as avg_2years from (
select t.year, round(avg(w.sensor_output),2) as avg_year from time t, water_quality w where t.time_id=w.time_id and w.sensor_id=117  group by t.year)
group by (year || '-' || (year+1)) having (year || '-' || (year+1) ) like '%2016%');
