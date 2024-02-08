use sqlclass_db;

-- drop table if exists birth_rate;
-- create table birth_rate
-- 	(시도별 varchar(20) primary key, 
-- 	2013년 float(4,3),
-- 	2014년 float(4, 3),
-- 	2015년 float(4, 3),
-- 	2016년 float(4, 3),
-- 	2017년 float(4, 3),
-- 	2018년 float(4, 3),
-- 	2019년 float(4, 3),
-- 	2020년 float(4, 3),
-- 	2021년 float(4, 3),
-- 	2022년 float(4, 3)
-- );
drop table if exists 시도_합계출산율;
select * from 시도_합계출산율;
drop table if exists 행정구역별_학령인구_총합;
select * from 행정구역별_학령인구_총합;
-- 
-- alter table 시도_합계출산율 add foreign key([시도별, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]) 
-- 
-- references 행정구역별_학령인구_총합([행정구역(시군구)별, 연령별, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]);



	