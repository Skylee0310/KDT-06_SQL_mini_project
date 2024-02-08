#데이터베이스 선택
use schoolage;

# csv 파일 불러오기.
select * from 시도_합계출산율;

# csv 파일 불러오기.
select * from 행정구역별_학령인구_총합;

# 필요없는 열 삭제
alter table 행정구역별_학령인구_총합 drop column 연령별;

# 데이터 확인
desc 행정구역별_학령인구_총합;
desc 시도_합계출산율;

# 컬럼명을 일치시키기 위해서 새로운 '시도별'열 추가 후 '행정구역'열의 값을 복사한 다음 '행정구역' 열 삭제.
alter table 행정구역별_학령인구_총합 add 시도별 varchar(50);
select * from 행정구역별_학령인구_총합;

update 행정구역별_학령인구_총합 set 시도별 = 행정구역;
select * from 행정구역별_학령인구_총합;
#alter table 행정구역별_학령인구_총합 drop column 행정구역;

# '행정구역별 학령인구 총합'의 시도별 열을 'not null'로 설정한 후 기본키로 설정.
alter table 행정구역별_학령인구_총합 modify column 시도별 varchar(50) not null;
#alter table 행정구역별_학령인구_총합 drop primary key;
alter table 행정구역별_학령인구_총합 add primary key(시도별);

# 시도별 열 기준으로 정렬
select * from 행정구역별_학령인구_총합
order by 시도별;

select * from 시도_합계출산율
order by 시도별;

# 맨 마지막에 추가되었던 시도별 열을 맨앞으로 이동
ALTER TABLE 행정구역별_학령인구_총합 MODIFY COLUMN 시도별 varchar(50) FIRST;

#select * from 행정구역별_학령인구_총합;

# 시도별 열을 외래키로 하여 시도_합계 출산율과 행정구역별 학령인구 총합을 연결.
alter table 시도_합계출산율 add constraint foreign key (시도별) references 행정구역별_학령인구_총합(시도별);