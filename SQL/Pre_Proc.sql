
------------ Fill missing dates between Min and Max date --- 
drop table if exists tem_simple_date;
create table tem_simple_date as
select * from dim_simple_date where datevalue between (select min(DD_ACTUALDELIVERYDATE) as min_date from tmp_subprocess6_final) and (select ADD_DAYS(max(DD_ACTUALDELIVERYDATE),28) as max_date from tmp_subprocess6_final)
order by datevalue;

drop table if exists temp_grain_date ;
create table temp_grain_date as
select distinct DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME, DATEVALUE as 
DD_ACTUALDELIVERYDATE, 0 as CT_ACTUALDELIVEREDQUANTITY  from tmp_subprocess6_final
cross join tem_simple_date;

alter table temp_grain_date add column sample varchar(10);

update temp_grain_date
set sample = case when DD_ACTUALDELIVERYDATE <= (select max(DD_ACTUALDELIVERYDATE) as min_date from tmp_subprocess6_final) Then 'Training' ELSE 'Horizon' END;

drop table if exists temp_prepprossed;
create table temp_prepprossed as
(select
a.DD_FORECASTUNITCODE,a.DD_ECCCUSTOMERLEVEL7CODE,a.DD_PLANTCODE,a.DD_MODEL_HIERARCHY,a.DD_SUBDIVISION2NAME,a.DD_CATEGORYNAME,
a.DD_ACTUALDELIVERYDATE,a.sample, b.DD_ACTUALDELIVERYDATE AS old_date, ifnull(b.CT_ACTUALDELIVEREDQUANTITY ,0) as CT_ACTUALDELIVEREDQUANTITY
 From temp_grain_date  a left join
tmp_subprocess6_final b
on a.DD_FORECASTUNITCODE=b.DD_FORECASTUNITCODE 
and a.DD_ECCCUSTOMERLEVEL7CODE=b.DD_ECCCUSTOMERLEVEL7CODE 
and a.DD_PLANTCODE=b.DD_PLANTCODE
and a.DD_ACTUALDELIVERYDATE=b.DD_ACTUALDELIVERYDATE);

----  Remove leading zeros ----------
DROP TABLE IF EXISTS tmp_preprocessed_cumsum;
CREATE TABLE tmp_preprocessed_cumsum AS (
SELECT DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,DD_ACTUALDELIVERYDATE,CT_ACTUALDELIVEREDQUANTITY,sample,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME,
SUM(CT_ACTUALDELIVEREDQUANTITY) OVER(PARTITION BY DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE ORDER BY DD_ACTUALDELIVERYDATE asc) AS CUM_SHARE 
from temp_prepprossed ); 

DROP TABLE IF EXISTS tmp_leadingzero;
CREATE TABLE tmp_leadingzero AS (
SELECT DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,DD_ACTUALDELIVERYDATE,CT_ACTUALDELIVEREDQUANTITY,sample,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME from tmp_preprocessed_cumsum 
where cum_share <> 0 order by DD_ACTUALDELIVERYDATE );

----- calculating MOQ --------
DROP TABLE IF EXISTS temp_28daysRmin;
CREATE TABLE temp_28daysRmin AS(
select DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,CT_ACTUALDELIVEREDQUANTITY,DD_ACTUALDELIVERYDATE,sample,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME,
 TRUNC(min(CT_ACTUALDELIVEREDQUANTITY) over(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE rows between 28 preceding and current row),3) as rolling_min
from tmp_leadingzero );

DROP TABLE IF EXISTS temp_28daysRmin_LAG;
CREATE TABLE temp_28daysRmin_LAG AS (
SELECT DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,CT_ACTUALDELIVEREDQUANTITY,DD_ACTUALDELIVERYDATE,sample,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME,
       Lag(rolling_min, 1,rolling_min) OVER(
       partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE) AS MOQ
FROM temp_28daysRmin);

----- calculating lot size  --------
DROP TABLE IF EXISTS temp_28daysMOA;
CREATE TABLE temp_28daysMOA AS (
select DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,CT_ACTUALDELIVEREDQUANTITY,DD_ACTUALDELIVERYDATE,MOQ,sample,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME,
 TRUNC(avg(CT_ACTUALDELIVEREDQUANTITY) over(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE rows between 28 preceding and current row),3) as rolling_avg
from temp_28daysRmin_LAG);

DROP TABLE IF EXISTS temp_28daysMOA_LAG;
CREATE TABLE temp_28daysMOA_LAG AS (
SELECT DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE,sample,DD_MODEL_HIERARCHY,DD_SUBDIVISION2NAME,DD_CATEGORYNAME,
ROW_NUMBER() OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE) AS dd_rownum,
CT_ACTUALDELIVEREDQUANTITY,DD_ACTUALDELIVERYDATE,
MOQ,
Lag(rolling_avg, 1,rolling_avg) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE) AS Lot_Size
FROM temp_28daysMOA);

UPDATE temp_28daysMOA_LAG 
SET moq = 0,
lot_size = 0
WHERE dd_rownum <= 27;

------------------------- joins with external data -------------------------------

--join on Demographic
drop table if exists temp_demographics;
create table temp_demographics as (
select a.*,
b.DD_CITY,
b.DD_COUNTY,
b.DD_CTR,
b.DD_FULL_COUNTRY_NAME,
b.DD_FULL_STATE_NAME,
b.DD_PLACE,
b.DD_POSTALCODE,
b.DD_RG
from
temp_28daysMOA_LAG a left join
FACT_NA_DC_DEMOGRAPHIC_DATA b on a.DD_PLANTCODE = b.DD_PLNT);

------ join on Stringency Index --------
drop table if exists temp_stringencyIndex;
create table temp_stringencyIndex as (
select a.*,
b.DD_STRINGENCYINDEX from
temp_demographics a left join 
fact_NA_Covid_Stringency_Data b on 
a.DD_FULL_STATE_NAME=b.DD_REGIONNAME and a.DD_ACTUALDELIVERYDATE=b.DD_FORMATTED_DATE);

------- join on covid Mobility Index -----------

drop table if exists temp_google_covid_mobility_county;
create table temp_google_covid_mobility_county as (select DD_COUNTRY_REGION_CODE,
DD_DATE_,
DD_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE,
DD_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE,
DD_SUB_REGION_1,
DD_SUB_REGION_2 from Fact_NA_Covid_Mobility_Data where DD_SUB_REGION_2 is not null or DD_SUB_REGION_2<>'');

drop table if exists temp_google_covid_mobility_state;
create table temp_google_covid_mobility_state as (select DD_COUNTRY_REGION_CODE,
DD_DATE_,
DD_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE,
DD_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE,
DD_SUB_REGION_1,
DD_SUB_REGION_2 from Fact_NA_Covid_Mobility_Data where (DD_SUB_REGION_1 is not null or DD_SUB_REGION_1<>'') and DD_SUB_REGION_2 IS null or DD_SUB_REGION_2<>'' ); 

drop table if exists temp_google_covid_mobility_country;
create table temp_google_covid_mobility_country as (select DD_COUNTRY_REGION_CODE,
DD_DATE_,
DD_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE,
DD_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE,
DD_SUB_REGION_1,
DD_SUB_REGION_2 from Fact_NA_Covid_Mobility_Data where DD_SUB_REGION_1 IS null or DD_SUB_REGION_1<>'' and DD_SUB_REGION_2 IS null or DD_SUB_REGION_2<>'' and DD_COUNTRY_REGION is not null or DD_COUNTRY_REGION<>'' );


drop table if exists temp_google_covid_mobility_countyM;
create table temp_google_covid_mobility_countyM as (select a.*,
b.DD_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE as County_grocery_and_pharmacy_percent_change_from_baseline,
b.DD_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE as County_retail_and_recreation_percent_change_from_baseline 
from temp_stringencyIndex a left join temp_google_covid_mobility_county b on a.DD_CTR=b.DD_COUNTRY_REGION_CODE and a.DD_FULL_STATE_NAME=b.DD_SUB_REGION_1 and a.DD_COUNTY=b.DD_SUB_REGION_2 and a.DD_ACTUALDELIVERYDATE=b.DD_DATE_ );

drop table if exists temp_google_covid_mobility_stateM;
create table temp_google_covid_mobility_stateM as (select a.*,
b.DD_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE as State_grocery_and_pharmacy_percent_change_from_baseline,
b.DD_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE as State_retail_and_recreation_percent_change_from_baseline
from temp_google_covid_mobility_countyM a left join temp_google_covid_mobility_state b on a.DD_CTR=b.DD_COUNTRY_REGION_CODE and a.DD_FULL_STATE_NAME=b.DD_SUB_REGION_1  and a.DD_ACTUALDELIVERYDATE=b.DD_DATE_ );

drop table if exists temp_google_covid_mobility_countryM;
create table temp_google_covid_mobility_countryM as (select a.*,
b.DD_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE as Country_grocery_and_pharmacy_percent_change_from_baseline,
b.DD_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE as Country_retail_and_recreation_percent_change_from_baseline
from temp_google_covid_mobility_stateM a left join temp_google_covid_mobility_country b on a.DD_CTR=b.DD_COUNTRY_REGION_CODE and a.DD_ACTUALDELIVERYDATE=b.DD_DATE_ );

update temp_google_covid_mobility_countryM
set County_retail_and_recreation_percent_change_from_baseline = CASE WHEN County_retail_and_recreation_percent_change_from_baseline IS NULL OR County_retail_and_recreation_percent_change_from_baseline <>''
THEN  STATE_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE ELSE  County_retail_and_recreation_percent_change_from_baseline END;

update temp_google_covid_mobility_countryM
set County_retail_and_recreation_percent_change_from_baseline = CASE WHEN County_retail_and_recreation_percent_change_from_baseline IS NULL OR County_retail_and_recreation_percent_change_from_baseline <>''
THEN  COUNTRY_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE ELSE  County_retail_and_recreation_percent_change_from_baseline END;

update temp_google_covid_mobility_countryM
set COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE = CASE WHEN COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE IS NULL OR COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE <>'' 
THEN  STATE_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE ELSE  COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE END;

update temp_google_covid_mobility_countryM
set COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE  = CASE WHEN COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE IS NULL OR COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE <>'' 
THEN  COUNTRY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE ELSE  COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE END;

ALTER TABLE temp_google_covid_mobility_countryM RENAME COLUMN COUNTY_GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE TO GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE;
ALTER TABLE temp_google_covid_mobility_countryM  RENAME COLUMN COUNTY_RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE TO RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE;

ALTER TABLE temp_google_covid_mobility_countryM DROP COLUMN State_retail_and_recreation_percent_change_from_baseline;
ALTER TABLE temp_google_covid_mobility_countryM  DROP COLUMN State_grocery_and_pharmacy_percent_change_from_baseline;
ALTER TABLE temp_google_covid_mobility_countryM  DROP COLUMN Country_retail_and_recreation_percent_change_from_baseline;
ALTER TABLE temp_google_covid_mobility_countryM  DROP COLUMN Country_grocery_and_pharmacy_percent_change_from_baseline ;

------- join on holiay feature add get holiday features  ----------------
DROP TABLE IF EXISTS tmp_alldates;
CREATE TABLE tmp_alldates
AS
SELECT t.dd_country, t.dd_jurisdiction, 
d.datevalue, 
CAST(0 AS TINYINT) IsHoliday,
CAST(0 AS TINYINT) BeforeHolidayFlag_7days,
CAST(0 AS TINYINT) AfterHolidayFlag_7days,
CAST(NULL AS VARCHAR(50)) dd_holiday_description
FROM dim_simple_date d, (SELECT DISTINCT dd_country, dd_jurisdiction FROM FACT_NA_HOLIDAY_LIST) t
WHERE d.datevalue >= '2019-01-01'
AND d.datevalue <= '2022-12-31';

--- Update isholiday flag based on 'Federal' jurisdiction. Apply this flag to all country-jurisdiction pairs
UPDATE tmp_alldates t
SET t.isholiday = 1,
t.dd_holiday_description = h.dd_holiday_description
FROM tmp_alldates t, FACT_NA_HOLIDAY_LIST h
WHERE t.dd_country = h.dd_country
AND t.datevalue = h.dd_date_
AND h.dd_jurisdiction = 'Federal';

--- Update BeforeHolidayFlag_7days flag for 7 days before the holiday. AfterHolidayFlag_7days for 7 days after the holiday.  
UPDATE tmp_alldates t
SET t.BeforeHolidayFlag_7days = 1
WHERE EXISTS ( SELECT 1 FROM tmp_alldates h
WHERE t.dd_country = h.dd_country
AND t.dd_jurisdiction = h.dd_jurisdiction
AND h.isholiday = 1
AND t.datevalue >= h.datevalue - 7
AND t.datevalue < h.datevalue);

UPDATE tmp_alldates t
SET t.AfterHolidayFlag_7days = 1
WHERE EXISTS ( SELECT 1 FROM tmp_alldates h
WHERE t.dd_country = h.dd_country
AND t.dd_jurisdiction = h.dd_jurisdiction
AND h.isholiday = 1
AND t.datevalue <= h.datevalue + 7
AND t.datevalue > h.datevalue);

drop table if exists temp_holiday_pre;

create table temp_holiday_pre as (select a.*,
case when b.isholiday = 1 then 'Yes' else 'No' end as isholiday,
ifnull(b.dd_holiday_description,'No_Holiday') Holiday_Desc,
b.BeforeHolidayFlag_7days,
b.AfterHolidayFlag_7days
from temp_google_covid_mobility_countryM a left join 
tmp_alldates b on b.Datevalue=a.DD_ACTUALDELIVERYDATE and b.DD_COUNTRY=a.DD_Ctr
where b.dd_jurisdiction = 'Federal');
 
-------- preprocessing part start ----------------

--- fill missing values in the external featutes ----
DROP TABLE IF EXISTS temp_preprocessingtable;
CREATE TABLE temp_preprocessingtable AS SELECT
b.DD_MODEL_HIERARCHY,
b.DD_SUBDIVISION2NAME,
b.DD_CATEGORYNAME,
b.DD_FORECASTUNITCODE,
b.DD_ECCCUSTOMERLEVEL7CODE,
b.DD_PLANTCODE,
b.DD_ACTUALDELIVERYDATE,
IFNULL(b.CT_ACTUALDELIVEREDQUANTITY ,0) CT_ACTUALDELIVEREDQUANTITY,
IFNULL(b.MOQ,0) MOQ, 
IFNULL(b.LOT_SIZE,0) LOT_SIZE,
b.DD_CITY,
b.DD_COUNTY,
b.DD_CTR,
b.DD_FULL_COUNTRY_NAME,
b.DD_FULL_STATE_NAME,
b.DD_PLACE,
b.DD_POSTALCODE,
b.DD_RG,
IFNULL(b.DD_STRINGENCYINDEX ,0) DD_STRINGENCYINDEX,
IFNULL(b.GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE,0) GROCERY_AND_PHARMACY_PERCENT_CHANGE_FROM_BASELINE,
IFNULL(b.RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE,0) RETAIL_AND_RECREATION_PERCENT_CHANGE_FROM_BASELINE,
IFNULL(b.HOLIDAY_DESC,'No_Holiday') HOLIDAY_DESC,
b.ISHOLIDAY,
b.BeforeHolidayFlag_7days,
b.AfterHolidayFlag_7days,
b.sample
FROM temp_holiday_pre b;

---- remove special characyters ----------
update temp_preprocessingtable set HOLIDAY_DESC=replace(replace(replace(replace(HOLIDAY_DESC,'.',''),'(',''),')',''),'''','');

---------  replace special character by space -------
update temp_preprocessingtable set HOLIDAY_DESC=replace(HOLIDAY_DESC,' ','_');

---- lag added  --------
drop table if exists temp_preprocessingtable_lag;

create table temp_preprocessingtable_lag as (
select a.*,
Lag(CT_ACTUALDELIVEREDQUANTITY, 7,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_1,
Lag(CT_ACTUALDELIVEREDQUANTITY, 14,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE) as Lag_2,
Lag(CT_ACTUALDELIVEREDQUANTITY, 21,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_3,
Lag(CT_ACTUALDELIVEREDQUANTITY, 28,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_4,
Lag(CT_ACTUALDELIVEREDQUANTITY, 35,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_5,
Lag(CT_ACTUALDELIVEREDQUANTITY, 42,0)  OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE) as Lag_6,
Lag(CT_ACTUALDELIVEREDQUANTITY, 49,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_7,
Lag(CT_ACTUALDELIVEREDQUANTITY, 56,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_8,
Lag(CT_ACTUALDELIVEREDQUANTITY, 63,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_9,
Lag(CT_ACTUALDELIVEREDQUANTITY, 70,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_10,
Lag(CT_ACTUALDELIVEREDQUANTITY, 77,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_11,
Lag(CT_ACTUALDELIVEREDQUANTITY, 84,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_12,
Lag(CT_ACTUALDELIVEREDQUANTITY, 91,0) OVER(partition by DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE order by DD_ACTUALDELIVERYDATE)  as Lag_13
from temp_preprocessingtable a );

------ outlier treatment ------
drop table if exists temp_preprocessingOutlier;

create table temp_preprocessingOutlier as(
select b.*, cast(0 as decimal(18,0)) Outlier_treated_sales from 
(SELECT a.*,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY CT_ACTUALDELIVEREDQUANTITY) 
        OVER (PARTITION BY DD_FORECASTUNITCODE, DD_ECCCUSTOMERLEVEL7CODE, DD_PLANTCODE) Percentile
FROM temp_preprocessingtable_lag a where a.sample = 'Training' ORDER BY DD_ACTUALDELIVERYDATE) b);

update temp_preprocessingOutlier
set CT_ACTUALDELIVEREDQUANTITY = case when CT_ACTUALDELIVEREDQUANTITY >= Percentile  then Percentile else CT_ACTUALDELIVEREDQUANTITY end;

alter table temp_preprocessingOutlier drop column PERCENTILE;
alter table temp_preprocessingOutlier drop column OUTLIER_TREATED_SALES;

drop table if exists temp_outlierperproc;

create table temp_outlierperproc as(
SELECT a.*
FROM temp_preprocessingOutlier a
where a.sample = 'Training'
UNION
SELECT b.*
FROM temp_preprocessingtable_lag b
where b.sample = 'Horizon');
 -----  renaming column for final table ---------
ALTER TABLE  temp_outlierperproc RENAME COLUMN DD_MODEL_HIERARCHY TO MODEL_HIERARCHY;
ALTER TABLE  temp_outlierperproc RENAME COLUMN DD_SUBDIVISION2NAME TO SUBDIVISION2NAME;
ALTER TABLE  temp_outlierperproc RENAME COLUMN DD_CATEGORYNAME TO CATEGORYNAME;
ALTER TABLE  temp_outlierperproc RENAME COLUMN DD_FORECASTUNITCODE TO FORECASTUNITCODE ;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_ECCCUSTOMERLEVEL7CODE TO ECCCUSTOMERLEVEL7CODE;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_PLANTCODE TO PLANTCODE;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN CT_ACTUALDELIVEREDQUANTITY TO ACTUALDELIVEREDQUANTITY;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_ACTUALDELIVERYDATE TO ACTUALDELIVERYDATE;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_COUNTY TO COUNTY;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_CTR TO CTR;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_FULL_COUNTRY_NAME TO FULL_COUNTRY_NAME;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_FULL_STATE_NAME TO FULL_STATE_NAME;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_PLACE TO PLACE;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_POSTALCODE TO POSTALCODE;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_RG TO RG;
ALTER TABLE  temp_outlierperproc  RENAME COLUMN DD_STRINGENCYINDEX TO STRINGENCYINDEX;

--- select count(distinct(FORECASTUNITCODE,ECCCUSTOMERLEVEL7CODE,PLANTCODE)) as cnt from DS_ML_Dressings_RES0715

--- select * from DS_ML_Dressings_RES0715 where MODEL_HIERARCHY = '00048001213579_0030026717_3990'

--- select * from temp_outlierperproc where MODEL_HIERARCHY = '00048001213579_0030026717_3990' 
