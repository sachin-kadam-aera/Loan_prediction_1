
/*
insert into stage_fosalesforecastcortex select * from stage_fosalesforecast_10holdout
  where DD_FORECASTRANK >=0*/

/* MRC-886 - Vali -  added ct_salesquantity_original - sales from file, event_category from skill */
/*insert into stage_fosalesforecastcortex 
select t.*, 
       s.sales_original   as ct_salesquantity_original,
       s.dd_event_category
from stage_fosalesforecast_10holdout t
left join saleshistory_fromprd_dfsubjarea_3MHO s
on  t.dd_partnumber = s.DD_PARTNUMBER
and t.dd_SALES_COCD || '|' || t.dd_REPORTING_COMPANY_CODE || '|' || t.dd_HEI_CODE || '|' || t.dd_COUNTRY_DESTINATION_CODE || '|' || t.dd_MARKET_GROUPING = s.COMOP
and trunc( to_date( to_char(t.dd_forecastdate), 'yyyymmdd'), 'mm') =  to_date(to_char(s.yyyymm), 'yyyymm') 
where t.DD_FORECASTRANK >=0*/

DELETE FROM Temp_merck_gross_sales_history
WHERE dd_reportingdate in (SELECT DISTINCT dd_reportingdate FROM Temp_merck_gross_sales);

INSERT INTO Temp_merck_gross_sales_history (DD_REPORTINGDATE,DD_FORECASTDATE,DD_PARTNUMBER,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,CT_SALESQUANTITY,CT_FORECASTQUANTITY,CT_LOWPI,CT_HIGHPI,CT_MAE,DD_LASTDATE,DD_HOLDOUTDATE,DD_FORECASTSAMPLE,DD_FORECASTTYPE,DD_FORECASTRANK,DD_FORECASTMODE,DD_COMPANYCODE,CT_BIAS_ERROR_RANK,CT_BIAS_ERROR,RESIDUALSCORE,FORECASTABILITY)
SELECT DD_REPORTINGDATE,DD_FORECASTDATE,DD_PARTNUMBER,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,CT_SALESQUANTITY,CT_FORECASTQUANTITY,CT_LOWPI,CT_HIGHPI,CT_MAE,DD_LASTDATE,DD_HOLDOUTDATE,DD_FORECASTSAMPLE,DD_FORECASTTYPE,DD_FORECASTRANK,DD_FORECASTMODE,DD_COMPANYCODE,CT_BIAS_ERROR_RANK,CT_BIAS_ERROR,RESIDUALSCORE,FORECASTABILITY FROM Temp_merck_gross_sales;

Create or replace table  stage_fosalesforecastcortex as
select t.*, 
       s.sales_original   as ct_salesquantity_original,
       s.dd_event_category
from Temp_merck_gross_sales t
left join saleshistory_fromprd_dfsubjarea_3MHO  s
on  t.dd_partnumber = s.DD_PARTNUMBER
and t.dd_SALES_COCD || '|' || t.dd_REPORTING_COMPANY_CODE || '|' || t.dd_HEI_CODE || '|' || t.dd_COUNTRY_DESTINATION_CODE || '|' || t.dd_MARKET_GROUPING = s.COMOP
and trunc( to_date( to_char(t.dd_forecastdate), 'yyyymmdd'), 'mm') =  to_date(to_char(s.yyyymm), 'yyyymm') 
where t.DD_FORECASTRANK >=0;

INSERT INTO stage_fosalesforecastcortex (DD_REPORTINGDATE,DD_FORECASTDATE,DD_PARTNUMBER,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,CT_SALESQUANTITY,CT_FORECASTQUANTITY,CT_LOWPI,CT_HIGHPI,CT_MAE,DD_LASTDATE,DD_HOLDOUTDATE,DD_FORECASTSAMPLE,DD_FORECASTTYPE,DD_FORECASTRANK,DD_FORECASTMODE,DD_COMPANYCODE,CT_BIAS_ERROR_RANK,CT_BIAS_ERROR,RESIDUALSCORE,FORECASTABILITY,CT_SALESQUANTITY_ORIGINAL,DD_EVENT_CATEGORY)
 SELECT DD_REPORTINGDATE,to_char(ADD_YEARS(to_date(DD_FORECASTDATE,'YYYYMMDD'),1),'YYYYMMDD') AS DD_FORECASTDATE,DD_PARTNUMBER,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,
 DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,CT_SALESQUANTITY,CT_FORECASTQUANTITY,
 CT_LOWPI,CT_HIGHPI,CT_MAE,DD_LASTDATE,DD_HOLDOUTDATE,DD_FORECASTSAMPLE,DD_FORECASTTYPE,DD_FORECASTRANK,
 DD_FORECASTMODE,DD_COMPANYCODE,CT_BIAS_ERROR_RANK,CT_BIAS_ERROR,RESIDUALSCORE,FORECASTABILITY,
 CT_SALESQUANTITY_ORIGINAL,DD_EVENT_CATEGORY
 FROM stage_fosalesforecastcortex 
 WHERE substr(DD_FORECASTDATE,1,6) = to_char(ADD_YEARS(CURRENT_DATE,2),'YYYYMM');

ALTER TABLE stage_fosalesforecastcortex RENAME COLUMN CT_MAE TO CT_MAPE;
ALTER TABLE stage_fosalesforecastcortex ADD DD_FORECASTAPPROACH VARCHAR(12) UTF8;

/*15 Feb 2018 Georgiana Changes according to App - 5981*/
/*Round Forecast Quantity*/

UPDATE merck.stage_fosalesforecastcortex
SET ct_forecastquantity=ceil(ct_forecastquantity);

update stage_fosalesforecastcortex
set ct_salesquantity_original = NULL
where ct_salesquantity_original = 0
AND DD_FORECASTSAMPLE = 'Test';

/*Recalculate Mapes based on the new forecast quantities*/
/*DROP TABLE IF EXISTS tmp_recalculatemape*/
/* commented out to use mape from DS team
CREATE TABLE tmp_recalculatemape
AS
SELECT
f.dd_reportingdate,dd_partnumber,dd_sales_cocd,f.dd_REPORTING_COMPANY_CODE,
f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING,
dd_forecasttype,
100 * avg(abs((ct_forecastquantity-ct_salesquantity)/ct_salesquantity)) ct_mape_fofcst
FROM stage_fosalesforecastcortex f
WHERE dd_forecastsample = 'Test'
GROUP BY
f.dd_reportingdate,dd_partnumber,dd_sales_cocd,f.dd_REPORTING_COMPANY_CODE,
f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING,
dd_forecasttype*/

/* commented out to use mape from DS team
update stage_fosalesforecastcortex f
set f.ct_mape = t.ct_mape_fofcst
from tmp_recalculatemape t ,stage_fosalesforecastcortex f
where f.dd_reportingdate=t.dd_reportingdate
and f.dd_partnumber=t.dd_partnumber
and t.dd_sales_cocd=f.dd_sales_cocd
and f.dd_REPORTING_COMPANY_CODE =t.dd_REPORTING_COMPANY_CODE
and f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
and f.DD_HEI_CODE=t.DD_HEI_CODE
and f.DD_MARKET_GROUPING=t.DD_MARKET_GROUPING
and f.dd_forecasttype=t.dd_forecasttype*/


/*Ranks Recalculation based on the new mapes*/
/*DROP TABLE IF EXISTS tmp_recalculaterank
CREATE TABLE tmp_recalculaterank
AS
SELECT row_number() over(partition by f.dd_reportingdate,dd_partnumber,dd_sales_cocd,f.dd_REPORTING_COMPANY_CODE,
f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING
 order by ct_mape,dd_forecasttype asc) dd_rank,
f.dd_reportingdate,dd_partnumber,dd_sales_cocd,f.dd_REPORTING_COMPANY_CODE,
f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING,dd_forecasttype
FROM (
SELECT DISTINCT f.dd_reportingdate,dd_partnumber,dd_sales_cocd,f.dd_REPORTING_COMPANY_CODE,
f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING,f.dd_forecasttype, f.ct_mape
FROM stage_fosalesforecastcortex f
WHERE f.dd_forecastsample = 'Test') f

UPDATE stage_fosalesforecastcortex f
SET dd_forecastrank=dd_rank
FROM stage_fosalesforecastcortex  f,tmp_recalculaterank t
where f.dd_reportingdate=t.dd_reportingdate
and f.dd_partnumber=t.dd_partnumber
and t.dd_sales_cocd=f.dd_sales_cocd
and f.dd_REPORTING_COMPANY_CODE =t.dd_REPORTING_COMPANY_CODE
and f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
and f.DD_HEI_CODE=t.DD_HEI_CODE
and f.DD_MARKET_GROUPING=t.DD_MARKET_GROUPING
and f.dd_forecasttype=t.dd_forecasttype
and f.dd_forecastrank <> t.dd_rank
*/

 UPDATE stage_fosalesforecastcortex f
SET f.ct_mape = 0 where f.ct_mape is null;

/* 15 Feb 2018 End of changes*/

DROP TABLE IF EXISTS tmp_forecastcov1;
CREATE TABLE tmp_forecastcov1
AS
select DD_PARTNUMBER,DD_SALES_COCD,DD_HEI_CODE,DD_REPORTING_COMPANY_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,
dd_forecastdate,dd_forecastrank,dd_forecasttype,dd_reportingdate,
ct_forecastquantity
FROM stage_fosalesforecastcortex s
WHERE s.dd_forecastsample in ('Test','Horizon');

UPDATE tmp_forecastcov1
SET ct_forecastquantity = 1
WHERE ct_forecastquantity = 0;

DROP TABLE IF EXISTS tmp_forecastcov;
CREATE TABLE tmp_forecastcov
as
SELECT DD_PARTNUMBER,DD_SALES_COCD,DD_HEI_CODE,DD_REPORTING_COMPANY_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,
dd_forecastrank,dd_forecasttype,dd_reportingdate,
(stddev(ct_forecastquantity) / avg(ct_forecastquantity)) ct_variationcoeff
FROM tmp_forecastcov1
GROUP BY DD_PARTNUMBER,DD_SALES_COCD,DD_HEI_CODE,DD_REPORTING_COMPANY_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,
dd_forecastrank,dd_forecasttype,dd_reportingdate;


/* Get all parts-plant-mkt.. which have atleast 1 non-straightline forecast e.g CoV > 1% */

DROP TABLE IF EXISTS tmp_atleast1nonstlinefcst;
CREATE TABLE tmp_atleast1nonstlinefcst
AS
SELECT DISTINCT DD_PARTNUMBER,DD_SALES_COCD,DD_HEI_CODE,DD_REPORTING_COMPANY_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,dd_reportingdate
FROM tmp_forecastcov
WHERE ct_variationcoeff > 0.01;

/* Delete all ranks where there's a straight line forecast */
/*DELETE FROM stage_fosalesforecastcortex s
WHERE EXISTS ( SELECT 1 FROM tmp_atleast1nonstlinefcst l
WHERE l.DD_PARTNUMBER = s.DD_PARTNUMBER AND l.DD_SALES_COCD = s.DD_SALES_COCD AND l.DD_REPORTING_COMPANY_CODE = s.DD_REPORTING_COMPANY_CODE
AND l.DD_HEI_CODE = s.DD_HEI_CODE AND l.DD_COUNTRY_DESTINATION_CODE = s.DD_COUNTRY_DESTINATION_CODE AND l.DD_MARKET_GROUPING = s.DD_MARKET_GROUPING
and l.dd_reportingdate=s.dd_reportingdate)
AND EXISTS ( SELECT 1 FROM tmp_forecastcov t
WHERE t.DD_PARTNUMBER = s.DD_PARTNUMBER AND t.DD_SALES_COCD = s.DD_SALES_COCD AND t.DD_REPORTING_COMPANY_CODE = s.DD_REPORTING_COMPANY_CODE
AND t.DD_HEI_CODE = s.DD_HEI_CODE AND t.DD_COUNTRY_DESTINATION_CODE = s.DD_COUNTRY_DESTINATION_CODE AND t.DD_MARKET_GROUPING = s.DD_MARKET_GROUPING
AND t.dd_forecastrank = s.dd_forecastrank
AND t.dd_forecasttype = s.dd_forecasttype
and t.dd_reportingdate=s.dd_reportingdate
AND t.ct_variationcoeff <= 0.01)*/

/* Explicitly delete the 4 methods which always give st. line forecasts */
/*DELETE FROM stage_fosalesforecastcortex s
WHERE (DD_FORECASTTYPE in ('Auto-Regressive','ARMA','Auto ETS')
OR DD_FORECASTTYPE like 'Croston%')

DROP TABLE IF EXISTS tmp_partvsmape
CREATE TABLE tmp_partvsmape
AS
SELECT DISTINCT DD_PARTNUMBER,DD_SALES_COCD,DD_HEI_CODE,DD_REPORTING_COMPANY_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,
s.dd_reportingdate, s.dd_forecasttype,s.dd_forecastrank,ct_mape
FROM stage_fosalesforecastcortex s
WHERE dd_forecastsample = 'Test'

DROP TABLE IF EXISTS tmp_partvsmape_rerank
CREATE TABLE tmp_partvsmape_rerank
AS
SELECT t.*,rank() over(partition by DD_PARTNUMBER,DD_SALES_COCD,DD_HEI_CODE,DD_REPORTING_COMPANY_CODE,DD_COUNTRY_DESTINATION_CODE,DD_MARKET_GROUPING,
    t.dd_reportingdate order by ct_mape,dd_forecasttype ) dd_rank
FROM tmp_partvsmape t

UPDATE stage_fosalesforecastcortex s
SET s.dd_forecastrank = t.dd_rank
FROM stage_fosalesforecastcortex s,tmp_partvsmape_rerank t
WHERE t.DD_PARTNUMBER = s.DD_PARTNUMBER AND t.DD_SALES_COCD = s.DD_SALES_COCD AND t.DD_REPORTING_COMPANY_CODE = s.DD_REPORTING_COMPANY_CODE
AND t.DD_HEI_CODE = s.DD_HEI_CODE AND t.DD_COUNTRY_DESTINATION_CODE = s.DD_COUNTRY_DESTINATION_CODE AND t.DD_MARKET_GROUPING = s.DD_MARKET_GROUPING
AND s.dd_reportingdate = t.dd_reportingdate
AND s.dd_forecasttype = t.dd_forecasttype*/

/* Cap High PI and Bias errors */
UPDATE stage_fosalesforecastcortex
SET ct_highpi = 1000000
where ct_highpi > 1000000;
UPDATE stage_fosalesforecastcortex
SET ct_bias_error = 1000000
where ct_bias_error > 1000000;

/* Update future sales to NULL */
update stage_fosalesforecastcortex
set ct_salesquantity = NULL
where ct_salesquantity = 0
AND DD_FORECASTSAMPLE = 'Horizon';

/* Update highpi and lowpi to NULL for dates before holdout date */
UPDATE stage_fosalesforecastcortex f
set ct_highpi = NULL
WHERE ct_highpi = 0
AND dd_forecastsample = 'Train';

UPDATE stage_fosalesforecastcortex f
set ct_lowpi = NULL
WHERE ct_lowpi = 0
AND dd_forecastsample = 'Train';

/*DELETE FROM stage_fosalesforecastcortex
WHERE ct_forecastquantity > 1000000*/



/* Staging data cleaned up > St. line forecasts have been removed and reranking is done. Each part should have only 1 forecast type for a given rank */
/* Exception: There might still be some st. line forecasts left. For example when all forecast types gave a st. line */

/***************************************************************/
/*** Populate fact from staging table                        ***/
/*  1. Delete all rows with same reporting date as in STG      */
/*  2. Update dimension ids                                    */
/***************************************************************/

DROP TABLE IF EXISTS tmp_maxrptdate_cortex;
CREATE TABLE tmp_maxrptdate_cortex
as
SELECT DISTINCT TO_DATE(dd_reportingdate,'DD MON YYYY') dd_reportingdate
from stage_fosalesforecastcortex ;

/* MRC-1056 Vali add pre-check for next 24 forecast months */
CREATE OR REPLACE TABLE stage_deleted_fosalesforecastcortex 
AS
SELECT 
	dd_partnumber,
	dd_sales_cocd,
	dd_reportingdate,
	dd_reporting_company_code,
	dd_forecastrank,
	dd_country_destination_code 
	,count(dd_forecastdate) ct_forecast_months
FROM stage_fosalesforecastcortex
WHERE to_date(to_char(dd_forecastdate) , 'yyyymmdd') between trunc(to_date(dd_reportingdate, 'DD MON YYYY'), 'mm') and add_months( trunc(to_date(dd_reportingdate, 'DD MON YYYY'), 'mm'), 36)
GROUP BY  dd_partnumber,
		  dd_sales_cocd,
		  dd_reportingdate,
		  dd_reporting_company_code,
		  dd_forecastrank,
		  dd_country_destination_code
HAVING COUNT(dd_forecastdate) < 36;

DELETE FROM stage_fosalesforecastcortex s
WHERE (s.dd_partnumber, s.dd_sales_cocd, s.dd_reporting_company_code, s.dd_country_destination_code) 
IN ( SELECT  dd_partnumber,
		     dd_sales_cocd,
			 dd_reporting_company_code,
			 dd_country_destination_code
	FROM stage_deleted_fosalesforecastcortex); 

/* Delete rows from fact_fosalesforecastcortex where reporting date matches that in stage_fosalesforecastcortex and re-populate from stage_fosalesforecastcortex */

DELETE FROM fact_fosalesforecastcortex f
WHERE EXISTS ( SELECT 1 FROM tmp_maxrptdate_cortex t WHERE TO_DATE(f.dd_reportingdate,'DD MON YYYY') = t.dd_reportingdate );

drop table if exists tmp_saleshistory_grain_reqmonths;
drop table if exists tmp_saleshistory_grain_reqmonths_2;

drop table if exists fact_fosalesforecastcortex_temp;
create table fact_fosalesforecastcortex_temp as
select * from fact_fosalesforecastcortex WHERE 1=2;

alter table fact_fosalesforecastcortex_temp add column dd_forecastdatevalue date default '1900-01-01';

delete from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex';

insert into number_fountain
select  'fact_fosalesforecastcortex',
ifnull(max(d.fact_fosalesforecastcortexid),
	ifnull((select min(s.dim_projectsourceid * s.multiplier)
			from dim_projectsource s),0))
from fact_fosalesforecastcortex d
WHERE d.fact_fosalesforecastcortexid <> 1;

insert into fact_fosalesforecastcortex_temp
(
fact_fosalesforecastcortexid,
DD_PARTNUMBER,      
dd_plantcode,       
dd_SALES_COCD,      
dd_REPORTING_COMPANY_CODE,  
dd_home_vs_export,          
dd_HEI_CODE,                
dd_COUNTRY_DESTINATION_CODE,    
DD_MARKET_COUNTRY,              
dd_MARKET_GROUPING,             
dim_partid,
dim_plantid,
dd_reportingdate,
dim_dateidreporting,
dd_forecasttype,
dd_forecastsample,
dd_forecastdate,
dim_dateidforecast,
ct_salesquantity,
ct_forecastquantity,
ct_lowpi,
ct_highpi,
ct_mape,
dd_forecastrank,
dd_holdoutdate,
dd_lastdate,
dd_forecastmode,
dd_forecastdatevalue,
ct_bias_error,
ct_bias_error_rank,
dd_forecastapproach,
ct_residualscore,
dd_forecastability,
ct_transformedsales,
dd_event_category
)
select  (select ifnull(m.max_id, 0) from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex')
+ row_number() over(order by dd_partnumber,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,dd_reportingdate,dd_forecastdate,dd_forecasttype) as fact_fosalesforecastcortexid,
ifnull(DD_PARTNUMBER,'Not Set'),
ifnull(dd_SALES_COCD,'Not Set') dd_plantcode,
ifnull(dd_SALES_COCD,'Not Set') dd_SALES_COCD,  /* Dup of dd_plantcode */
ifnull(dd_REPORTING_COMPANY_CODE,'Not Set'),
ifnull(dd_HEI_CODE,'Not Set') dd_home_vs_export,
ifnull(dd_HEI_CODE,'Not Set') dd_HEI_CODE,  /* Dup of dd_home_vs_export */
ifnull(dd_COUNTRY_DESTINATION_CODE,'Not Set'),
ifnull(dd_MARKET_GROUPING,'Not Set') DD_MARKET_COUNTRY,
ifnull(dd_MARKET_GROUPING,'Not Set') dd_MARKET_GROUPING,    /* Dup of DD_MARKET_COUNTRY */
ifnull((select min(dim_partid) from dim_part dp where dp.partnumber = sf.dd_partnumber and dp.plant = sf.dd_SALES_COCD),1) dim_partid,
ifnull((select min(dim_plantid) from dim_plant pl where pl.plantcode = sf.dd_SALES_COCD),1) dim_plantid,
sf.dd_reportingdate ,
ifnull((select min(dim_dateid) from dim_date d where d.companycode = 'Not Set' and d.datevalue = TO_DATE(sf.dd_reportingdate,'DD MON YYYY')),1) as dim_dateidreporting,
ifnull(sf.dd_forecasttype,'Not Set'),
ifnull(sf.dd_forecastsample,'Not Set'),
ifnull(sf.dd_forecastdate,1),
1 as dim_dateidforecast,
sf.ct_salesquantity_original,
sf.ct_forecastquantity,
sf.ct_lowpi,
sf.ct_highpi,
sf.ct_mape,
ifnull(sf.dd_forecastrank,0),
ifnull(sf.dd_holdoutdate,'1'),
ifnull(sf.dd_lastdate,'1'),
ifnull(sf.dd_forecastmode,'Not Set'),
case when sf.dd_forecastdate is null then cast('1900-01-01' as date)
else cast(concat(substring(sf.dd_forecastdate,1,4) , '-' ,
substring(sf.dd_forecastdate,5,2) , '-' ,
substring(sf.dd_forecastdate,7,2) ) as date)
end dd_forecastdatevalue,
ct_bias_error,
ct_bias_error_rank,
dd_forecastapproach,
ifnull(RESIDUALSCORE,0) as ct_residualscore,
ifnull(FORECASTABILITY,'Not Set') as dd_forecastability,
(case when dd_SALES_COCD = 'GB20' then 0 else ct_salesquantity end) as ct_transformedsales,
dd_event_category
from stage_fosalesforecastcortex sf;

UPDATE fact_fosalesforecastcortex_temp f
SET f.dim_dateidforecast = d.dim_dateid
from (select datevalue,min(dim_dateid) dim_dateid
	        from dim_date d  where d.companycode = 'Not Set' group by datevalue)d,fact_fosalesforecastcortex_temp f
WHERE f.dd_forecastdatevalue = d.datevalue AND f.dim_dateidforecast <> d.dim_dateid;

/* Create table for autoforecast functionality*/
CREATE OR REPLACE TABLE TMP_AUTOFORECAST_ON AS
SELECT
	distinct dd_partnumber,
	dd_plantcode,
	dd_reporting_company_code,
	dd_country_destination_code,
	dd_autopredictive,
	max(datevalue) as dd_reportingdate
from
	fact_fosalesforecastcortex,
	dim_date d
where d.dim_dateid = dim_dateidreporting
 and dd_forecastrank = 1
 and dd_autopredictive = 'On'
 and to_char(datevalue,'YYYYMM') = to_char(add_months(CURRENT_DATE,-1),'YYYYMM')
 AND dd_forecastdate > to_number(to_char(CURRENT_DATE,'YYYYMMDD'))
 group by dd_partnumber,
	dd_plantcode,
	dd_reporting_company_code,
	dd_country_destination_code,
	dd_autopredictive;

/* MRC-704 -Octavian S - Create table for DS vs FF history functionality*/
CREATE OR REPLACE TABLE TMP_DS_vs_FF AS
SELECT
	DISTINCT DD_PARTNUMBER,
	DD_PLANTCODE,
	DD_REPORTING_COMPANY_CODE,
	DD_COUNTRY_DESTINATION_CODE,
	DD_COMMNENTS_DFVSFF,
	DD_REASON_OF_DIFFERENCE_DFVSFF,
	max(datevalue) as dd_reportingdate
FROM
	FACT_FOSALESFORECASTCORTEX,
	dim_date d
WHERE d.dim_dateid = dim_dateidreporting
 AND DD_FORECASTRANK = 1
 AND (IFNULL(DD_COMMNENTS_DFVSFF,'999') <> '999'
 OR IFNULL(DD_REASON_OF_DIFFERENCE_DFVSFF ,'999') <> '999')
 group by DD_PARTNUMBER,
	DD_PLANTCODE,
	DD_REPORTING_COMPANY_CODE,
	DD_COUNTRY_DESTINATION_CODE,
	DD_COMMNENTS_DFVSFF,
	DD_REASON_OF_DIFFERENCE_DFVSFF;

 CREATE OR REPLACE TABLE TMP_DF_TO_FF AS
SELECT
	DISTINCT
	dd_partnumber,
	dd_plantcode,
	dd_reporting_company_code,
	dd_country_destination_code,
    dd_forecastdate,
    CT_SUPPLY_CONSTRAINT,
    CT_NEW_PRODUCT,
    CT_COMMERCIAL_RISK,
    CT_FINANCIAL_ADJUSTMENT
from
	fact_fosalesforecastcortex
WHERE dd_forecastrank = 1 
 AND to_char(to_date(DD_REPORTINGDATE,'dd mon yyyy'),'YYYYMM') = to_char(CURRENT_DATE - interval '1' MONTH,'YYYYMM');
   

CREATE OR REPLACE TABLE TMP_NASP_CHANGED AS
SELECT
    DISTINCT
    dd_partnumber,
    dd_plantcode,
    dd_reporting_company_code,
    dd_country_destination_code,
    dd_forecastdate,
    CT_NASP_PRA_ADJUSTMENT
from
    fact_fosalesforecastcortex
WHERE dd_forecastrank = 1
  AND to_char(to_date(DD_REPORTINGDATE,'dd mon yyyy'),'YYYYMM') = to_char(CURRENT_DATE - interval '1' MONTH,'YYYYMM');

insert into fact_fosalesforecastcortex
(
fact_fosalesforecastcortexid,
DD_PARTNUMBER,
dd_plantcode,
dd_SALES_COCD,
dd_REPORTING_COMPANY_CODE,
dd_home_vs_export,
dd_HEI_CODE,
dd_COUNTRY_DESTINATION_CODE,
DD_MARKET_COUNTRY,
dd_MARKET_GROUPING,
dim_partid,
dim_plantid,
dd_companycode,
dd_reportingdate,
dim_dateidreporting,
dd_forecasttype,
dd_forecastsample,
dd_forecastdate,
dim_dateidforecast,
ct_salesquantity,
ct_forecastquantity,
ct_lowpi,
ct_highpi,
ct_mape,
dd_forecastrank,
dd_holdoutdate,
dd_lastdate,
dd_forecastmode,
ct_bias_error,
ct_bias_error_rank,
dd_forecastapproach,
CT_RESIDUALSCORE,
DD_FORECASTABILITY,
ct_transformedsales,
dd_event_category
)
select
fact_fosalesforecastcortexid,
DD_PARTNUMBER,
dd_plantcode,
dd_SALES_COCD,
dd_REPORTING_COMPANY_CODE,
dd_home_vs_export,
dd_HEI_CODE,
dd_COUNTRY_DESTINATION_CODE,
DD_MARKET_COUNTRY,
dd_MARKET_GROUPING,
dim_partid,
dim_plantid,
dd_companycode,
dd_reportingdate,
dim_dateidreporting,
dd_forecasttype,
dd_forecastsample,
dd_forecastdate,
dim_dateidforecast,
ct_salesquantity,
ct_forecastquantity,
ct_lowpi,
ct_highpi,
ct_mape,
dd_forecastrank,
dd_holdoutdate,
dd_lastdate,
dd_forecastmode,
ct_bias_error,
ct_bias_error_rank,
dd_forecastapproach,
CT_RESIDUALSCORE,
DD_FORECASTABILITY,
ct_transformedsales,
dd_event_category
from fact_fosalesforecastcortex_temp;


/* Format reporting date as DD MON YYYY */
UPDATE fact_fosalesforecastcortex f
set f.dd_reportingdate = to_char(to_date(f.dd_reportingdate,'YYYY-MM-DD') , 'DD MON YYYY')
where f.dd_reportingdate like '%-%-%';

UPDATE fact_fosalesforecastcortex f
SET dd_latestreporting = 'No'
WHERE dd_latestreporting <> 'No';

UPDATE fact_fosalesforecastcortex f
SET dd_latestreporting = 'Yes'
FROM tmp_maxrptdate_cortex r,fact_fosalesforecastcortex f
where DATE_TRUNC('month',  TO_DATE(f.dd_reportingdate,'DD MON YYYY')) =DATE_TRUNC('month',  r.dd_reportingdate)
and  dd_forecastapproach is null ;

/* BI-5598 - OZ,GN,MH - Fixes for several columns in Sales Forecast */
UPDATE fact_fosalesforecastcortex f
SET f.dim_dateidforecast = dnew.dim_dateid
from dim_date dold,fact_fosalesforecastcortex f, dim_plant pl, dim_date dnew,tmp_maxrptdate_cortex r
WHERE f.dim_dateidforecast = dold.dim_dateid
and dnew.datevalue = dold.datevalue
AND f.dim_plantid = pl.dim_plantid
AND dnew.plantcode_factory = pl.plantcode
AND dnew.companycode = pl.companycode
AND dold.plantcode_factory = 'Not Set'
AND dold.companycode = 'Not Set'
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND f.dim_dateidforecast <> dnew.dim_dateid;


update fact_fosalesforecastcortex f
set f.dim_partid = dp.dim_partid
from fact_fosalesforecastcortex f, dim_part dp,tmp_maxrptdate_cortex r
where f.dd_partnumber = dp.partnumber
AND CASE WHEN f.dd_plantcode = 'NL10' then 'XX20' ELSE f.dd_plantcode END = dp.plant
AND f.dim_partid = 1
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND f.dim_partid <> dp.dim_partid;

update fact_fosalesforecastcortex f
set f.dim_plantid = pl.dim_plantid
from fact_fosalesforecastcortex f, dim_part dp, dim_plant pl,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
AND f.dd_plantcode = 'NL10'
AND dp.plant = pl.plantcode
and dp.plant = 'XX20'
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and f.dim_plantid <> pl.dim_plantid;

update fact_fosalesforecastcortex f
set f.dd_plantcode = pl.plantcode
from fact_fosalesforecastcortex f, dim_plant pl, tmp_maxrptdate_cortex r
where f.dim_plantid = pl.dim_plantid
and pl.plantcode = 'XX20'
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and f.dd_plantcode <> pl.plantcode;

update fact_fosalesforecastcortex f
set dd_country_destination_code = trim(dd_country_destination_code)
where dd_country_destination_code <> trim(dd_country_destination_code);

update fact_fosalesforecastcortex f
set dd_market_grouping = trim(dd_market_grouping)
where dd_market_grouping <> trim(dd_market_grouping);

merge into fact_fosalesforecastcortex a
using (
select distinct b.dim_partid, min(b.dd_market_grouping) over (partition by b.dim_partid) as market_grouping
from fact_fosalesforecastcortex b
where b.dd_country_destination_code is not null
) t on a.dim_partid = t.dim_partid
when matched then update set a.dd_market_grouping = t.market_grouping
where a.dd_market_grouping is null and a.dd_country_destination_code is null;
/* BI-5598 - OZ,GN,MH - Fixes for several columns in Sales Forecast */


/***************************************************************/
/*** Post-processing data in fact table                        */
/*  1. Update selling price, q-o-q measures                    */
/*  2. Update customer forecast and related measures           */
/*  3. Any other additional measures                           */
/***************************************************************/


/* Update amt_sellingpriceperunit_gbl for each part-plant pair as avg selling price over last 1 year (from reporting date) */
DROP TABLE IF EXISTS tmp_amt_sellingpriceperunit_gbl;
CREATE TABLE tmp_amt_sellingpriceperunit_gbl
AS
SELECT dp.partnumber,pl.plantcode,avg(fso.amt_UnitPrice * amt_exchangerate_gbl) amt_sellingpriceperunit_gbl_oldmethod,
avg(amt_exchangerate_gbl * amt_UnitPriceUoM/(CASE WHEN fso.ct_PriceUnit <> 0 THEN fso.ct_PriceUnit ELSE 1 END)) amt_sellingpriceperunit_gbl
FROM fact_salesorder fso inner join dim_part dp on dp.dim_partid = fso.dim_partid
INNER JOIN dim_date d on d.dim_dateid = fso.dim_dateidsocreated
left outer join dim_currency tra on tra.dim_currencyid = fso.dim_currencyid_tra
left outer join dim_currency lcl on lcl.dim_currencyid = fso.dim_currencyid
left outer join dim_currency gbl on tra.dim_currencyid = fso.dim_currencyid_gbl
inner join dim_plant pl on pl.dim_plantid = fso.dim_plantid,
tmp_maxrptdate_cortex r
WHERE d.datevalue >= r.dd_reportingdate - interval '1' year
AND ct_ScheduleQtySalesUnit > 0
group by dp.partnumber,pl.plantcode;

UPDATE fact_fosalesforecastcortex f
SET f.amt_sellingpriceperunit = t.amt_sellingpriceperunit_gbl
FROM fact_fosalesforecastcortex f,tmp_amt_sellingpriceperunit_gbl t,tmp_maxrptdate_cortex r
WHERE f.dd_partnumber = t.partnumber
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND f.dd_plantcode = t.plantcode;

DROP TABLE IF EXISTS tmp_qoqmeasures;
CREATE TABLE tmp_qoqmeasures
AS
SELECT f.dd_reportingdate,dd_forecasttype,dd_partnumber,dd_plantcode,dd_market_country,dd_home_vs_export,
CALENDARQUARTERID,sum(CT_SALESQUANTITY) CT_SALESQUANTITY,
sum(CT_FORECASTQUANTITY) CT_FORECASTQUANTITY
FROM fact_fosalesforecastcortex f inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
INNER JOIN tmp_maxrptdate_cortex r ON TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
GROUP BY f.dd_reportingdate,dd_forecasttype,dd_partnumber,dd_plantcode,dd_market_country,dd_home_vs_export,CALENDARQUARTERID;

DROP TABLE IF EXISTS tmp_qoqmeasures_2;
CREATE TABLE tmp_qoqmeasures_2
AS
SELECT f.*,row_number() over(partition by dd_reportingdate,dd_forecasttype,dd_partnumber,dd_plantcode,dd_market_country,dd_home_vs_export order by CALENDARQUARTERID) qtr_no
from tmp_qoqmeasures f;

DROP TABLE IF EXISTS tmp_qoqmeasures_3;
CREATE TABLE tmp_qoqmeasures_3
AS
SELECT DISTINCT f.*,t.CT_SALESQUANTITY CT_SALESQUANTITY_prevqtr,t.CT_FORECASTQUANTITY CT_FORECASTQUANTITY_prevqtr
FROM tmp_qoqmeasures_2 f,tmp_qoqmeasures_2 t
WHERE f.dd_reportingdate = t.dd_reportingdate AND f.dd_forecasttype = t.dd_forecasttype
AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode AND f.dd_market_country = t.dd_market_country AND f.dd_home_vs_export = t.dd_home_vs_export
AND f.qtr_no = t.qtr_no + 1;

UPDATE fact_fosalesforecastcortex f
SET f.CT_SALES_PREVQTR = t.CT_SALESQUANTITY_prevqtr,
f.CT_FORECAST_PREVQTR = t.CT_FORECASTQUANTITY_prevqtr,
f.CT_SALES_CURRENTQTR = t.CT_SALESQUANTITY,
f.CT_FORECAST_CURRENTQTR = t.CT_FORECASTQUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_qoqmeasures_3 t,tmp_maxrptdate_cortex r
WHERE f.dd_reportingdate = t.dd_reportingdate
AND f.dd_forecasttype = t.dd_forecasttype
AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode AND f.dd_market_country = t.dd_market_country AND f.dd_home_vs_export = t.dd_home_vs_export
AND f.dim_dateidforecast = d.dim_dateid
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND d.CALENDARQUARTERID = t.CALENDARQUARTERID;

/* Update customer forecast */

/*09 Feb 2017 Georgiana changes according to BI-5459*/
/* Horizon Forecast Sample*/
drop table if exists tmp_for_upd_customerquantity;
create table tmp_for_upd_customerquantity as
select distinct fact_fosalesforecastcortexid,dim_dateidforecast,f.dd_reportingdate,d.calendarmonthid as focalendarmonthid,dd_country_destination_code,
year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) as repcalendarmonthid,
/*f.DD_MARKET_GROUPING,*/f.dd_partnumber,f.dd_plantcode,f.dd_REPORTING_COMPANY_CODE
FROM fact_fosalesforecastcortex f
inner join tmp_maxrptdate_cortex r on TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
inner join dim_date d on f.dim_dateidforecast = d.dim_dateid
where dd_forecastsample='Horizon';

/*This will include only NL10 plant*/
/*UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
AND f.dd_plantcode='NL10'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/

drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select distinct fact_fosalesforecastcortexid, t.PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode='NL10';

update fact_fosalesforecastcortex f
set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
from
fact_fosalesforecastcortex f, tmp_for_upd_customerquantity2 t
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;


/*XX20 separate update*/
/*UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = case when t.PLANT_CODE ='NL10' then 'XX20' end AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and f.dd_plantcode='XX20'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/

drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select distinct fact_fosalesforecastcortexid, t.PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/ and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode='XX20';

update fact_fosalesforecastcortex f
set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
from
fact_fosalesforecastcortex f, tmp_for_upd_customerquantity2 t
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;


/*This update is for USA0 plants*/

/*UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = 'USA'
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code in ( 'USA0')
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/

drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select distinct fact_fosalesforecastcortexid, t.PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE /* AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE */
/*AND  f.DD_MARKET_GROUPING = 'USA'*/ and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode='USA0';

update fact_fosalesforecastcortex f
set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY 
from
fact_fosalesforecastcortex f, tmp_for_upd_customerquantity2 t
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;


drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select  fact_fosalesforecastcortexid, max(t.PRA_QUANTITY) as PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING */and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode not in ( 'NL10','USA0')
group by fact_fosalesforecastcortexid;

update fact_fosalesforecastcortex f
set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
from fact_fosalesforecastcortex f, tmp_for_upd_customerquantity2 t
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;

drop table if exists tmp_for_upd_customerquantity2;

/*Train and Test Forecast sample*/


/*Train and Test Forecast sample*/
/*This will include NL10 plant*/

drop table if exists tmp_for_upd_customerquantity;
create table tmp_for_upd_customerquantity as
select distinct fact_fosalesforecastcortexid,dim_dateidforecast,f.dd_reportingdate,d.calendarmonthid as focalendarmonthid,dd_country_destination_code,
year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) as repcalendarmonthid
/*f.DD_MARKET_GROUPING*/,f.dd_partnumber,f.dd_plantcode,f.dd_REPORTING_COMPANY_CODE
FROM fact_fosalesforecastcortex f
inner join tmp_maxrptdate_cortex r on TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
inner join dim_date d on f.dim_dateidforecast = d.dim_dateid
where dd_forecastsample in ('Train','Test');

drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select distinct fact_fosalesforecastcortexid, t.PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/ and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode='NL10';

update fact_fosalesforecastcortex f
set f.ct_forecastquantity_customer = t.PRA_QUANTITY
from tmp_for_upd_customerquantity2 t,fact_fosalesforecastcortex f
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;

/*UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and dd_forecastsample in ('Train','Test')
and f.dd_plantcode = 'NL10'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/


/*This update is for USA10 plants*/

/*UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = 'USA'
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code in ( 'USA0')
and dd_forecastsample in ('Train','Test')
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/


drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select distinct fact_fosalesforecastcortexid, t.PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE /* AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE */
/*AND  f.DD_MARKET_GROUPING = 'USA' */ and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode='USA0';

update fact_fosalesforecastcortex f
set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
from
fact_fosalesforecastcortex f, tmp_for_upd_customerquantity2 t
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;



/*This update is for the rest of plants*/

/*UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code not in ( 'NL10','USA0')
and dd_forecastsample in ('Train','Test')
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/


/*merge into fact_fosalesforecastcortex f
using (select distinct f.fact_fosalesforecastcortexid,t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code not in ( 'NL10','USA0')
and dd_forecastsample in ('Train','Test')
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY*/

drop table if exists tmp_for_upd_customerquantity2;
create table tmp_for_upd_customerquantity2 as
select distinct fact_fosalesforecastcortexid, max(t.PRA_QUANTITY) as PRA_QUANTITY
from tmp_for_upd_customerquantity f,atlas_forecast_pra_forecasts_merck_DC t
where
f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/ and f.repcalendarmonthid=t.PRA_REPORTING_PERIOD and f.focalendarmonthid=t.pra_forecasting_period
and dd_plantcode not in ( 'NL10','USA0')
group by fact_fosalesforecastcortexid;

update fact_fosalesforecastcortex f
set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
from fact_fosalesforecastcortex f, tmp_for_upd_customerquantity2 t
where f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid;

drop table if exists tmp_for_upd_customerquantity2;


/*09 Feb 2017 End of changes*/


DROP TABLE IF EXISTS merck.tmp_custmapes_fosp;
CREATE TABLE merck.tmp_custmapes_fosp
AS
SELECT f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING,dd_forecasttype,
100 * avg(abs((ct_forecastquantity_customer-ct_salesquantity)/ct_salesquantity)) ct_mape_customerfcst
FROM merck.fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE f.DD_FORECASTSAMPLE = 'Test'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
GROUP BY f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,f.DD_MARKET_GROUPING,dd_forecasttype;


UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_mape_customerfcst = t.ct_mape_customerfcst
FROM merck.fact_fosalesforecastcortex f,merck.tmp_custmapes_fosp t
WHERE f.dd_reportingdate = t.dd_reportingdate AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.DD_HEI_CODE AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING
AND f.dd_forecasttype = t.dd_forecasttype;

/* Other measures */
update fact_fosalesforecastcortex f set ct_ratio_mape = cT_MAPE_CUSTOMERFCST/ct_mape where ct_mape > 0 and cT_MAPE_CUSTOMERFCST > 0
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') in (select r.dd_reportingdate from tmp_maxrptdate_cortex r);
update fact_fosalesforecastcortex f set ct_ratio_mape = 0.01 where ct_ratio_mape = 0
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') in (select r.dd_reportingdate from tmp_maxrptdate_cortex r);

DROP TABLE IF EXISTS tmp_ct_salescontribglobal;
CREATE TABLE tmp_ct_salescontribglobal
(
    dd_forecastdate date,
    dd_partnumber varchar(40),
    dd_plantcode varchar(30),
    ct_salescontribglobal decimal(18,4)
);



/* Update NASP measures from fact_atlaspharmlogiforecast_merck */
/*DROP TABLE IF EXISTS tmp_update_nasmpeasures_to_fopsfcst
CREATE TABLE tmp_update_nasmpeasures_to_fopsfcst
AS
SELECT year(d.datevalue) || lpad( month(d.datevalue),2,0)  fcstdate_yyyymm,dp.partnumber dd_partnumber,pl.plantcode dd_plantcode,reporting_company_code dd_REPORTING_COMPANY_CODE,
country_destination_code dd_COUNTRY_DESTINATION_CODE,
market_grouping DD_MARKET_GROUPING, hei_code DD_HEI_CODE,
sum(f.ct_nasp) ct_nasp,sum(f.ct_nasp_cy) ct_nasp_cy,sum(f.ct_nasp_global) ct_nasp_global,
sum(f.ct_nasp_py) ct_nasp_py,sum(f.ct_nasp_fc_cy) ct_nasp_fc_cy,sum(f.ct_nasp_fc_ny) ct_nasp_fc_ny,
sum(f.ct_salesdeliv2mthsnasp) as ct_salesdeliv2mthsnasp
FROM fact_atlaspharmlogiforecast_merck f inner join dim_part dp on dp.dim_partid = f.dim_partid
inner join dim_plant pl on pl.dim_plantid = f.dim_plantid
inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
group by year(d.datevalue) || lpad( month(d.datevalue),2,0),dp.partnumber,pl.plantcode,reporting_company_code,country_destination_code,market_grouping,hei_code*/

/*UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_nasp = t.ct_nasp,
    f.ct_nasp_cy = t.ct_nasp_cy,
    f.ct_nasp_global = t.ct_nasp_global,
    f.ct_nasp_py = t.ct_nasp_py,
    f.ct_nasp_fc_cy = t.ct_nasp_fc_cy,
    f.ct_nasp_fc_ny = t.ct_nasp_fc_ny,
	f.ct_salesdeliv2mthsnasp=t.ct_salesdeliv2mthsnasp
FROM merck.fact_fosalesforecastcortex f, tmp_update_nasmpeasures_to_fopsfcst t,tmp_maxrptdate_cortex r,dim_date d
WHERE f.dim_dateidforecast = d.dim_dateid AND d.calendarmonthid = t.fcstdate_yyyymm
AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.DD_HEI_CODE AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/

/*Georgiana 01 Mar 2017 modified the default of all nasp columns in order to have default 0 and not null constraint, this update is not needed*/
/*UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_nasp = CASE WHEN f.ct_nasp is null then 0 else f.ct_nasp end ,
    f.ct_nasp_cy = CASE WHEN f.ct_nasp_cy is null then 0 else f.ct_nasp_cy end ,
    f.ct_nasp_global = CASE WHEN f.ct_nasp_global is null then 0 else f.ct_nasp_global end ,
    f.ct_nasp_py = CASE WHEN f.ct_nasp_py is null then 0 else f.ct_nasp_py end ,
    f.ct_nasp_fc_cy = CASE WHEN f.ct_nasp_fc_cy is null then 0 else f.ct_nasp_fc_cy end ,
    f.ct_nasp_fc_ny = CASE WHEN f.ct_nasp_fc_ny is null then 0 else f.ct_nasp_fc_ny end
FROM merck.fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND (f.ct_nasp is null or ct_nasp_cy is null or ct_nasp_global is null or ct_nasp_py is null or ct_nasp_fc_cy is null or ct_nasp_fc_ny is null)*/
/* BI-5395 - OZ,GN,MH : Add PlantTitle and PlantCode for DFA and Report Available */

drop table if exists fact_atlaspharmlogiforecast_merck_for_salesforecast;
create table fact_atlaspharmlogiforecast_merck_for_salesforecast
as select fd.*,
(case when (fd.dd_version = 'SFA' and fd.dim_plantid in (91,124)) or fd.dd_version = 'DTA' then fd.dd_country_destination_code else 'Not Set' end) as country_destination_code_upd,
dp.partnumber as dd_partnumber,
(CASE WHEN dp.plant = 'NL10' THEN 'XX20' ELSE dp.plant END) as dd_plant_upd,
d.MonthEndDate as reporting_monthenddate,(CASE WHEN d.plantcode_factory = 'NL10' THEN 'XX20' ELSE d.plantcode_factory END) as reporting_plantcode_factory_upd,
d.companycode as reporting_companycode
from fact_atlaspharmlogiforecast_merck fd, dim_date d, dim_part dp
where fd.dim_dateidreporting = d.dim_dateid
AND fd.dim_partid = dp.dim_partid
and dd_version = 'SFA';

/*merge into fact_fosalesforecastcortex fso
using (
select distinct fso.fact_fosalesforecastcortexid,fd.dd_planttitlemerck,fd.dd_plantcode,fd.dd_reportavailable
from
fact_fosalesforecastcortex fso inner join dim_date d on fso.dim_dateidforecast = d.dim_dateid
inner join fact_atlaspharmlogiforecast_merck_for_salesforecast fd
on fd.dd_partnumber = fso.dd_partnumber
AND fd.dd_plant_upd = fso.dd_plantcode
AND ifnull(fso.DD_MARKET_GROUPING, 'Not Set') = fd.market_grouping
AND ifnull(fso.dd_COUNTRY_DESTINATION_CODE, 'Not Set') = ifnull(country_destination_code_upd,'Not Set')
AND fd.reporting_MonthEndDate = d.MonthEndDate
AND fd.reporting_plantcode_factory_upd = d.plantcode_factory
AND fd.reporting_companycode = d.companycode
/* where fd.dd_partnumber is null and
 d.datevalue < current_date + interval '1' month and
d.datevalue not between '2013-01-01' AND '2014-12-31' and
CT_FORECASTQUANTITY_CUSTOMER > 1 and
fd.dd_version = 'SFA'
) t
ON fso.fact_fosalesforecastcortexid = t.fact_fosalesforecastcortexid
WHEN MATCHED THEN UPDATE
SET fso.dd_planttitlemerck = t.dd_planttitlemerck,
fso.dd_plantcodemerck = t.dd_plantcode,
fso.dd_reportavailable = t.dd_reportavailable*/

merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid, f.dd_reportavailable
from fact_atlaspharmlogiforecast_merck_for_salesforecast f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp, tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(f.dd_country_destination_code_upd,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
/*and dp.partnumber ='141332' and dp.plant='ZA20'
and dt.calendarmonthid like '2017%'
and dd_version='SFA'
and fos.dd_partnumber='000215'
and fos.DD_PLANTCODEMERCK='Not Set'*/
) t
ON fos.fact_fosalesforecastcortexid = t.fact_fosalesforecastcortexid
WHEN MATCHED THEN UPDATE
SET
fos.dd_reportavailable = t.dd_reportavailable;

drop table if exists tmp_for_updplanttitle;
create table tmp_for_updplanttitle as
select distinct p.partnumber,pl.plantcode,f.dd_COUNTRY_DESTINATION_CODE AS COUNTRY_DESTINATION_CODE,f.dd_REPORTING_COMPANY_CODE AS REPORTING_COMPANY_CODE,dd_planttitlemerck,dd_plantcode from fact_atlaspharmlogiforecast_merck f, dim_part p,dim_plant pl
where f.dim_partid=p.dim_partid
and f.dim_plantid=pl.dim_plantid
and dd_planttitlemerck<>'Not Set';

merge into fact_fosalesforecastcortex f
using (
select distinct f.DD_REPORTINGDATE,f.DD_PARTNUMBER,t.DD_PLANTCODE ,f.DD_REPORTING_COMPANY_CODE,f.DD_COUNTRY_DESTINATION_CODE,t.dd_planttitlemerck
from fact_fosalesforecastcortex f,tmp_for_updplanttitle t,tmp_maxrptdate_cortex r
where f.dd_partnumber=t.partnumber
and f.dd_plantcode=t.plantcode
and f.dd_COUNTRY_DESTINATION_CODE=case when t.plantcode <>'NL10' then 'Not Set' else t.COUNTRY_DESTINATION_CODE end
and f.dd_REPORTING_COMPANY_CODE=t.REPORTING_COMPANY_CODE
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND f.DD_LATESTREPORTING = 'Yes' ) t
on f.DD_REPORTINGDATE=t.DD_REPORTINGDATE
AND f.DD_PARTNUMBER = t.DD_PARTNUMBER
AND f.DD_PLANTCODE = t.DD_PLANTCODE
AND f.DD_REPORTING_COMPANY_CODE = t.DD_REPORTING_COMPANY_CODE
AND f.DD_COUNTRY_DESTINATION_CODE = t.DD_COUNTRY_DESTINATION_CODE
when matched then update set f.dd_planttitlemerck=ifnull(t.dd_planttitlemerck,'Not Set'),
f.dd_plantcodemerck=t.dd_plantcode;

/* BI-5395 - OZ,GN,MH : Add PlantTitle and PlantCode for DFA and Report Available */

DROP TABLE IF EXISTS tmp_fact_salesorder_12monthsales_1;
create table tmp_fact_salesorder_12monthsales_1(
dd_partnumber varchar(22) not null ,
dd_plantcode varchar(20) not null ,
dd_REPORTING_COMPANY_CODE varchar(200),
dd_COUNTRY_DESTINATION_CODE varchar(200),
DD_HEI_CODE varchar(10),
DD_MARKET_GROUPING varchar(100),
ct_DeliveredQty_YTD decimal(36,6),
amt_shipped decimal(36,6) ,
dd_countpartplant int,
dd_sales_rank int not null
);

/* changed by bogdani to use sales from shipping */ 
INSERT INTO tmp_fact_salesorder_12monthsales_1
SELECT  dd_partnumber,
 dd_plantcode,
 dd_REPORTING_COMPANY_CODE,
 dd_COUNTRY_DESTINATION_CODE,
 DD_HEI_CODE,
 DD_MARKET_GROUPING,
sum(ct_salesquantity) ct_DeliveredQty_YTD,
cast(0 as decimal(12,4)) amt_shipped,
cast(0 as decimal(12,4)) dd_countpartplant,
0 dd_sales_rank
FROM FACT_FOSALESFORECASTCORTEX 
WHERE DD_FORECASTDATE is not null and DD_PARTNUMBER is not null
AND year(to_date(to_char(dd_forecastdate),'YYYYMMDD')) >= year(current_date)
AND DD_LATESTREPORTING = 'Yes'
group by dd_partnumber,dd_plantcode,dd_REPORTING_COMPANY_CODE,dd_COUNTRY_DESTINATION_CODE,DD_HEI_CODE,DD_MARKET_GROUPING;

/* Avg price is calculated at part level*/
DROP TABLE IF EXISTS tmp_avg_sp_fromfso;
CREATE TABLE tmp_avg_sp_fromfso
AS
SELECT
dp.partnumber dd_partnumber,
avg(f_so.amt_UnitPriceUoM/(CASE WHEN f_so.ct_PriceUnit <> 0 THEN f_so.ct_PriceUnit ELSE NULL END) * amt_exchangerate_gbl) amt_avgprice
FROM fact_salesorder f_so
INNER JOIN dim_part dp on dp.dim_partid = f_so.dim_partid
INNER JOIN dim_date spdd on spdd.dim_dateid = f_so.DIM_DATEIDSALESORDERCREATED
WHERE year(spdd.datevalue) >= year(current_date) - 1
GROUP BY dp.partnumber;

UPDATE tmp_fact_salesorder_12monthsales_1 f
SET f.amt_shipped = ct_DeliveredQty_YTD * t.amt_avgprice
FROM tmp_fact_salesorder_12monthsales_1 f, tmp_avg_sp_fromfso t
WHERE f.dd_partnumber = t.dd_partnumber;

DROP TABLE IF EXISTS tmp_upd_dd_sales_rank;
CREATE TABLE tmp_upd_dd_sales_rank
as
select t.*,rank() over(order by amt_shipped desc) dd_rank
from tmp_fact_salesorder_12monthsales_1 t;

UPDATE tmp_fact_salesorder_12monthsales_1 t1
SET t1.dd_sales_rank = t.dd_rank
FROM tmp_upd_dd_sales_rank t,tmp_fact_salesorder_12monthsales_1 t1
WHERE t.dd_partnumber = t1.dd_partnumber
AND t.dd_plantcode = t1.dd_plantcode
AND t.dd_REPORTING_COMPANY_CODE = t1.dd_REPORTING_COMPANY_CODE
AND t.dd_COUNTRY_DESTINATION_CODE = t1.dd_COUNTRY_DESTINATION_CODE
AND t.DD_HEI_CODE = t1.DD_HEI_CODE
AND t.DD_MARKET_GROUPING = t1.DD_MARKET_GROUPING;

UPDATE tmp_fact_salesorder_12monthsales_1
SET dd_countpartplant = (SELECT max(dd_sales_rank)
FROM tmp_fact_salesorder_12monthsales_1);

DROP TABLE IF EXISTS tmp_fact_salesorder_12monthsales;
CREATE TABLE tmp_fact_salesorder_12monthsales
AS
SELECT t1.*,
case WHEN (dd_sales_rank/dd_countpartplant) < 0.25 THEN 1
     WHEN (dd_sales_rank/dd_countpartplant) >= 0.25 and (dd_sales_rank/dd_countpartplant) < 0.5 THEN 2
     WHEN (dd_sales_rank/dd_countpartplant) >= 0.5 and (dd_sales_rank/dd_countpartplant) < 0.75 THEN 3
     ELSE 4 END
dd_salescontribution_quartile_rank
FROM tmp_fact_salesorder_12monthsales_1 t1;

UPDATE fact_fosalesforecastcortex f
SET f.dd_salescontribution_quartile_rank = t.dd_salescontribution_quartile_rank,
f.amt_shippedqtyytd = t.amt_shipped,
f.ct_DeliveredQty_YTD = t.ct_DeliveredQty_YTD
FROM fact_fosalesforecastcortex f,tmp_fact_salesorder_12monthsales t,tmp_maxrptdate_cortex r
WHERE f.dd_partnumber = t.dd_partnumber
AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.DD_HEI_CODE
AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING
AND r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY');


/* Partnumber was not found in fact_salesorder for sales in last 1 year */
UPDATE fact_fosalesforecastcortex f
SET f.dd_salescontribution_quartile_rank = -1
WHERE f.dd_salescontribution_quartile_rank IS NULL;

/* Clean up data */
/* Remove part numbers that were not found. e.g if input data was from prd and fcst is in stg, then there will be some parts that are not found */
/*
DELETE from fact_fosalesforecastcortex
where dd_latestreporting = 'Yes'
and dim_partid = 1*/

UPDATE fact_fosalesforecastcortex
SET dd_forecastquantity = round(ct_forecastquantity,0);

DROP TABLE IF EXISTS tmp_fact_salesorder_12monthsales_1;
create table tmp_fact_salesorder_12monthsales_1(
dd_partnumber varchar(22) not null ,
dd_plantcode varchar(20) not null ,
dd_REPORTING_COMPANY_CODE varchar(200),
dd_COUNTRY_DESTINATION_CODE varchar(200),
DD_HEI_CODE varchar(10),
DD_MARKET_GROUPING varchar(100),
ct_DeliveredQty_YTD decimal(36,6),
amt_shipped decimal(36,6) ,
dd_countpartplant int,
dd_sales_rank int not null
);

/* changed by bogdani to use sales from shipping */ 
INSERT INTO tmp_fact_salesorder_12monthsales_1
SELECT  dd_partnumber,
 dd_plantcode,
 dd_REPORTING_COMPANY_CODE,
 dd_COUNTRY_DESTINATION_CODE,
 DD_HEI_CODE,
 DD_MARKET_GROUPING,
sum(ct_salesquantity) ct_DeliveredQty_YTD,
cast(0 as decimal(12,4)) amt_shipped,
cast(0 as decimal(12,4)) dd_countpartplant,
0 dd_sales_rank
FROM FACT_FOSALESFORECASTCORTEX 
WHERE DD_FORECASTDATE is not null and DD_PARTNUMBER is not null
AND to_char(substr(dd_forecastdate,1,6)) <= to_char(current_date-interval '1' month,'YYYYMM')
AND to_char(substr(dd_forecastdate,1,6)) >= to_char(current_date-interval '1' year,'YYYYMM')
AND DD_LATESTREPORTING = 'Yes'
group by dd_partnumber,dd_plantcode,dd_REPORTING_COMPANY_CODE,dd_COUNTRY_DESTINATION_CODE,DD_HEI_CODE,DD_MARKET_GROUPING;


/* Avg price is calculated at part level*/
DROP TABLE IF EXISTS tmp_avg_sp_fromfso;
CREATE TABLE tmp_avg_sp_fromfso
AS
SELECT
dp.partnumber dd_partnumber,
avg(f_so.amt_UnitPriceUoM/(CASE WHEN f_so.ct_PriceUnit <> 0 THEN f_so.ct_PriceUnit ELSE NULL END) * amt_exchangerate_gbl) amt_avgprice
FROM fact_salesorder f_so
INNER JOIN dim_part dp on dp.dim_partid = f_so.dim_partid
INNER JOIN dim_date spdd on spdd.dim_dateid = f_so.DIM_DATEIDSALESORDERCREATED
WHERE year(spdd.datevalue) >= year(current_date) - 1 /* To have more no. of parts, check current yr + last years data */
GROUP BY dp.partnumber;

UPDATE tmp_fact_salesorder_12monthsales_1 f
SET f.amt_shipped = ct_DeliveredQty_YTD * t.amt_avgprice
FROM tmp_fact_salesorder_12monthsales_1 f, tmp_avg_sp_fromfso t
WHERE f.dd_partnumber = t.dd_partnumber;

DROP TABLE IF EXISTS tmp_upd_dd_sales_rank;
CREATE TABLE tmp_upd_dd_sales_rank
as
select t.*,rank() over(order by amt_shipped desc) dd_rank
from tmp_fact_salesorder_12monthsales_1 t;

UPDATE tmp_fact_salesorder_12monthsales_1 t1
SET t1.dd_sales_rank = t.dd_rank
FROM tmp_upd_dd_sales_rank t,tmp_fact_salesorder_12monthsales_1 t1
WHERE t.dd_partnumber = t1.dd_partnumber
AND t.dd_plantcode = t1.dd_plantcode
AND t.dd_REPORTING_COMPANY_CODE = t1.dd_REPORTING_COMPANY_CODE
AND t.dd_COUNTRY_DESTINATION_CODE = t1.dd_COUNTRY_DESTINATION_CODE
AND t.DD_HEI_CODE = t1.DD_HEI_CODE
AND t.DD_MARKET_GROUPING = t1.DD_MARKET_GROUPING;


UPDATE tmp_fact_salesorder_12monthsales_1
SET dd_countpartplant = (SELECT max(dd_sales_rank)
FROM tmp_fact_salesorder_12monthsales_1);

DROP TABLE IF EXISTS tmp_fact_salesorder_12monthsales;
CREATE TABLE tmp_fact_salesorder_12monthsales
AS
SELECT t1.*,
case WHEN (dd_sales_rank/dd_countpartplant) < 0.25 THEN 1
     WHEN (dd_sales_rank/dd_countpartplant) >= 0.25 and (dd_sales_rank/dd_countpartplant) < 0.5 THEN 2
     WHEN (dd_sales_rank/dd_countpartplant) >= 0.5 and (dd_sales_rank/dd_countpartplant) < 0.75 THEN 3
     ELSE 4 END
dd_salescontribution_quartile_rank,
case    WHEN (dd_sales_rank/dd_countpartplant) < 0.333 THEN 1
    WHEN (dd_sales_rank/dd_countpartplant) >= 0.333 and (dd_sales_rank/dd_countpartplant) < 0.667 THEN 2
    ELSE 3 END
dd_salescontribution_3levels_rank
FROM tmp_fact_salesorder_12monthsales_1 t1;

UPDATE fact_fosalesforecastcortex  f
SET f.dd_salescontribution_quartile_rank = NULL,f.dd_salescontribution_3levels_rank = NULL,
f.amt_shippedqtyytd = 0,f.ct_DeliveredQty_YTD = 0
FROM fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY');

UPDATE fact_fosalesforecastcortex f
SET f.dd_salescontribution_quartile_rank = t.dd_salescontribution_quartile_rank,
f.amt_shippedqtyytd = t.amt_shipped,
f.ct_DeliveredQty_YTD = t.ct_DeliveredQty_YTD,
f.dd_salescontribution_3levels_rank = t.dd_salescontribution_3levels_rank
FROM fact_fosalesforecastcortex f,tmp_fact_salesorder_12monthsales t,tmp_maxrptdate_cortex r
WHERE f.dd_partnumber = t.dd_partnumber
AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.DD_HEI_CODE
AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING
AND r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY');


DROP TABLE IF EXISTS tmp_upd_ct_volatility_yoy_percent;
CREATE TABLE tmp_upd_ct_volatility_yoy_percent
AS
SELECT dd_partnumber,d.CALENDARYEAR,sum(CASE WHEN dd_forecastsample in ('Train','Test') THEN (ct_salesquantity) ELSE (ct_forecastquantity) END) ct_salesquantity
from fact_fosalesforecastcortex f inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
inner join tmp_maxrptdate_cortex r on r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
WHERE dd_forecastrank = 1
AND d.CALENDARYEAR <= year(current_date)
GROUP BY dd_partnumber,d.CALENDARYEAR;


DROP TABLE IF EXISTS tmp_upd_ct_volatility_yoy_percent_STDDEV;
CREATE TABLE tmp_upd_ct_volatility_yoy_percent_STDDEV
AS
SELECT dd_partnumber,stddev(ct_salesquantity) ct_salesquantity_yoy_sd,avg(ct_salesquantity) ct_salesquantity_yoy_avg,
case when avg(ct_salesquantity) = 0 then 0 else 100 * stddev(ct_salesquantity)/avg(ct_salesquantity) end ct_volatility_yoy_percent
from tmp_upd_ct_volatility_yoy_percent
group by dd_partnumber;


UPDATE fact_fosalesforecastcortex f
SET f.ct_volatility_yoy_percent = t.ct_volatility_yoy_percent, dd_volatility_yoy_gt50pc = 'Y'
FROM fact_fosalesforecastcortex f, tmp_upd_ct_volatility_yoy_percent_STDDEV t
WHERE f.dd_partnumber = t.dd_partnumber;

UPDATE fact_fosalesforecastcortex f
SET dd_volatility_yoy_gt50pc =  CASE WHEN f.ct_volatility_yoy_percent >= 50 THEN 'Y' ELSE 'N' END ;

DROP TABLE IF EXISTS tmp_upd_ct_volatility_yoy_percent_overall;
CREATE TABLE tmp_upd_ct_volatility_yoy_percent_overall
AS
SELECT d.CALENDARYEAR,sum(ct_salesquantity) ct_salesquantity
from fact_fosalesforecastcortex f inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
inner join tmp_maxrptdate_cortex r on r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
GROUP BY d.CALENDARYEAR;

/* Update ct_coeffofvariation */
DROP TABLE IF EXISTS tmp_monthlysales_cov;
CREATE TABLE tmp_monthlysales_cov
AS
SELECT  DD_PARTNUMBER,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,
stddev(CT_SALESQUANTITY) CT_SALESQUANTITY_stddev,avg(CT_SALESQUANTITY) CT_SALESQUANTITY_avg,
case when avg(CT_SALESQUANTITY) = 0 then 0 else stddev(CT_SALESQUANTITY)/avg(CT_SALESQUANTITY) end ct_coeffofvariation
FROM
(
SELECT distinct DD_PARTNUMBER,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,
d.datevalue dd_forecastdate,CT_SALESQUANTITY,f.dd_reportingdate,dd_forecasttype
from fact_fosalesforecastcortex f inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
inner join tmp_maxrptdate_cortex r on r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
WHERE dd_latestreporting = 'Yes'
AND dd_forecastsample in ('Train','Test')
AND CT_SALESQUANTITY is not null
AND dd_forecastrank = 1) t
GROUP BY DD_PARTNUMBER,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING
HAVING avg(CT_SALESQUANTITY) > 0;

UPDATE fact_fosalesforecastcortex f
SET f.ct_coeffofvariation = ifnull(c.ct_coeffofvariation,0)
FROM tmp_monthlysales_cov c,fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE f.DD_PARTNUMBER = c.DD_PARTNUMBER
AND ifnull(f.dd_SALES_COCD,'xxx') = ifnull(c.dd_SALES_COCD,'xxx')
AND ifnull(f.dd_REPORTING_COMPANY_CODE,'xxx') = ifnull(c.dd_REPORTING_COMPANY_CODE,'xxx')
AND ifnull(f.dd_HEI_CODE,'xxx') = ifnull(c.dd_HEI_CODE,'xxx')
AND ifnull(f.dd_COUNTRY_DESTINATION_CODE,'xxx') = ifnull(c.dd_COUNTRY_DESTINATION_CODE,'xxx')
AND ifnull(f.dd_MARKET_GROUPING,'xxx') = ifnull(c.dd_MARKET_GROUPING,'xxx')
AND dd_latestreporting = 'Yes'
AND r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
AND ifnull(f.ct_coeffofvariation,-1) <> ifnull(c.ct_coeffofvariation,0);

/* Update coeffovfariation_forecast */
UPDATE fact_fosalesforecastcortex f
SET f.ct_coeffofvariation_forecast = c.ct_variationcoeff
FROM tmp_forecastcov c,fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE f.DD_PARTNUMBER = c.DD_PARTNUMBER
AND ifnull(f.dd_SALES_COCD,'xxx') = ifnull(c.dd_SALES_COCD,'xxx')
AND ifnull(f.dd_REPORTING_COMPANY_CODE,'xxx') = ifnull(c.dd_REPORTING_COMPANY_CODE,'xxx')
AND ifnull(f.dd_HEI_CODE,'xxx') = ifnull(c.dd_HEI_CODE,'xxx')
AND ifnull(f.dd_COUNTRY_DESTINATION_CODE,'xxx') = ifnull(c.dd_COUNTRY_DESTINATION_CODE,'xxx')
AND ifnull(f.dd_MARKET_GROUPING,'xxx') = ifnull(c.dd_MARKET_GROUPING,'xxx')
AND f.dd_forecasttype = c.dd_forecasttype
AND r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
AND c.dd_reportingdate = f.dd_reportingdate
AND f.dd_latestreporting = 'Yes';

DROP TABLE IF EXISTS tmp_upd_forecastproductcategory;
CREATE TABLE tmp_upd_forecastproductcategory
AS
SELECT t.*,ceiling(MONTHS_BETWEEN(dd_lastdate , min_forecastdate)) ct_diff_holdout_minus_minfcst
from (
SELECT DD_PARTNUMBER,dd_plantcode,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_home_vs_export,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,
f.dd_reportingdate,to_date(dd_lastdate,'DD MON YYYY') dd_lastdate,min(d.datevalue) min_forecastdate
FROM fact_fosalesforecastcortex f inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
inner join tmp_maxrptdate_cortex r on r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
WHERE f.dd_latestreporting = 'Yes'
AND f.ct_salesquantity > 0
GROUP BY DD_PARTNUMBER,dd_plantcode,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_home_vs_export,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,
f.dd_reportingdate,to_date(dd_lastdate,'DD MON YYYY')) t;

UPDATE fact_fosalesforecastcortex f
SET f.dd_forecastproductcategory =  CASE WHEN t.ct_diff_holdout_minus_minfcst < 24 THEN 'New Launch' ELSE 'Established Product' END
FROM fact_fosalesforecastcortex f,tmp_upd_forecastproductcategory t,tmp_maxrptdate_cortex r
WHERE f.DD_PARTNUMBER = t.DD_PARTNUMBER AND f.dd_plantcode = t.dd_plantcode
AND f.dd_SALES_COCD = t.dd_SALES_COCD AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE
AND f.dd_home_vs_export = t.dd_home_vs_export AND f.dd_HEI_CODE = t.dd_HEI_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
AND f.dd_MARKET_GROUPING = t.dd_MARKET_GROUPING AND f.dd_reportingdate = t.dd_reportingdate;


DROP TABLE IF EXISTS tmp_upd_forecastproductcategory_2;
CREATE TABLE tmp_upd_forecastproductcategory_2
AS
SELECT DD_PARTNUMBER,dd_plantcode,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_home_vs_export,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,
f.dd_reportingdate,to_date(dd_lastdate,'DD MON YYYY') dd_lastdate,max(CT_FORECASTQUANTITY_CUSTOMER) max_forecast_in_first3horizonmonths,
sum(CT_FORECASTQUANTITY_CUSTOMER) sum_forecast_in_first3horizonmonths,min(CT_FORECASTQUANTITY_CUSTOMER) min_forecast_in_first3horizonmonths
FROM fact_fosalesforecastcortex f inner join dim_date d on d.dim_dateid = f.dim_dateidforecast
inner join tmp_maxrptdate_cortex r on r.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
WHERE f.dd_latestreporting = 'Yes'
AND f.dd_forecastsample = 'Horizon'
AND MONTHS_BETWEEN(d.datevalue,to_date(dd_lastdate,'DD MON YYYY')) <= 3
AND f.dd_forecastrank = 1
GROUP BY DD_PARTNUMBER,dd_plantcode,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_home_vs_export,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,dd_MARKET_GROUPING,
f.dd_reportingdate,to_date(dd_lastdate,'DD MON YYYY')
HAVING max(CT_FORECASTQUANTITY_CUSTOMER) = 0;

UPDATE fact_fosalesforecastcortex f
SET f.dd_forecastproductcategory =  'Potential Phase out'
FROM fact_fosalesforecastcortex f,tmp_upd_forecastproductcategory_2 t
WHERE f.DD_PARTNUMBER = t.DD_PARTNUMBER AND f.dd_plantcode = t.dd_plantcode
AND f.dd_SALES_COCD = t.dd_SALES_COCD AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE
AND f.dd_home_vs_export = t.dd_home_vs_export AND f.dd_HEI_CODE = t.dd_HEI_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.dd_MARKET_GROUPING = t.dd_MARKET_GROUPING AND f.dd_reportingdate = t.dd_reportingdate;

/* New HEI Code changes - 20 Dec */
update fact_fosalesforecastcortex set dd_heicode_new = DD_HEI_CODE where DD_SALES_COCD = 'NL10' AND dd_heicode_new <> DD_HEI_CODE;
update fact_fosalesforecastcortex set dd_heicode_new = 'Not Set' where DD_SALES_COCD <> 'NL10' AND dd_heicode_new <> 'Not Set' AND dd_latestreporting = 'Yes';

/* BI-5395 - OZ : Adapt grain for Sales Quantity for all plants different than NL10 to ignore market group */
/* update fact_fosalesforecastcortex
set dd_country_destination_code = 'Not Set'
where DD_SALES_COCD not in ('NL10','XX20')
and dd_country_destination_code <> 'Not Set'


drop table if exists fosalesforecast_updateSalesQuantity
create table fosalesforecast_updateSalesQuantity AS
select substring(dd_forecastdate,1,6) as dd_forecastdate,dd_forecastrank, dd_forecastsample,dd_reportingdate,f.DD_PARTNUMBER,f.DD_SALES_COCD,DD_MARKET_GROUPING,DD_REPORTING_COMPANY_CODE,
avg(DD_FORECASTQUANTITY) as old_DD_FORECASTQUANTITY,
sum(DD_FORECASTQUANTITY) as new_DD_FORECASTQUANTITY
from fact_fosalesforecastcortex f
 where f.DD_SALES_COCD not in ('NL10','XX20')
and DD_PARTNUMBER = '141166'
 AND DD_SALES_COCD = 'CZ20'
and dd_forecastrank = '1'
and dd_forecastsample = 'Test'
 AND dd_reportingdate = '12 Dec 2016'
group by substring(dd_forecastdate,1,6),dd_forecastrank, dd_forecastsample,dd_reportingdate,f.DD_PARTNUMBER,f.DD_SALES_COCD,DD_MARKET_GROUPING,DD_REPORTING_COMPANY_CODE

update fact_fosalesforecastcortex fo
set fo.dd_forecastquantity = CASE WHEN fo.dd_flag_salesquantity_editable = 'oldrows' THEN temp.old_DD_FORECASTQUANTITY ELSE temp.new_DD_FORECASTQUANTITY END
from fact_fosalesforecastcortex fo
inner join fosalesforecast_updateSalesQuantity temp on temp.dd_forecastdate = substring(fo.dd_forecastdate,1,6)
and fo.dd_forecastrank = temp.dd_forecastrank
and fo.dd_forecastsample = temp.dd_forecastsample
and fo.dd_reportingdate = temp.dd_reportingdate
and fo.DD_PARTNUMBER = temp.DD_PARTNUMBER
and fo.DD_SALES_COCD = temp.DD_SALES_COCD
and fo.dd_market_grouping = temp.dd_market_grouping
and fo.DD_REPORTING_COMPANY_CODE = temp.DD_REPORTING_COMPANY_CODE
and fo.dd_forecastquantity <> CASE WHEN fo.dd_flag_salesquantity_editable = 'oldrows' THEN temp.old_DD_FORECASTQUANTITY ELSE temp.new_DD_FORECASTQUANTITY END

update fact_fosalesforecastcortex
set dd_flag_salesquantity_editable = 'oldrows'
where dd_flag_salesquantity_editable <> 'oldrows'

drop table if exists fosalesforecast_updateSalesQuantity */
/* BI-5395 - OZ : Adapt grain for Sales Quantity for all plants different than NL10 to ignore market group */


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,
avg(f.ct_nasp) as ct_nasp,
avg(f.ct_nasp_pra) as ct_nasp_pra,
sum(f.CT_SALESDELIVEREDUSD) as CT_SALESDELIVEREDUSD,
avg(f.ct_nasp_cy) as ct_nasp_cy,
avg(f.ct_nasp_global) as ct_nasp_global,
avg(f.ct_nasp_py) as ct_nasp_py,
avg(f.ct_nasp_fc_cy) as ct_nasp_fc_cy,
avg(f.ct_nasp_fc_ny) as ct_nasp_fc_ny,
SUM(f.ct_salesdeliv2mthsnasp) as ct_salesdeliv2mthsnasp,
dt.calendarmonthid
from  fact_atlaspharmlogiforecast_merck f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
AND m.dd_reportingdate = TO_DATE(fos.dd_reportingdate,'DD MON YYYY')
and dd_version='SFA'
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.ct_nasp = t.ct_nasp,
fos.ct_nasp_pra = t.ct_nasp_pra,
fos.CT_SALESDELIVEREDUSD = t.CT_SALESDELIVEREDUSD,
fos.ct_nasp_cy=t.ct_nasp_cy,
fos.ct_nasp_global=t.ct_nasp_global,
fos.ct_nasp_py=t.ct_nasp_py,
fos.ct_nasp_fc_cy=t.ct_nasp_fc_cy,
fos.ct_nasp_fc_ny=t.ct_nasp_fc_ny,
fos.ct_salesdeliv2mthsnasp=t.ct_salesdeliv2mthsnasp;

/*BI-4363 End*/
/*28 Feb 2017 End of Changes*/

/*24 Jul 2020 - MRC-1178*/
merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/, 
                dt.calendarmonthid, f.dd_COUNTRY_DESTINATION_CODE,
                avg(f.CT_NASP_PRAUSED) as CT_NASP_PRAUSED,
                 avg(f.CT_NASP_PRAUSEDLC) as CT_NASP_PRAUSEDLC
from  fact_atlaspharmlogiforecast_merck f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND SUBSTR(dp.plant, 1,2) =  f.dd_COUNTRY_DESTINATION_CODE
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set') 
and dd_version='DTA' 
AND f.CT_NASP_PRAUSED IS NOT NULL
AND m.dd_reportingdate = TO_DATE(fos.dd_reportingdate,'DD MON YYYY')
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,
dt.calendarmonthid, f.dd_COUNTRY_DESTINATION_CODE ) t
 on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.CT_NASP_PRAUSED = t.CT_NASP_PRAUSED,
fos.CT_NASP_PRAUSEDLC = t.CT_NASP_PRAUSEDLC;

/* 28 Feb 2017 Georgiana add Sales Delivered PY from DF SA according to BI-5589*/

/*new logic*/
drop table if exists tmp_for_salesdeliveredPY;
create table tmp_for_salesdeliveredPY as
select distinct f.dim_partid,f.dim_plantid,dd_COUNTRY_DESTINATION_CODE AS COUNTRY_DESTINATION_CODE,dd_REPORTING_COMPANY_CODE AS REPORTING_COMPANY_CODE,dim_dateidreporting,f.ct_salesdeliveredPY  from fact_atlaspharmlogiforecast_merck f, dim_part p,dim_plant pl
where f.dim_partid=p.dim_partid
and f.dim_plantid=pl.dim_plantid;


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
SUM(f.ct_salesdeliveredPY) as ct_salesdeliveredPY,
dt.calendarmonthid
from tmp_for_salesdeliveredPY f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,dim_plant pl, tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
and f.dim_plantid=pl.dim_plantid
and f.dim_plantid=fos.dim_plantid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(case when pl.plantcode <>'NL10' then 'Not Set' else f.COUNTRY_DESTINATION_CODE end,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
/*and dp.partnumber ='119977' and dp.plant='IE20'and dt.calendarmonthid like '2016%'*/
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set fos.ct_salesdeliveredPY=t.ct_salesdeliveredPY;

/*BI-5589 End*/

/* BI-4363 corrected NASP Update and calculated Sales Amount from Demand forecast in order to have the same aggregation between the two SA's*/
drop table if exists tmp_for_upd_salesdeliveredmonth;
create table tmp_for_upd_salesdeliveredmonth as
select f.dim_partid,dt.datevalue,dd_reporting_company_code AS reporting_company_code,dim_dateidreporting,dd_version,(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end) as country_destination_code_upd,
CASE WHEN
MAX( (f.dd_reportavailable) ) = 'Yes'
THEN
SUM(f.ct_salesdeliveredusd) /* MRC-860 */
ELSE
NULL
END as col1,
sum(f.ct_salesmonth1) as col2,sum(f.ct_nasp) as col3 from fact_atlaspharmlogiforecast_merck f,dim_part dp,dim_Date dt
where f.dim_partid=dp.dim_partid
/*and dp.partnumber ='141332' and dp.plant='ZA20'
and dt.datevalue='2016-02-01'*/
and dim_dateidreporting=dt.dim_dateid
and  dd_version='SFA'
group by f.dim_partid,dt.datevalue,f.dd_reporting_company_code,dim_dateidreporting,dd_version,(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end);


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
sum(f.col1) as col1,
dt.calendarmonthid
from tmp_for_upd_salesdeliveredmonth f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp, tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(f.country_destination_code_upd,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
/*and dp.partnumber ='141332' and dp.plant='ZA20'
and dd_version='SFA'
and dt.calendarmonthid like '2016%'
and dd_reportingdate='08 Feb 2017'
and dd_forecastrank='1'
and dd_forecastdate='20160229'*/
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.ct_salesdeliveredmonth = ifnull(t.col1,0);




/*06 Mar 2017 Georgiaan changes according to BI-5640 adding dd_forecastrank2 as an editable attribute*/

merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,
dd_forecastrank as dd_forecastrank2
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
where TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set fos.dd_forecastrank2=t.dd_forecastrank2;

/* 06 Mar 2017 End of changes*/

/*Georgiana BI 5717 Changes*/


/*Start inserting missing data from Demand Forecast SA with 'Manual' forecast type, we are using DTA version in order to have two years of forecast*/
drop table if exists fact_atlaspharmlogiforecast_merck_for_salesforecast;
create table fact_atlaspharmlogiforecast_merck_for_salesforecast
as select fd.*,
(case when fd.dim_plantid in (91,124) and fd.dd_version = 'DTA' then fd.dd_country_destination_code else 'Not Set' end) as country_destination_code_upd,
dp.partnumber as dd_partnumber,
(CASE WHEN dp.plant = 'NL10' THEN 'XX20' ELSE dp.plant END) as dd_plant_upd,
d.MonthEndDate as reporting_monthenddate,(CASE WHEN d.plantcode_factory = 'NL10' THEN 'XX20' ELSE d.plantcode_factory END) as reporting_plantcode_factory_upd,
d.companycode as reporting_companycode
from fact_atlaspharmlogiforecast_merck fd, dim_date d, dim_part dp
where fd.dim_dateidreporting = d.dim_dateid
AND fd.dim_partid = dp.dim_partid
and dd_version = 'DTA';

/* 06 Jun 2018 Georgiana Changes: commented out the condition on forecast date as we don't want to insert Manual inserts for materials that have a PF run and added a rank condition in order to optimize the statement*/
drop table if exists tmp_for_insert_fopssales;
create table tmp_for_insert_fopssales as select distinct a.dim_partid,a.dim_plantid,a.dim_dateidreporting,a.dim_dateidforecast,a.country_destination_code_upd,a.dd_reporting_company_code AS reporting_company_code,b.dd_reportingdate
from fact_fosalesforecastcortex b,dim_date dt,fact_atlaspharmlogiforecast_merck_for_salesforecast a,tmp_maxrptdate_cortex m
 where  a.dim_partid=b.dim_partid
and a.dim_plantid=b.dim_plantid
AND a.dim_dateidreporting = dt.dim_dateid
AND ifnull(a.country_destination_code_upd,'Not Set') = ifnull(b.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(a.dd_reporting_company_code,'Not Set')=ifnull(b.dd_reporting_company_code,'Not Set')
and m.dd_reportingdate = TO_DATE(b.dd_reportingdate,'DD MON YYYY') and dd_version='DTA'
and dd_forecastrank='1'
;



delete from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex';

insert into number_fountain
select  'fact_fosalesforecastcortex',
ifnull(max(d.fact_fosalesforecastcortexid),
	ifnull((select min(s.dim_projectsourceid * s.multiplier)
			from dim_projectsource s),0))
from fact_fosalesforecastcortex d
WHERE d.fact_fosalesforecastcortexid <> 1;


insert into fact_fosalesforecastcortex (fact_fosalesforecastcortexid,dim_partid,dim_plantid,dim_dateidforecast,dd_reportingdate,dd_country_destination_code,dd_reporting_company_code,dd_insertsource,dd_forecasttype/*,dd_market_grouping*/)
select
(select ifnull(m.max_id, 0) from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex')
+ row_number() over(order by '') as fact_fosalesforecastcortexid,
t.*
from (
select distinct a.dim_partid,a.dim_plantid,a.dim_dateidreporting,to_char(to_date(m.dd_reportingdate, 'YYYY-MM-DD'),'DD Mon YYYY'),a.country_destination_code_upd,a.dd_reporting_company_code AS reporting_company_code,'Manual','Manual'/*,market_grouping,*/
 from fact_atlaspharmlogiforecast_merck_for_salesforecast a,tmp_maxrptdate_cortex m
where not exists (select 1 from tmp_for_insert_fopssales b
where
a.dim_partid=b.dim_partid and a.dim_plantid=b.dim_plantid and a.dim_dateidreporting=b.dim_dateidreporting and
 a.country_destination_code_upd=b.country_destination_code_upd and a.dd_reporting_company_code =b.reporting_company_code
and dd_version='DTA'
 and m.dd_reportingdate = TO_DATE(b.dd_reportingdate,'DD MON YYYY')
)) t;

/*APP-11716 - insert missing dates*/
/*correction: get the last run using tmp_maxrptdate_cortex instead of dd_holdout, which is updated later in the script */
drop table if exists tmp_forinsertmissingmonths;
create table tmp_forinsertmissingmonths as
select distinct dim_partid,dim_plantid,d.datevalue,dim_dateid,f.dd_reportingdate,dd_country_destination_code,dd_reporting_company_code,dd_insertsource,dd_forecasttype,dd_market_grouping
from
fact_fosalesforecastcortex f
inner join tmp_maxrptdate_cortex m on m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
cross join dim_date d where datevalue between current_date and current_date + interval '36' Month and dayofmonth='01' and companycode='Not Set' and plantcode_factory='Not Set' and
 dd_forecasttype='Manual'
/* and dd_latestreporting='Yes' and dd_holdout='3' */
;

delete from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex';

insert into number_fountain
select  'fact_fosalesforecastcortex',
ifnull(max(d.fact_fosalesforecastcortexid),
	ifnull((select min(s.dim_projectsourceid * s.multiplier)
			from dim_projectsource s),0))
from fact_fosalesforecastcortex d
WHERE d.fact_fosalesforecastcortexid <> 1;

insert into fact_fosalesforecastcortex (fact_fosalesforecastcortexid,dim_partid,dim_plantid,dim_dateidforecast,dd_reportingdate,dd_country_destination_code,dd_reporting_company_code,dd_insertsource,dd_forecasttype/*,dd_market_grouping*/,
			dd_latestreporting, dd_holdout)
select
(select ifnull(m.max_id, 0) from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex')
+ row_number() over(order by '') as fact_fosalesforecastcortexid, t1.*
from (
select dim_partid,dim_plantid,dim_dateid,dd_reportingdate,dd_country_destination_code,dd_reporting_company_code,dd_insertsource,dd_forecasttype/*,dd_market_grouping*/,
			'Yes' as dd_latestreporting, '10' as dd_holdout
  from tmp_forinsertmissingmonths t
where not exists ( select 1 from fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex m
where f.dim_partid=t.dim_partid and
f.dim_plantid=t.dim_plantid and
f.dim_dateidforecast=d.dim_dateid and
d.datevalue=t.datevalue and
f.dd_reportingdate=t.dd_reportingdate and
f.dd_country_destination_code=t.dd_country_destination_code and
f.dd_reporting_company_code=t.dd_reporting_company_code and
f.dd_insertsource=t.dd_insertsource and
f.dd_forecasttype=t.dd_forecasttype and
 m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
)) t1;
/*END APP-11716*/

merge into fact_fosalesforecastcortex f
using (select distinct fact_fosalesforecastcortexid,min(dim_dateid) as dim_dateid
from dim_date d,fact_fosalesforecastcortex sf,tmp_maxrptdate_cortex m
where d.companycode = 'Not Set'
and d.datevalue = TO_DATE(sf.dd_reportingdate,'DD MON YYYY')
and dd_insertsource='Manual'
and m.dd_reportingdate = TO_DATE(sf.dd_reportingdate,'DD MON YYYY')
group by fact_fosalesforecastcortexid ) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set f.dim_dateidreporting=ifnull(t.dim_dateid,1);

merge into fact_fosalesforecastcortex f
using (select distinct fact_fosalesforecastcortexid,min( cast(concat(year(d.datevalue),case when length(month(d.datevalue))=1 then concat('0',month(d.datevalue)) else month(d.datevalue) end,case when length(day(d.datevalue))=1 then concat('0',day(d.datevalue)) else day(d.datevalue) end)as integer)) as datevalue
from dim_date d,fact_fosalesforecastcortex sf ,tmp_maxrptdate_cortex m
where d.dim_dateid=sf.dim_dateidforecast
and dd_insertsource='Manual'
and m.dd_reportingdate = TO_DATE(sf.dd_reportingdate,'DD MON YYYY')
group by fact_fosalesforecastcortexid ) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set f.dd_forecastdate=ifnull(t.datevalue,'00010101');


update fact_fosalesforecastcortex f
set dd_partnumber=dp.partnumber
from fact_fosalesforecastcortex f,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid=dp.dim_partid
and m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype = 'Manual';


update fact_fosalesforecastcortex f
set dd_plantcode=dp.plant
from fact_fosalesforecastcortex f,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid=dp.dim_partid
and m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype = 'Manual';

update fact_fosalesforecastcortex f
set dd_forecastrank='1'
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex m
where m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype = 'Manual';

update fact_fosalesforecastcortex f
set DD_MARKET_COUNTRY=f.dd_MARKET_GROUPING
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex m
where
m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype = 'Manual';

merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,
avg(f.ct_nasp) as ct_nasp,
avg(f.ct_nasp_pra) as ct_nasp_pra,
avg(f.ct_nasp_cy) as ct_nasp_cy,
avg(f.ct_nasp_global) as ct_nasp_global,
avg(f.ct_nasp_py) as ct_nasp_py,
avg(f.ct_nasp_fc_cy) as ct_nasp_fc_cy,
avg(f.ct_nasp_fc_ny) as ct_nasp_fc_ny,
SUM(f.ct_salesdeliv2mthsnasp) as ct_salesdeliv2mthsnasp,
dt.calendarmonthid,
sum(f.CT_SALESDELIVEREDUSD) as CT_SALESDELIVEREDUSD
from  fact_atlaspharmlogiforecast_merck f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then fos.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
AND m.dd_reportingdate = TO_DATE(fos.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype='Manual'
and dd_version='SFA'
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.ct_nasp = t.ct_nasp,
fos.ct_nasp_pra = t.ct_nasp_pra,
fos.ct_nasp_cy=t.ct_nasp_cy,
fos.ct_nasp_global=t.ct_nasp_global,
fos.ct_nasp_py=t.ct_nasp_py,
fos.ct_nasp_fc_cy=t.ct_nasp_fc_cy,
fos.ct_nasp_fc_ny=t.ct_nasp_fc_ny,
fos.ct_salesdeliv2mthsnasp=t.ct_salesdeliv2mthsnasp,
fos.CT_SALESDELIVEREDUSD=t.CT_SALESDELIVEREDUSD;

/*24 Jul 2020 - MRC-1178*/
merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/, 
                dt.calendarmonthid, f.dd_COUNTRY_DESTINATION_CODE,
                avg(f.CT_NASP_PRAUSED) as CT_NASP_PRAUSED,
                avg(f.CT_NASP_PRAUSEDLC) as CT_NASP_PRAUSEDLC
from  fact_atlaspharmlogiforecast_merck f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND SUBSTR(dp.plant, 1,2) =  f.dd_COUNTRY_DESTINATION_CODE
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set') 
and dd_version='DTA' 
AND f.CT_NASP_PRAUSED IS NOT NULL
AND m.dd_reportingdate = TO_DATE(fos.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype='Manual'
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,
dt.calendarmonthid, f.dd_COUNTRY_DESTINATION_CODE ) t
 on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.CT_NASP_PRAUSED = t.CT_NASP_PRAUSED,
fos.CT_NASP_PRAUSEDLC = t.CT_NASP_PRAUSEDLC;

/* Add logic for future ct_nasp_pra */
create or replace table tmp_fact_fosalesforecastcortex_nasp_pra as
  select
	f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code,
	dd.datevalue,
	f.ct_nasp_pra
from
	fact_fosalesforecastcortex f,
	dim_date dd
where
	dd.dim_dateid = f.dim_dateidforecast
	AND f.DD_LATESTREPORTING = 'Yes';

CREATE OR REPLACE TABLE tmp_fact_fosalesforecastcortex_nasp_max_date as
  SELECT
	f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code,
	max(f.DATEVALUE) max_date
  FROM
	tmp_fact_fosalesforecastcortex_nasp_pra f
  WHERE f.ct_nasp_pra > 0
  GROUP BY f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code;

CREATE OR REPLACE TABLE tmp_fact_fosalesforecastcortex_nasp_max_value as
SELECT
	f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code,
	fm.max_date,
	f.ct_nasp_pra
FROM
	tmp_fact_fosalesforecastcortex_nasp_pra f,
	tmp_fact_fosalesforecastcortex_nasp_max_date fm
WHERE fm.max_date = f.datevalue
  AND fm.dd_partnumber = f.dd_partnumber
  AND fm.dd_plantcode = f.dd_plantcode
  AND fm.dd_reportingdate = f.dd_reportingdate
  AND fm.dd_forecastrank = f.dd_forecastrank
  AND fm.dd_reporting_company_code = f.dd_reporting_company_code
  AND fm.dd_country_destination_code = f.dd_country_destination_code;
        
UPDATE tmp_fact_fosalesforecastcortex_nasp_pra f
   SET f.ct_nasp_pra = fv.ct_nasp_pra
  FROM tmp_fact_fosalesforecastcortex_nasp_pra f,
  	   tmp_fact_fosalesforecastcortex_nasp_max_value fv
 WHERE f.dd_partnumber = fv.dd_partnumber
   AND f.dd_plantcode = fv.dd_plantcode
   AND f.dd_reportingdate = fv.dd_reportingdate
   AND f.dd_forecastrank = fv.dd_forecastrank
   AND f.dd_reporting_company_code = fv.dd_reporting_company_code
   AND f.dd_country_destination_code = fv.dd_country_destination_code
   AND f.DATEVALUE > fv.max_date
   AND f.ct_nasp_pra = 0;

UPDATE fact_fosalesforecastcortex f
   SET f.ct_nasp_pra = fv.ct_nasp_pra
  FROM fact_fosalesforecastcortex f,
  	   tmp_fact_fosalesforecastcortex_nasp_pra fv
 WHERE f.dd_partnumber = fv.dd_partnumber
	   AND f.dd_plantcode = fv.dd_plantcode
	   AND f.dd_reportingdate = fv.dd_reportingdate
	   AND f.dd_forecastrank = fv.dd_forecastrank
	   AND f.dd_reporting_company_code = fv.dd_reporting_company_code
       AND f.dd_country_destination_code = fv.dd_country_destination_code
       AND substr(F.DD_FORECASTDATE ,1,6)= substr(fv.DD_FORECASTDATE ,1,6)
       AND f.ct_nasp_pra = 0
       AND f.ct_nasp_pra <> fv.ct_nasp_pra
       AND f.DD_LATESTREPORTING = 'Yes';
/*end logic for ct_nasp_pra*/ 


drop table if exists tmp_for_updplanttitle;
create table tmp_for_updplanttitle as
select distinct p.partnumber,pl.plantcode,dd_COUNTRY_DESTINATION_CODE AS COUNTRY_DESTINATION_CODE, dd_REPORTING_COMPANY_CODE AS REPORTING_COMPANY_CODE,dd_planttitlemerck,dd_plantcode from fact_atlaspharmlogiforecast_merck f, dim_part p,dim_plant pl
where f.dim_partid=p.dim_partid
and f.dim_plantid=pl.dim_plantid
and dd_planttitlemerck<>'Not Set';

merge into fact_fosalesforecastcortex f
using (
select distinct f.DD_REPORTINGDATE,f.DD_PARTNUMBER,t.DD_PLANTCODE ,f.DD_REPORTING_COMPANY_CODE,f.DD_COUNTRY_DESTINATION_CODE,t.dd_planttitlemerck
from fact_fosalesforecastcortex f,tmp_for_updplanttitle  t, tmp_maxrptdate_cortex r
where f.dd_partnumber=t.partnumber
and f.dd_plantcode=t.plantcode
and f.dd_COUNTRY_DESTINATION_CODE=case when t.plantcode <>'NL10' then 'Not Set' else t.COUNTRY_DESTINATION_CODE end
and f.dd_REPORTING_COMPANY_CODE=t.REPORTING_COMPANY_CODE
and dd_forecasttype='Manual'
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
/*having count(*)>1*/
 ) t
 on f.DD_REPORTINGDATE=t.DD_REPORTINGDATE
AND f.DD_PARTNUMBER = t.DD_PARTNUMBER
AND f.DD_PLANTCODE = t.DD_PLANTCODE
AND f.DD_REPORTING_COMPANY_CODE = t.DD_REPORTING_COMPANY_CODE
AND f.DD_COUNTRY_DESTINATION_CODE = t.DD_COUNTRY_DESTINATION_CODE
when matched then update set f.dd_planttitlemerck=ifnull(t.dd_planttitlemerck,'Not Set'),
f.dd_plantcodemerck=t.dd_plantcode;

drop table if exists tmp_for_salesdeliveredPY;
create table tmp_for_salesdeliveredPY as
select distinct f.dim_partid,f.dim_plantid,dd_COUNTRY_DESTINATION_CODE AS COUNTRY_DESTINATION_CODE, dd_REPORTING_COMPANY_CODE AS REPORTING_COMPANY_CODE,dim_dateidreporting,f.ct_salesdeliveredPY  from fact_atlaspharmlogiforecast_merck f, dim_part p,dim_plant pl
where f.dim_partid=p.dim_partid
and f.dim_plantid=pl.dim_plantid;


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
SUM(f.ct_salesdeliveredPY) as ct_salesdeliveredPY,
dt.calendarmonthid
from tmp_for_salesdeliveredPY f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,dim_plant pl, tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
and f.dim_plantid=pl.dim_plantid
and f.dim_plantid=fos.dim_plantid
AND f.dim_dateidreporting = dt.dim_dateid
AND fos.dd_forecasttype='Manual'
and TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(case when pl.plantcode <>'NL10' then 'Not Set' else f.COUNTRY_DESTINATION_CODE end,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
/*and dp.partnumber ='119977' and dp.plant='IE20'and dt.calendarmonthid like '2016%'*/
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set fos.ct_salesdeliveredPY=t.ct_salesdeliveredPY;

drop table if exists tmp_for_upd_salesdeliveredmonth;
create table tmp_for_upd_salesdeliveredmonth as
select f.dim_partid,dt.datevalue,f.dd_reporting_company_code AS reporting_company_code,dim_dateidreporting,dd_version,(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end) as country_destination_code_upd,
CASE WHEN
MAX( (f.dd_reportavailable) ) = 'Yes'
THEN
SUM(f.ct_salesdeliveredusd) /* MRC-860 */
ELSE
NULL
END as col1,
sum(f.ct_salesmonth1) as col2,sum(f.ct_nasp) as col3 from fact_atlaspharmlogiforecast_merck f,dim_part dp,dim_Date dt
where f.dim_partid=dp.dim_partid
and dim_dateidreporting=dt.dim_dateid
and  dd_version='SFA'
group by f.dim_partid,dt.datevalue,dd_reporting_company_code,dim_dateidreporting,dd_version,(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then dd_country_destination_code else 'Not Set' end);


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
sum(f.col1) as col1,
dt.calendarmonthid
from tmp_for_upd_salesdeliveredmonth f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex m
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(f.country_destination_code_upd,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
AND m.dd_reportingdate = TO_DATE(fos.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype='Manual'
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.ct_salesdeliveredmonth = ifnull(t.col1,0);


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,
dd_forecastrank as dd_forecastrank2
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
where TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and dd_forecasttype='Manual') t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set fos.dd_forecastrank2=t.dd_forecastrank2;

/*This will include only NL10 plant*/
UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE*/ /*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
AND f.dd_plantcode='NL10'
and dd_forecasttype ='Manual'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

/*XX20 separate update*/
UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = case when t.PLANT_CODE ='NL10' then 'XX20' end AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE*//*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and f.dd_plantcode='XX20'
and dd_forecasttype ='Manual'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;


/*This update is for USA0 plants*/

UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE*//* AND f.DD_MARKET_GROUPING = 'USA'*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code in ( 'USA0')
and dd_forecasttype ='Manual'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

/*This update is for the rest of plants*/
merge into fact_fosalesforecastcortex f
using (
select distinct fact_fosalesforecastcortexid,t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE*/ /*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code not in ( 'NL10','USA0')
and dd_forecasttype ='Manual'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY;



/*Train and Test Forecast sample*/
/*This will include NL10 plant*/
UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
/*AND f.DD_HEI_CODE = t.HEI_CODE*/ /*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and d.calendarmonthid < year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0)
and dd_forecasttype ='Manual'
and f.dd_plantcode = 'NL10'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

/*This update is for USA10 plants*/

UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE*/ /*AND f.DD_MARKET_GROUPING = 'USA'*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code in ( 'USA0')
and dd_forecasttype ='Manual'
and d.calendarmonthid < year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) 
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

/*This update is for the rest of plants*/

merge into fact_fosalesforecastcortex f
using (select distinct f.fact_fosalesforecastcortexid,
max(t.PRA_QUANTITY) as PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
/*AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.HEI_CODE*/ /*AND f.DD_MARKET_GROUPING = t.MARKET_GROUPING*/
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code not in ( 'NL10','USA0')
and d.calendarmonthid < year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0)
and dd_forecasttype ='Manual'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
group by f.fact_fosalesforecastcortexid) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY;


/* Update for forecastquantitycustomer <> manual */

UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dd_COUNTRY_DESTINATION_CODE = t.COUNTRY_DESTINATION_CODE
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and d.calendarmonthid < year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0)
and dd_forecasttype NOT IN ('Manual')
and f.dd_plantcode = 'NL10'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

UPDATE fact_fosalesforecastcortex f
SET f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code in ( 'USA0')
and dd_forecasttype NOT IN ('Manual')
and d.calendarmonthid < year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0) 
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

merge into fact_fosalesforecastcortex f
using (select distinct f.fact_fosalesforecastcortexid,
max(t.PRA_QUANTITY) as PRA_QUANTITY
FROM fact_fosalesforecastcortex f,dim_date d,tmp_maxrptdate_cortex r,atlas_forecast_pra_forecasts_merck_DC t
WHERE f.dd_partnumber = t.PRA_UIN AND f.dd_plantcode = t.PLANT_CODE AND f.dd_REPORTING_COMPANY_CODE = t.REPORTING_COMPANY_CODE
AND f.dim_dateidforecast = d.dim_dateid
AND year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) = t.PRA_REPORTING_PERIOD
AND d.calendarmonthid = t.PRA_FORECASTING_PERIOD
and t.plant_code not in ( 'NL10','USA0')
and d.calendarmonthid < year(to_date(f.dd_reportingdate,'DD MON YYYY')) || lpad( month(to_date(f.dd_reportingdate,'DD MON YYYY')),2,0)
and dd_forecasttype NOT IN ('Manual')
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
group by f.fact_fosalesforecastcortexid) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set f.CT_FORECASTQUANTITY_CUSTOMER = t.PRA_QUANTITY
WHERE IFNULL(f.CT_FORECASTQUANTITY_CUSTOMER,0) <> IFNULL(t.PRA_QUANTITY,0);

/* removed by bogdani changed to use shipping qty*/
/*merge into fact_fosalesforecastcortex fos
using (
SELECT distinct 
max(aux.FACT_FOSALESFORECASTCORTEXID ) AS FACT_FOSALESFORECASTCORTEXID ,
fos.dd_forecasttype
,ifnull(c.new_uin, ifnull(b.new_uin,dp.partnumber)) as partnumber
,ifnull(c.NEW_plant_code, ifnull(b.NEW_plant_code,dp.plant)) as plant
,fos.dd_forecastrank
,fos.dd_reportingdate
,fos.dd_forecastdate
,dt.calendarmonthid
,sum(ct_shippedQty) as ct_salesdelivered 
from fact_atlaspharmlogiforecast_merck f
inner join fact_fosalesforecastcortex fos
on f.dim_partid=fos.dim_partid 
and ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set') 
inner join dim_date dt 
on f.dim_dateidreporting = dt.dim_dateid
and year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
inner join dim_part dp
on  f.dim_partid = dp.dim_partid
inner join tmp_maxrptdate_cortex r
on TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate 
inner join tmp_for_upd_from_shipping sp
on sp.dim_partid = f.dim_partid
and sp.dim_plantid = f.dim_plantid
and sp.calendarmonthid = dt.calendarmonthid
LEFT JOIN UIN_Historical_Mappings b 
	on dp.partnumber=b.old_uin
	and dp.plant=b.plant_code
	and f.REPORTING_COMPANY_CODE = b.Old_Reporting_Company_Code
LEFT JOIN UIN_Historical_Mappings c 
	on c.old_uin=b.new_uin
	and b.new_plant_code=c.plant_code
	and b.New_Reporting_Company_Code = c.Old_Reporting_Company_Code
INNER JOIN (SELECT DISTINCT fosaaa.FACT_FOSALESFORECASTCORTEXID  AS FACT_FOSALESFORECASTCORTEXID ,
fosaaa.dd_forecasttype
,dp.partnumber as partnumber
,dp.plant as plant
,fosaaa.dd_forecastrank
,fosaaa.dd_reportingdate
,dd_forecastdate
,dt.calendarmonthid
from fact_atlaspharmlogiforecast_merck f
inner join fact_fosalesforecastcortex fosaaa
on f.dim_partid=fosaaa.dim_partid 
and ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then country_destination_code else 'Not Set' end),'Not Set') = ifnull(fosaaa.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.reporting_company_code,'Not Set')= ifnull(fosaaa.dd_REPORTING_COMPANY_CODE,'Not Set') 
inner join dim_date dt 
on f.dim_dateidreporting = dt.dim_dateid
and year(to_date(to_char(fosaaa.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fosaaa.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
inner join dim_part dp
on  f.dim_partid = dp.dim_partid
inner join tmp_maxrptdate_cortex r
on TO_DATE(fosaaa.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate 
WHERE fosaaa.dd_forecasttype='Manual'
) aux
ON 
fos.dd_forecasttype=aux.dd_forecasttype
AND ifnull(c.new_uin, ifnull(b.new_uin,dp.partnumber))=aux.partnumber
AND ifnull(c.NEW_plant_code, ifnull(b.NEW_plant_code,dp.plant))=aux.plant
AND fos.dd_forecastrank=aux.dd_forecastrank
AND fos.dd_reportingdate=aux.dd_reportingdate
AND fos.dd_forecastdate=aux.dd_forecastdate
AND dt.calendarmonthid= aux.calendarmonthid
where 
fos.dd_forecasttype='Manual'
group by 
fos.dd_forecasttype
,ifnull(c.new_uin, ifnull(b.new_uin,dp.partnumber))
,ifnull(c.NEW_plant_code, ifnull(b.NEW_plant_code,dp.plant))
,fos.dd_forecastrank
,fos.dd_reportingdate
,fos.dd_forecastdate
,dt.calendarmonthid) t
on t.FACT_FOSALESFORECASTCORTEXID =fos.FACT_FOSALESFORECASTCORTEXID 
when matched then update set 	fos.ct_salesquantity=t.ct_salesdelivered */

merge into fact_fosalesforecastcortex fos
using (
SELECT distinct 
max(fos.FACT_FOSALESFORECASTCORTEXID ) AS FACT_FOSALESFORECASTCORTEXID ,
fos.dd_forecasttype
,ifnull(c.new_uin, ifnull(b.new_uin,fos.DD_PARTNUMBER)) as partnumber
,ifnull(c.NEW_plant_code, ifnull(b.NEW_plant_code,fos.DD_PLANTCODE)) as plant
,fos.dd_forecastrank
,fos.dd_reportingdate
,fos.dd_forecastdate
,dt.calendarmonthid
,sum(sp.ct_shippedQty) as ct_salesdelivered 
from fact_fosalesforecastcortex fos
inner join dim_date dt 
on fos.DIM_DATEIDFORECAST = dt.dim_dateid
and year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
inner join tmp_maxrptdate_cortex r
on TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate 
inner join tmp_for_upd_from_shipping sp
on sp.dim_partid = fos.dim_partid
and sp.dim_plantid = fos.dim_plantid
and sp.calendarmonthid = dt.calendarmonthid
LEFT JOIN UIN_Historical_Mappings b 
	on fos.DD_PARTNUMBER=b.old_uin
	and fos.DD_PLANTCODE=b.plant_code
	and fos.DD_REPORTING_COMPANY_CODE = b.Old_Reporting_Company_Code
LEFT JOIN UIN_Historical_Mappings c 
	on c.old_uin=b.new_uin
	and b.new_plant_code=c.plant_code
	and b.New_Reporting_Company_Code = c.Old_Reporting_Company_Code
WHERE fos.dd_forecasttype='Manual'
GROUP BY fos.dd_forecasttype
,ifnull(c.new_uin, ifnull(b.new_uin,fos.DD_PARTNUMBER))
,ifnull(c.NEW_plant_code, ifnull(b.NEW_plant_code,fos.DD_PLANTCODE))
,fos.dd_forecastrank
,fos.dd_reportingdate
,fos.dd_forecastdate
,dt.calendarmonthid) AA
on aa.FACT_FOSALESFORECASTCORTEXID =fos.FACT_FOSALESFORECASTCORTEXID 
when matched then update set fos.ct_salesquantity=aa.ct_salesdelivered;


update fact_fosalesforecastcortex
set ct_salesquantity=null
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex m
where ct_salesquantity=0 and dd_forecasttype='Manual'
and m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY');

update fact_fosalesforecastcortex f
set ct_forecastquantity=f.CT_FORECASTQUANTITY_CUSTOMER
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex m
where
m.dd_reportingdate = TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and dd_forecasttype='Manual';

/*22 Mar 2017 Adding ct_mape_customerfcst for Manual Records*/
DROP TABLE IF EXISTS merck.tmp_custmapes_fosp;
CREATE TABLE merck.tmp_custmapes_fosp
AS
SELECT f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE/*,f.DD_MARKET_GROUPING*/,dd_forecasttype,
100 * avg(abs((ct_forecastquantity_customer-ct_salesquantity)/case when ct_salesquantity=0 then 1 else ct_salesquantity end )) as ct_mape_customerfcst
FROM merck.fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE
cast(concat(substring(dd_forecastdate,1,4) , '-' ,
    substring(dd_forecastdate,5,2) , '-' ,
    substring(dd_forecastdate,7,2) ) as date)
between   add_months(TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-3)  and TO_DATE(f.dd_reportingdate,'DD MON YYYY')
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND dd_forecasttype='Manual'
GROUP BY f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE/*,f.DD_MARKET_GROUPING*/,dd_forecasttype;

UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_mape_customerfcst = t.ct_mape_customerfcst
FROM merck.fact_fosalesforecastcortex f,merck.tmp_custmapes_fosp t,tmp_maxrptdate_cortex r
WHERE f.dd_reportingdate = t.dd_reportingdate AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND ifnull(f.DD_HEI_CODE,'Not Set')= ifnull(t.DD_HEI_CODE,'Not Set') /*AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING*/
and f.dd_forecasttype='Manual'
AND TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
AND f.dd_forecasttype = t.dd_forecasttype;

update fact_fosalesforecastcortex f
set ct_mape=f.ct_mape_customerfcst
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
where TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and dd_forecasttype='Manual';

UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_mape = 0 where f.ct_mape is null
and  dd_forecasttype='Manual';

UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_mape_customerfcst = 0 where f.ct_mape_customerfcst is null
and  dd_forecasttype='Manual';

UPDATE fact_fosalesforecastcortex f
SET dd_latestreporting = 'No'
WHERE dd_latestreporting <> 'No'
and dd_forecasttype='Manual';

UPDATE fact_fosalesforecastcortex f
SET dd_latestreporting = 'Yes'
FROM tmp_maxrptdate_cortex r,fact_fosalesforecastcortex f
where DATE_TRUNC('month',  TO_DATE(f.dd_reportingdate,'DD MON YYYY')) =DATE_TRUNC('month',  r.dd_reportingdate)
and  dd_forecastapproach is null
and dd_forecasttype='Manual';


/*End of BI-5717*/



DELETE FROM stage_fosalesforecastcortex;
DROP TABLE IF EXISTS backup_fact_fosalesforecastcortex_largefcstqty;
drop table if exists fact_fosalesforecastcortex_temp;
DROP TABLE IF EXISTS merck.tmp_custmapes_fosp;
DROP TABLE IF EXISTS tmp_amt_sellingpriceperunit_gbl;
DROP TABLE IF EXISTS tmp_atleast1nonstlinefcst;
DROP TABLE IF EXISTS tmp_avg_sp_fromfso;
DROP TABLE IF EXISTS tmp_ct_salescontribglobal;
DROP TABLE IF EXISTS tmp_custmapes_fosp;
DROP TABLE IF EXISTS tmp_distinctdmdunit_loc_mape;
DROP TABLE IF EXISTS tmp_distinctdmdunit_loc_mape_upd;
DROP TABLE IF EXISTS tmp_fact_salesorder_12monthsales;
DROP TABLE IF EXISTS tmp_fact_salesorder_12monthsales_1;
DROP TABLE IF EXISTS tmp_forecastcov;
DROP TABLE IF EXISTS tmp_forecastcov1;
DROP TABLE IF EXISTS tmp_gaincritlt0;
/*DROP TABLE IF EXISTS tmp_maxrptdate_cortex*/
DROP TABLE IF EXISTS tmp_monthlysales_cov;
DROP TABLE IF EXISTS tmp_partvsmape;
DROP TABLE IF EXISTS tmp_partvsmape_rerank;
DROP TABLE IF EXISTS tmp_qoqmeasures;
DROP TABLE IF EXISTS tmp_qoqmeasures_2;
DROP TABLE IF EXISTS tmp_qoqmeasures_3;
DROP TABLE IF EXISTS tmp_rerank_fcst;
drop table if exists tmp_saleshistory_grain_reqmonths;
drop table if exists tmp_saleshistory_grain_reqmonths_2;
DROP TABLE IF EXISTS tmp_sod_denorm_fcst;
DROP TABLE IF EXISTS tmp_update_nasmpeasures_to_fopsfcst;
DROP TABLE IF EXISTS tmp_upd_ct_volatility_yoy_percent;
DROP TABLE IF EXISTS tmp_upd_ct_volatility_yoy_percent_overall;
DROP TABLE IF EXISTS tmp_upd_ct_volatility_yoy_percent_STDDEV;
DROP TABLE IF EXISTS tmp_upd_dd_sales_rank;
DROP TABLE IF EXISTS tmp_upd_fcstrank_fosf;
DROP TABLE IF EXISTS tmp_upd_fcstrank_fosf1;
DROP TABLE IF EXISTS tmp_upd_fcstrank_fosf2;
DROP TABLE IF EXISTS tmp_upd_fcstrank_fosf3;
DROP TABLE IF EXISTS tmp_upd_forecastproductcategory;
DROP TABLE IF EXISTS tmp_upd_forecastproductcategory_2;


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
sum(f.ct_forecast4mth1) as ct_forecast4mth1,
sum(f.ct_forecast4mth2) as ct_forecast4mth2,
sum(f.ct_salesmonth1) as ct_salesmonth1,
sum(f.ct_salesmonth2) as ct_salesmonth2,
dt.calendarmonthid
from fact_atlaspharmlogiforecast_merck f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
/*
and dp.partnumber ='136190' and dp.plant='NL10'
and dt.calendarmonthid like '201702%'
AND dd_reportingdate = '09 Mar 2017'
*/
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,dt.calendarmonthid
) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then
update set fos.ct_forecast4mth1=t.ct_forecast4mth1,
fos.ct_forecast4mth2=t.ct_forecast4mth2,
fos.ct_salesmonth1=t.ct_salesmonth1,
fos.ct_salesmonth2=t.ct_salesmonth2;



merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,
dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate, dt.calendarmonthid,f.dd_version,
f.dd_forecast4mthis1null,
f.dd_forecast4mthis2null
from fact_atlaspharmlogiforecast_merck f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and f.dd_version = 'SFA'
and  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate

) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then
update set fos.dd_forecast4mthis1null=t.dd_forecast4mthis1null,
fos.dd_forecast4mthis2null=t.dd_forecast4mthis2null;


update fact_fosalesforecastcortex fos
set ct_forecast4mth1 = 0 where ct_forecast4mth1 is null;

update fact_fosalesforecastcortex fos
set ct_forecast4mth2 = 0 where ct_forecast4mth2 is null;

update fact_fosalesforecastcortex fos
set ct_salesmonth1 = 0 where ct_salesmonth1 is null;

update fact_fosalesforecastcortex fos
set ct_salesmonth2 = 0 where ct_salesmonth2 is null;




merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,fos.dd_market_grouping*/,
SUM(f.CT_SALESDELIVMOVING) as CT_SALESDELIVMOVING,
dt.calendarmonthid
from fact_atlaspharmlogiforecast_merck f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and f.dd_version = 'SFA'

/*
and dp.partnumber ='136190' and dp.plant='NL10'
and dt.calendarmonthid like '201702%'
AND dd_reportingdate = '09 Mar 2017'
*/
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,fos.dd_market_grouping*/,dt.calendarmonthid
) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then
update set fos.CT_SALESDELIVMOVING=t.CT_SALESDELIVMOVING;


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*, dd_market_grouping*/,
   sum(f.ct_actualsMovingAnnualTotal_USD) as CT_SALESDELIVMOVINGusd, /* MRC-860 */ 
dt.calendarmonthid
from fact_atlaspharmlogiforecast_merck f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and f.dd_version = 'SFA'

group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*,dd_market_grouping*/,dt.calendarmonthid
) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then
update set fos.CT_SALESDELIVMOVINGusd = t.CT_SALESDELIVMOVINGusd;


drop table if exists tmp_for_upd_salesdelivactual_ytd;
create table tmp_for_upd_salesdelivactual_ytd as
select f.dim_partid,dt.datevalue,dd_reporting_company_code AS reporting_company_code,dim_dateidreporting,dd_version/*,market_grouping*/,
(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end) as country_destination_code_upd,
ct_Salesdelivactualytd as col01
from fact_atlaspharmlogiforecast_merck f,dim_part dp,dim_Date dt
where f.dim_partid=dp.dim_partid
and dim_dateidreporting=dt.dim_dateid
and  dd_version='SFA';

merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*, dd_market_grouping*/,
sum(f.col01) as col01,
dt.calendarmonthid
from tmp_for_upd_salesdelivactual_ytd f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(f.country_destination_code_upd,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate/*, dd_market_grouping*/,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.CT_DELIVEREDQTY_YTD = ifnull(t.col01,0);


/*Georgiana Changes according to APP-6365: Calculate all quantities that are needed for FO DFA-4 Measure*/

/*Pick the correct holdout period, the 3 month holdout was requested, but not all runs had the holdout perios for 3 months so in those cases we are picking the lowest value*/
drop table if exists tmp_for_holdoutperiod;
create table tmp_for_holdoutperiod as
select distinct f.dd_reportingdate,dd_forecastsample,dd_forecastdate from fact_fosalesforecastcortex f, tmp_maxrptdate_cortex r where
TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and dd_forecastsample='Test';

drop table if exists tmp_for_holdoutperiod_2;
create table tmp_for_holdoutperiod_2 as
select distinct dd_reportingdate, count(*) as holdout from tmp_for_holdoutperiod
group by dd_reportingdate;

update fact_fosalesforecastcortex
set dd_holdout=holdout
from  fact_fosalesforecastcortex f,tmp_for_holdoutperiod_2 t
where f.dd_reportingdate=t.dd_reportingdate
and ifnull(dd_holdout,0)<>holdout;


drop table if exists tmp_for_selectminholdout;
create table tmp_for_selectminholdout as
select month(TO_DATE(f.dd_reportingdate,'DD MON YYYY')) as holdoutmonth,year(TO_DATE(f.dd_reportingdate,'DD MON YYYY')) as holdoutyear,min(dd_holdout) as min_dd_holdout
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex m
where  month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=month(m.dd_reportingdate)
and year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=year(TO_DATE(m.dd_reportingdate,'DD MON YYYY'))
group by month(TO_DATE(f.dd_reportingdate,'DD MON YYYY')),year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'));


drop table if exists tmp_forlastmonthholdout;
create table  tmp_forlastmonthholdout as select distinct case when month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))-1=0 then 12 else month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))-1 end  as holdoutmonth,
case when month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))-1=0 then year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))-1 else year(TO_DATE(f.dd_reportingdate,'DD MON YYYY')) end as holdoutyear,dd_holdout
from fact_fosalesforecastcortex f;


drop table if exists tmp_for_selectminholdoutlast4month;
create table tmp_for_selectminholdoutlast4month as
select distinct
month(TO_DATE(f.dd_reportingdate,'DD MON YYYY')) as holdoutmonth,
year(TO_DATE(f.dd_reportingdate,'DD MON YYYY')) as holdoutyear,min(dd_holdout) as min_dd_holdout
from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex m
where  month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=case when month(m.dd_reportingdate)-4 <=0 then month(m.dd_reportingdate)+8 else month(m.dd_reportingdate)-4 end
and year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=case when month(m.dd_reportingdate)-4 <=0 then year(m.dd_reportingdate)-1 else year(m.dd_reportingdate) end
group by month(TO_DATE(f.dd_reportingdate,'DD MON YYYY')),year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'));

/*Sales from previous Month */
drop table if exists tmp_for_prevmonthsales;
create table tmp_for_prevmonthsales as
select distinct f.dd_reportingdate,f.dd_forecastdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,f.dd_country_destination_code,f.dd_forecasttype,/*f.dd_market_grouping,*/f.dd_forecastrank,f.dd_holdout,
lag(avg(f.ct_salesquantity)) over (partition by f.dd_reportingdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,f.dd_country_destination_code,f.dd_forecasttype,/*f.dd_market_grouping,*/f.dd_forecastrank,f.dd_holdout order by f.dd_forecastdate asc) as ct_salesquantity
from fact_fosalesforecastcortex f,tmp_for_selectminholdout t
where  month(TO_DATE(dd_reportingdate,'DD MON YYYY'))=t.holdoutmonth
and year(TO_DATE(dd_reportingdate,'DD MON YYYY'))=t.holdoutyear
and f.dd_holdout=t.min_dd_holdout
group by f.dd_reportingdate,f.dd_forecastdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,f.dd_country_destination_code,f.dd_forecasttype,/*f.dd_market_grouping,*/f.dd_forecastrank,f.dd_holdout;

merge into fact_fosalesforecastcortex f
using (
select distinct  f.fact_fosalesforecastcortexid,t.ct_salesquantity
from fact_fosalesforecastcortex f, tmp_for_prevmonthsales t
where t.dd_reportingDate=f.dd_reportingdate
and t.dd_partnumber=f.dd_partnumber
and t.dd_plantcode=f.dd_plantcode
and t.dd_reporting_company_code=f.dd_reporting_company_code
and t.dd_country_destination_code=f.dd_country_destination_code
and t.dd_forecasttype=f.dd_forecasttype
AND substr(F.DD_FORECASTDATE ,1,6)= substr(t.DD_FORECASTDATE ,1,6)
/*and t.dd_market_grouping=f.dd_market_grouping*/
and f.dd_holdout=t.dd_holdout
and f.dd_forecastrank=t.dd_forecastrank

) t
on f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid
when matched then update set  f.ct_salesquantityprevmonth=ifnull(t.ct_salesquantity,0);

/*Forecast 4 months from current run for current month run*/

drop table if exists tmp_for_prev4repmonth;
create table tmp_for_prev4repmonth as
select distinct dd_reportingdate,dd_partnumber,dd_plantcode,dd_reporting_company_code,dd_forecastdate,
case when dd_plantcode='NL10' then dd_country_destination_code else 'Not Set' end as dd_countrydestination_code,dd_forecasttype,dd_hei_code,/*dd_market_grouping,*/dd_forecastrank,f.dd_holdout,
case when month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4<=0  then (month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4 +12) else month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4 end foremonth,
case when month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4<=0 then year(TO_DATE(dd_reportingdate,'DD MON YYYY'))-1 else year(TO_DATE(dd_reportingdate,'DD MON YYYY')) end foreyear ,
month(TO_DATE(dd_reportingdate,'DD MON YYYY')) as snapmonth,
year(TO_DATE(dd_reportingdate,'DD MON YYYY')) as snapyear
from fact_fosalesforecastcortex f, tmp_for_selectminholdout t
where  month(TO_DATE(dd_reportingdate,'DD MON YYYY'))=t.holdoutmonth
and year(TO_DATE(dd_reportingdate,'DD MON YYYY'))=t.holdoutyear
/*and dd_partnumber='013774'
and dD_reportingdate='05 May 2017'*/
and f.dd_holdout=t.min_dd_holdout;


drop table if exists tmp_for_prev4monthforecast;
create table tmp_for_prev4monthforecast as
select  t.dd_reportingdate,f.dd_forecastdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end as dd_country_destination_code,t.dd_forecasttype,t.dd_hei_code,/*f.dd_market_grouping,*/t.dd_forecastrank,t.dd_holdout,avg(ct_forecastquantity) as ct_forecastquantity4mth1
from fact_fosalesforecastcortex f, tmp_for_prev4repmonth t,tmp_for_selectminholdoutlast4month t1
where
 t.foremonth=month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))
and t.foreyear=year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))
AND substr(F.DD_FORECASTDATE ,1,6)= substr(t.DD_FORECASTDATE ,1,6)
and t.dd_partnumber=f.dd_partnumber
and t.dd_plantcode=f.dd_plantcode
and t.dd_reporting_company_code=f.dd_reporting_company_code
and t.dd_countrydestination_code=case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end
and t.dd_forecasttype=f.dd_forecasttype
and month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=t1.holdoutmonth
and year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=t1.holdoutyear
and f.dd_holdout=t1.min_dd_holdout
/*and t.dd_market_grouping=f.dd_market_grouping*/
group by  t.dd_reportingdate,f.dd_forecastdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end,t.dd_forecasttype,t.dd_hei_code,/*f.dd_market_grouping,*/t.dd_forecastrank,t.dd_holdout;


merge into fact_fosalesforecastcortex f
using (select distinct fact_fosalesforecastcortexid,t.dd_forecasttype,t.ct_forecastquantity4mth1
from fact_fosalesforecastcortex f, tmp_for_prev4monthforecast t
where t.dd_reportingDate=f.dd_reportingdate
and t.dd_partnumber=f.dd_partnumber
and t.dd_plantcode=f.dd_plantcode
and t.dd_reporting_company_code=f.dd_reporting_company_code
and case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end=t.dd_country_destination_code
and t.dd_forecasttype=f.dd_forecasttype
and t.dd_hei_code=f.dd_hei_code
/*and t.dd_market_grouping=f.dd_market_grouping*/
AND substr(F.DD_FORECASTDATE ,1,6)= substr(t.DD_FORECASTDATE ,1,6)
and f.dd_forecastrank=t.dd_forecastrank
and f.dd_holdout=t.dd_holdout) t
on f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid
when matched then update set  f.ct_forecastquantity4mth1=ifnull(t.ct_forecastquantity4mth1,0);



/*Forecast 4 months from current run for previous month run*/


drop table if exists tmp_for_prev4repmonth;
create table tmp_for_prev4repmonth as
select distinct dd_reportingdate,dd_partnumber,dd_plantcode,dd_reporting_company_code,dd_forecastdate,
case when dd_plantcode='NL10' then dd_country_destination_code else 'Not Set' end as dd_countrydestination_code,dd_forecasttype,dd_hei_code,/*dd_market_grouping,*/dd_forecastrank,f.dd_holdout,
case when month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4<=0  then (month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4 +12) else month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4 end foremonth,
case when month(TO_DATE(dd_reportingdate,'DD MON YYYY'))-4<=0 then year(TO_DATE(dd_reportingdate,'DD MON YYYY'))-1 else year(TO_DATE(dd_reportingdate,'DD MON YYYY')) end foreyear ,
case when month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))-1=0 then month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))+11 else month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))-1 end as snapmonth,
case when month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))-1=0 then year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))-1 else year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) end as snapyear
from fact_fosalesforecastcortex f, tmp_for_selectminholdout t
where  month(TO_DATE(dd_reportingdate,'DD MON YYYY'))=t.holdoutmonth
and year(TO_DATE(dd_reportingdate,'DD MON YYYY'))=t.holdoutyear
and f.dd_holdout=t.min_dd_holdout;
/*and dd_partnumber='013774'
and dD_reportingdate='05 May 2017'*/
/*and  dd_partnumber='131058' and dd_plantcode='CZ20' and dd_reportingdate='05 May 2017' */


drop table if exists tmp_for_prev4monthforecast;
create table tmp_for_prev4monthforecast as
select  t.dd_reportingdate,t.dd_forecastdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end as dd_country_destination_code,t.dd_forecasttype,t.dd_hei_code,/*f.dd_market_grouping,*/t.dd_forecastrank,t.dd_holdout,avg(ct_forecastquantity) as ct_forecastquantity4mth2
from fact_fosalesforecastcortex f, tmp_for_prev4repmonth t,tmp_for_selectminholdoutlast4month t1
where
 t.foremonth=month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))
and t.foreyear=year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))
and t.dd_partnumber=f.dd_partnumber
and t.dd_plantcode=f.dd_plantcode
and t.dd_reporting_company_code=f.dd_reporting_company_code
and t.dd_countrydestination_code=case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end
and  month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))=t.snapmonth
and year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))=t.snapyear
and month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=t1.holdoutmonth
and year(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))=t1.holdoutyear
and f.dd_holdout=t1.min_dd_holdout
and t.dd_forecasttype=f.dd_forecasttype
/*and t.dd_market_grouping=f.dd_market_grouping*/
group by  t.dd_reportingdate,t.dd_forecastdate,f.dd_partnumber,f.dd_plantcode,f.dd_reporting_company_code,case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end,t.dd_forecasttype,t.dd_hei_code,/*f.dd_market_grouping,*/t.dd_forecastrank,t.dd_holdout;



merge into fact_fosalesforecastcortex f
using (select distinct fact_fosalesforecastcortexid,t.ct_forecastquantity4mth2
from fact_fosalesforecastcortex f, tmp_for_prev4monthforecast t
where t.dd_reportingDate=f.dd_reportingdate
and t.dd_partnumber=f.dd_partnumber
and t.dd_plantcode=f.dd_plantcode
and t.dd_reporting_company_code=f.dd_reporting_company_code
and case when f.dd_plantcode='NL10' then f.dd_country_destination_code else 'Not Set' end=t.dd_country_destination_code
and t.dd_forecasttype=f.dd_forecasttype
and t.dd_hei_code=f.dd_hei_code
AND substr(F.DD_FORECASTDATE ,1,6)= substr(t.DD_FORECASTDATE ,1,6)
/*and t.dd_market_grouping=f.dd_market_grouping*/
and f.dd_forecastrank=t.dd_forecastrank
and f.dd_holdout=t.dd_holdout) t
on f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid
when matched then update set  f.ct_forecastquantity4mth2=ifnull(t.ct_forecastquantity4mth2,0);

/*APP-6365 End*/

/*Georgiana Adding changes according to APP-7309*/

DROP TABLE IF EXISTS merck.tmp_custmapes_fosp_2;
CREATE TABLE merck.tmp_custmapes_fosp_2
AS
SELECT f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,/*f.DD_MARKET_GROUPING,*/dd_forecasttype,
100 * avg(abs((ct_forecastquantity-ct_salesquantity)/ct_salesquantity)) ct_mape_fofcst
FROM merck.fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE
cast(concat(substring(dd_forecastdate,1,4) , '-' ,
    substring(dd_forecastdate,5,2) , '-' ,
    substring(dd_forecastdate,7,2) ) as date)
between   add_months(TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-12)  and TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and ct_salesquantity <> 0
GROUP BY f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,/*f.DD_MARKET_GROUPING,*/dd_forecasttype;

UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_mape12months = ifnull(t.ct_mape_fofcst,0)
FROM merck.fact_fosalesforecastcortex f,merck.tmp_custmapes_fosp_2 t
WHERE f.dd_reportingdate = t.dd_reportingdate AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.DD_HEI_CODE /*AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING*/
AND f.dd_forecasttype = t.dd_forecasttype;

DROP TABLE IF EXISTS merck.tmp_custmapes_fosp_2_customerforecast;
CREATE TABLE merck.tmp_custmapes_fosp_2_customerforecast
AS
SELECT f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,/*f.DD_MARKET_GROUPING,*/dd_forecasttype,
100 * avg(abs((ct_forecastquantity_customer-ct_salesquantity)/case when ct_salesquantity=0 then 1 else ct_salesquantity end )) as ct_mape_customerfcst
FROM merck.fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
WHERE
cast(concat(substring(dd_forecastdate,1,4) , '-' ,
    substring(dd_forecastdate,5,2) , '-' ,
    substring(dd_forecastdate,7,2) ) as date)
between   add_months(TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-12)  and TO_DATE(f.dd_reportingdate,'DD MON YYYY')
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
GROUP BY f.dd_reportingdate,dd_partnumber,dd_plantcode,f.dd_REPORTING_COMPANY_CODE,f.dd_COUNTRY_DESTINATION_CODE,f.DD_HEI_CODE,/*f.DD_MARKET_GROUPING,*/dd_forecasttype;

UPDATE merck.fact_fosalesforecastcortex f
SET f.ct_mapecustomerforecast12months = ifnull(t.ct_mape_customerfcst,0)
FROM merck.fact_fosalesforecastcortex f,merck.tmp_custmapes_fosp_2_customerforecast t
WHERE f.dd_reportingdate = t.dd_reportingdate AND f.dd_partnumber = t.dd_partnumber AND f.dd_plantcode = t.dd_plantcode
AND f.dd_REPORTING_COMPANY_CODE = t.dd_REPORTING_COMPANY_CODE AND f.dd_COUNTRY_DESTINATION_CODE = t.dd_COUNTRY_DESTINATION_CODE
AND f.DD_HEI_CODE = t.DD_HEI_CODE /*AND f.DD_MARKET_GROUPING = t.DD_MARKET_GROUPING*/
AND f.dd_forecasttype = t.dd_forecasttype;

/*Georgiana Added changes for Forecast Selected Editable field form PF SA*/

/*update fact_fosalesforecastcortex
set dd_lowermape=CASE WHEN  CT_MAPE_CUSTOMERFCST  <  ct_mape THEN 'Current' WHEN  CT_MAPE_CUSTOMERFCST  >  ct_mape THEN 'Aera' ELSE ' - ' END
from fact_fosalesforecastcortex fos, tmp_maxrptdate_cortex r
where  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate*/

/*Calculating lower mape at material, plant and rank level*/
 merge into fact_fosalesforecastcortex f
using ( select distinct   f.dd_reportingdate,dd_partnumber,dd_plantcode,dd_forecastrank, CASE WHEN  avg(CT_MAPE_CUSTOMERFCST)  <  avg(ct_mape) THEN 'Current' WHEN  avg(CT_MAPE_CUSTOMERFCST) >  avg(ct_mape) THEN 'Aera' ELSE ' - ' END as lowermape
from fact_fosalesforecastcortex f, tmp_maxrptdate_cortex r
where  TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
group by  f.dd_reportingdate,dd_partnumber,dd_plantcode,dd_forecastrank
) t
on t.dd_reportingdate=f.dd_reportingdate
and t.dd_partnumber=f.dd_partnumber
and t.dd_plantcode = f.dd_plantcode
and t.dd_forecastrank=f.dd_forecastrank
when matched then update set f.dd_lowermape=t.lowermape;

/*25 Oct 2017 Georgiana changes: added new field category according to APP-7656*/
update fact_fosalesforecastcortex
set dd_category =to_char(add_months(TO_DATE(dd_reportingdate,'DD MON YYYY'),1),'Mon_YYYY')
from fact_fosalesforecastcortex fos, tmp_maxrptdate_cortex r
where  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

merge into fact_fosalesforecastcortex a
using( select distinct s.region,s.market_grouping
from region_marketing_grouping s) t
on t.market_grouping=a.dd_market_grouping
when matched then update set a.dd_region=ifnull(t.region,'Not Set');

/* 17 Jan 2018 Georgiana Changes according to APP-8530 new logic for NL10/XX20 cases, where we have one material with both NL10 and XX20 Plant Codes only the NL10 should be kept*/
delete from fact_fosalesforecastcortex
where TO_DATE( dd_reportingdate,'DD MON YYYY') in ( select distinct dd_reportingdate from tmp_maxrptdate_cortex r)
and dim_plantid='91'
and dd_plantcode='XX20';

merge into fact_fosalesforecastcortex f
using(
select distinct f.fact_fosalesforecastcortexid ,f.dim_partid, f.DD_PLANTCODEMERCK, f.DD_FORECASTRANK, f.dd_reportingdate, dt.monthyear,
AVG(CT_MAPE) as FOMAPE, AVG(CT_MAPE_CUSTOMERFCST)  AS CURRENTMAPE FROM  fact_fosalesforecastcortex f, dim_date dt,tmp_maxrptdate_cortex r
where  f.dim_dateidforecast = dt.dim_dateid
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
group by f.fact_fosalesforecastcortexid,f.dim_partid, f.DD_PLANTCODEMERCK, f.DD_FORECASTRANK, f.dd_reportingdate, dt.monthyear) t
on t.fact_fosalesforecastcortexid = f.fact_fosalesforecastcortexid
when matched then update set
f.DD_FOMAPE = ifnull(t.FOMAPE,0), f.DD_CURRENTMAPE = ifnull(t.CURRENTMAPE,0)
;

/*Georgiana 02 Jul 2018 - new logic for ct_forecastquantity_costomer according to APP-9892*/

/*Insert new records in PF with Manual Forecast type in order to have the next 2 years forecast date availavale, for example in July in PF we have the max forecast date 202006 with Manual forecasst type, and we need to insert another record for 202007*/
/*for this insert we are copying the row from the same month last year together will all details and insert it for 202007*/
/*The quantity will be updated bellow*/


drop table if exists tmp_for_insertnullforecastforManualdate;
create table tmp_for_insertnullforecastforManualdate as
select distinct max(calendarmonthid) as maxcalendarmonthid from fact_fosalesforecastcortex f ,dim_date d1,tmp_maxrptdate_cortex r
where TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and dim_dateidforecast=d1.dim_dateid
and dd_forecasttype='Manual';


delete from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex';

insert into number_fountain
select  'fact_fosalesforecastcortex',
ifnull(max(d.fact_fosalesforecastcortexid),
	ifnull((select min(s.dim_projectsourceid * s.multiplier)
			from dim_projectsource s),0))
from fact_fosalesforecastcortex d
WHERE d.fact_fosalesforecastcortexid <> 1;



insert into fact_fosalesforecastcortex
(fact_fosalesforecastcortexid,DIM_PARTID,DIM_PLANTID,DD_COMPANYCODE,DIM_DATEIDREPORTING,DD_FORECASTTYPE,DD_FORECASTSAMPLE,
dim_dateidforecast,
CT_FORECASTQUANTITY,CT_MAPE_OLD,DD_FORECASTRANK,DD_FORECASTMODE,DIM_PROJECTSOURCEID,DD_LATESTREPORTING,CT_FORECASTQUANTITY_CUSTOMER,
DD_FORECASTGRAINATTRONE,DD_FORECASTGRAINATTRTWO,DD_FORECASTGRAINATTRTHREE,DD_FORECASTGRAINATTRFOUR,DD_FORECASTGRAINATTRFIVE,
DD_FORECASTGRAIN,CT_SALESQUANTITY_NEW,CT_SALESQUANTITY,AMT_STDCOSTPERUNIT,AMT_SELLINGPRICEPERUNIT,DIM_SALESUOMID,DD_LASTDATE,DD_HOLDOUTDATE,
DD_REPORTINGDATE,
 dd_forecastdate,
CT_LOWPI,CT_HIGHPI,CT_SALES_PREVMONTH,CT_SALES_PREVQTR,CT_SALES_PREVYR,CT_FORECAST_PREVMONTH,CT_FORECAST_PREVQTR,CT_FORECAST_PREVYR,
CT_SALES_CURRENTQTR,CT_FORECAST_CURRENTQTR,CT_SALES_PREVQTR_PREVYEAR,CT_FORECAST_PREVQTR_PREVYEAR,DIM_PART_PRESCRIPTIVEID,
DIM_PLANT_PRESCRIPTIVEID,STD_EXCHANGERATE_DATEID,DD_LASTREFRESHDATE,DD_PARTNUMBER,DD_PLANTCODE,CT_BIAS_ERROR,CT_BIAS_ERROR_RANK,CT_MAPE_CUSTOMERFCST,
CT_SALESCONTRIBGLOBAL,CT_RATIO_MAPE,DD_SALESCONTRIBUTION_QUARTILE_RANK,AMT_SHIPPEDQTYYTD,AMT_EXCHANGERATE_GBL,AMT_EXCHANGERATE,CT_DELIVEREDQTY_YTD,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,
DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE/*,DD_MARKET_GROUPING*/,CT_NASP,CT_NASP_CY,CT_NASP_GLOBAL,CT_NASP_PY,CT_NASP_FC_CY,CT_NASP_FC_NY,DD_MARKET_COUNTRY,CT_COEFFOFVARIATION,
DD_SALESCONTRIBUTION_3LEVELS_RANK,CT_VOLATILITY_YOY_PERCENT,DD_VOLATILITY_YOY_GT50PC,DD_FORECASTQUANTITY,CT_SALESDELIV2MTHSIRU,CT_SALESDELIV2MTHSNASP,DD_DYNAMICCONTRIBUTIONRANK,
DD_DYNAMICCONTRIBUTION,DD_HOME_VS_EXPORT,CT_COEFFOFVARIATION_FORECAST,DD_FORECASTPRODUCTCATEGORY,DD_HEICODE_NEW,DD_FLAG_SALESQUANTITY_EDITABLE,DD_PLANTTITLEMERCK,DD_PLANTCODEMERCK,
DD_REPORTAVAILABLE,CT_SALESDELIVEREDPY,CT_SALESDELIVEREDMONTH,DD_FORECASTRANK2,DD_INSERTSOURCE,CT_SALESDELIVACTUALYTD,CT_FORECAST4MTH1,CT_FORECAST4MTH2,CT_SALESMONTH1,CT_SALESMONTH2,
DD_FORECAST3MTHISNULL,DD_FORECAST4MTHIS1NULL,DD_FORECAST4MTHIS2NULL,DD_FORECAST4MTHISNULL,DD_FORECAST4MTHISNULL_SUBPRODLEV,DD_FORECAST6MTHISNULL,DD_FORECAST9MTHISNULL,CT_SALESDELIVMOVING,
DD_FOMAPE,DD_CURRENTMAPE,CT_SALESDELIVMOVINGUSD,CT_MAPE,CT_SALESDELIVACTUALYTD_TESTGH,CT_SALESDELIVMOVINGUSD_TESTGH,DIM_CURRENCYID,CT_SALESQUANTITYPREVMONTH,CT_FORECASTQUANTITY4MTH1,
DD_HOLDOUT,CT_FORECASTQUANTITY4MTH2,DD_FORECASTSELECTED,CT_FODFA_4,DD_UNIQUEKEY,CT_MAPE12MONTHS,CT_MAPECUSTOMERFORECAST12MONTHS,DD_LOWERMAPE,DD_CATEGORY,DD_COMMENTS,CT_FORECASTQUANTITYRANK1,
CT_FORECASTQUANTITY_CUSTOMER_RANK1,DD_REGION,DD_FORECASTAPPROACH,DW_UPDATE_DATE,FLAG_UPDATE,DD_FORECASTCHANGED_BYMERCK,CT_MONTHSSINCELASTCHANGE,CT_FORECASTSELECTEDLASTMONTH,CT_FORECASTQUANTITYSELECTED,
CT_SALESDELIVMOVING_NEW,
DD_FORECASTSELECTEDFINAL,DD_NEWMATERIALNUMBER,
DD_OLDATERIALNUMBER,dd_confirmed,DD_FORECASTSELECTED_ORIG,DD_CURRENCYCODE,CT_EXCHANGERATE_PL)
select
(select ifnull(m.max_id, 0) from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex')
+ row_number() over(order by '') as fact_fosalesforecastcortexid,
t.*
from
(select distinct DIM_PARTID,DIM_PLANTID,DD_COMPANYCODE,DIM_DATEIDREPORTING,DD_FORECASTTYPE,DD_FORECASTSAMPLE,
min(d2.dim_dateid) as dim_dateidforecast,
CT_FORECASTQUANTITY,CT_MAPE_OLD,DD_FORECASTRANK,DD_FORECASTMODE,DIM_PROJECTSOURCEID,DD_LATESTREPORTING,CT_FORECASTQUANTITY_CUSTOMER,
DD_FORECASTGRAINATTRONE,DD_FORECASTGRAINATTRTWO,DD_FORECASTGRAINATTRTHREE,DD_FORECASTGRAINATTRFOUR,DD_FORECASTGRAINATTRFIVE,
DD_FORECASTGRAIN,CT_SALESQUANTITY_NEW,CT_SALESQUANTITY,AMT_STDCOSTPERUNIT,AMT_SELLINGPRICEPERUNIT,DIM_SALESUOMID,DD_LASTDATE,DD_HOLDOUTDATE,
f.DD_REPORTINGDATE,
min( cast(concat(year(d2.datevalue),case when length(month(d2.datevalue))=1 then concat('0',month(d2.datevalue)) else month(d2.datevalue) end,case when length(day(d2.datevalue))=1 then concat('0',day(d2.datevalue)) else day(d2.datevalue) end)as integer)) as dd_forecastdate,
CT_LOWPI,CT_HIGHPI,CT_SALES_PREVMONTH,CT_SALES_PREVQTR,CT_SALES_PREVYR,CT_FORECAST_PREVMONTH,CT_FORECAST_PREVQTR,CT_FORECAST_PREVYR,
CT_SALES_CURRENTQTR,CT_FORECAST_CURRENTQTR,CT_SALES_PREVQTR_PREVYEAR,CT_FORECAST_PREVQTR_PREVYEAR,DIM_PART_PRESCRIPTIVEID,
DIM_PLANT_PRESCRIPTIVEID,STD_EXCHANGERATE_DATEID,DD_LASTREFRESHDATE,DD_PARTNUMBER,DD_PLANTCODE,CT_BIAS_ERROR,CT_BIAS_ERROR_RANK,CT_MAPE_CUSTOMERFCST,
CT_SALESCONTRIBGLOBAL,CT_RATIO_MAPE,DD_SALESCONTRIBUTION_QUARTILE_RANK,AMT_SHIPPEDQTYYTD,AMT_EXCHANGERATE_GBL,AMT_EXCHANGERATE,CT_DELIVEREDQTY_YTD,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,
DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE/*,DD_MARKET_GROUPING*/,CT_NASP,CT_NASP_CY,CT_NASP_GLOBAL,CT_NASP_PY,CT_NASP_FC_CY,CT_NASP_FC_NY,DD_MARKET_COUNTRY,CT_COEFFOFVARIATION,
DD_SALESCONTRIBUTION_3LEVELS_RANK,CT_VOLATILITY_YOY_PERCENT,DD_VOLATILITY_YOY_GT50PC,DD_FORECASTQUANTITY,CT_SALESDELIV2MTHSIRU,CT_SALESDELIV2MTHSNASP,DD_DYNAMICCONTRIBUTIONRANK,
DD_DYNAMICCONTRIBUTION,DD_HOME_VS_EXPORT,CT_COEFFOFVARIATION_FORECAST,DD_FORECASTPRODUCTCATEGORY,DD_HEICODE_NEW,DD_FLAG_SALESQUANTITY_EDITABLE,DD_PLANTTITLEMERCK,DD_PLANTCODEMERCK,
DD_REPORTAVAILABLE,CT_SALESDELIVEREDPY,CT_SALESDELIVEREDMONTH,DD_FORECASTRANK2,DD_INSERTSOURCE,CT_SALESDELIVACTUALYTD,CT_FORECAST4MTH1,CT_FORECAST4MTH2,CT_SALESMONTH1,CT_SALESMONTH2,
DD_FORECAST3MTHISNULL,DD_FORECAST4MTHIS1NULL,DD_FORECAST4MTHIS2NULL,DD_FORECAST4MTHISNULL,DD_FORECAST4MTHISNULL_SUBPRODLEV,DD_FORECAST6MTHISNULL,DD_FORECAST9MTHISNULL,CT_SALESDELIVMOVING,
DD_FOMAPE,DD_CURRENTMAPE,CT_SALESDELIVMOVINGUSD,CT_MAPE,CT_SALESDELIVACTUALYTD_TESTGH,CT_SALESDELIVMOVINGUSD_TESTGH,DIM_CURRENCYID,CT_SALESQUANTITYPREVMONTH,CT_FORECASTQUANTITY4MTH1,
DD_HOLDOUT,CT_FORECASTQUANTITY4MTH2,DD_FORECASTSELECTED,CT_FODFA_4,DD_UNIQUEKEY,CT_MAPE12MONTHS,CT_MAPECUSTOMERFORECAST12MONTHS,DD_LOWERMAPE,DD_CATEGORY,DD_COMMENTS,CT_FORECASTQUANTITYRANK1,
CT_FORECASTQUANTITY_CUSTOMER_RANK1,DD_REGION,DD_FORECASTAPPROACH,f.DW_UPDATE_DATE,FLAG_UPDATE,DD_FORECASTCHANGED_BYMERCK,CT_MONTHSSINCELASTCHANGE,CT_FORECASTSELECTEDLASTMONTH,CT_FORECASTQUANTITYSELECTED,
CT_SALESDELIVMOVING_NEW,
DD_FORECASTSELECTEDFINAL,DD_NEWMATERIALNUMBER,
DD_OLDATERIALNUMBER,dd_confirmed,DD_FORECASTSELECTED_ORIG,DD_CURRENCYCODE,CT_EXCHANGERATE_PL
from fact_fosalesforecastcortex f
, dim_date d1, dim_date d2,
tmp_for_insertnullforecastforManualdate t,tmp_maxrptdate_cortex r
where TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and dd_forecasttype='Manual'
and dim_dateidforecast=d1.dim_dateid
and d1.calendarmonthid=maxcalendarmonthid -99
and d2.calendarmonthid=case when right(maxcalendarmonthid + 1,2) >12 then maxcalendarmonthid + 89 else maxcalendarmonthid+1 end
and d2.plantcode_factory='Not Set' and d2.companycode='Not Set'
group by DIM_PARTID,DIM_PLANTID,DD_COMPANYCODE,DIM_DATEIDREPORTING,DD_FORECASTTYPE,DD_FORECASTSAMPLE,
CT_FORECASTQUANTITY,CT_MAPE_OLD,DD_FORECASTRANK,DD_FORECASTMODE,DIM_PROJECTSOURCEID,DD_LATESTREPORTING,CT_FORECASTQUANTITY_CUSTOMER,
DD_FORECASTGRAINATTRONE,DD_FORECASTGRAINATTRTWO,DD_FORECASTGRAINATTRTHREE,DD_FORECASTGRAINATTRFOUR,DD_FORECASTGRAINATTRFIVE,
DD_FORECASTGRAIN,CT_SALESQUANTITY_NEW,CT_SALESQUANTITY,AMT_STDCOSTPERUNIT,AMT_SELLINGPRICEPERUNIT,DIM_SALESUOMID,DD_LASTDATE,DD_HOLDOUTDATE,
f.DD_REPORTINGDATE,CT_LOWPI,CT_HIGHPI,CT_SALES_PREVMONTH,CT_SALES_PREVQTR,CT_SALES_PREVYR,CT_FORECAST_PREVMONTH,CT_FORECAST_PREVQTR,CT_FORECAST_PREVYR,
CT_SALES_CURRENTQTR,CT_FORECAST_CURRENTQTR,CT_SALES_PREVQTR_PREVYEAR,CT_FORECAST_PREVQTR_PREVYEAR,DIM_PART_PRESCRIPTIVEID,
DIM_PLANT_PRESCRIPTIVEID,STD_EXCHANGERATE_DATEID,DD_LASTREFRESHDATE,DD_PARTNUMBER,DD_PLANTCODE,CT_BIAS_ERROR,CT_BIAS_ERROR_RANK,CT_MAPE_CUSTOMERFCST,
CT_SALESCONTRIBGLOBAL,CT_RATIO_MAPE,DD_SALESCONTRIBUTION_QUARTILE_RANK,AMT_SHIPPEDQTYYTD,AMT_EXCHANGERATE_GBL,AMT_EXCHANGERATE,CT_DELIVEREDQTY_YTD,DD_SALES_COCD,DD_REPORTING_COMPANY_CODE,
DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE/*,DD_MARKET_GROUPING*/,CT_NASP,CT_NASP_CY,CT_NASP_GLOBAL,CT_NASP_PY,CT_NASP_FC_CY,CT_NASP_FC_NY,DD_MARKET_COUNTRY,CT_COEFFOFVARIATION,
DD_SALESCONTRIBUTION_3LEVELS_RANK,CT_VOLATILITY_YOY_PERCENT,DD_VOLATILITY_YOY_GT50PC,DD_FORECASTQUANTITY,CT_SALESDELIV2MTHSIRU,CT_SALESDELIV2MTHSNASP,DD_DYNAMICCONTRIBUTIONRANK,
DD_DYNAMICCONTRIBUTION,DD_HOME_VS_EXPORT,CT_COEFFOFVARIATION_FORECAST,DD_FORECASTPRODUCTCATEGORY,DD_HEICODE_NEW,DD_FLAG_SALESQUANTITY_EDITABLE,DD_PLANTTITLEMERCK,DD_PLANTCODEMERCK,
DD_REPORTAVAILABLE,CT_SALESDELIVEREDPY,CT_SALESDELIVEREDMONTH,DD_FORECASTRANK2,DD_INSERTSOURCE,CT_SALESDELIVACTUALYTD,CT_FORECAST4MTH1,CT_FORECAST4MTH2,CT_SALESMONTH1,CT_SALESMONTH2,
DD_FORECAST3MTHISNULL,DD_FORECAST4MTHIS1NULL,DD_FORECAST4MTHIS2NULL,DD_FORECAST4MTHISNULL,DD_FORECAST4MTHISNULL_SUBPRODLEV,DD_FORECAST6MTHISNULL,DD_FORECAST9MTHISNULL,CT_SALESDELIVMOVING,
DD_FOMAPE,DD_CURRENTMAPE,CT_SALESDELIVMOVINGUSD,CT_MAPE,CT_SALESDELIVACTUALYTD_TESTGH,CT_SALESDELIVMOVINGUSD_TESTGH,DIM_CURRENCYID,CT_SALESQUANTITYPREVMONTH,CT_FORECASTQUANTITY4MTH1,
DD_HOLDOUT,CT_FORECASTQUANTITY4MTH2,DD_FORECASTSELECTED,CT_FODFA_4,DD_UNIQUEKEY,CT_MAPE12MONTHS,CT_MAPECUSTOMERFORECAST12MONTHS,DD_LOWERMAPE,DD_CATEGORY,DD_COMMENTS,CT_FORECASTQUANTITYRANK1,
CT_FORECASTQUANTITY_CUSTOMER_RANK1,DD_REGION,DD_FORECASTAPPROACH,f.DW_UPDATE_DATE,FLAG_UPDATE,DD_FORECASTCHANGED_BYMERCK,CT_MONTHSSINCELASTCHANGE,CT_FORECASTSELECTEDLASTMONTH,CT_FORECASTQUANTITYSELECTED,
CT_SALESDELIVMOVING_NEW,
DD_FORECASTSELECTEDFINAL,DD_NEWMATERIALNUMBER,
DD_OLDATERIALNUMBER,dd_confirmed,DD_FORECASTSELECTED_ORIG,DD_CURRENCYCODE,CT_EXCHANGERATE_PL
) t;
/* This logic was implemented to bring the quantity for current reporting month but from next year and transpose it to current forecast month but for current year +2 for forecast quantity customer, as from PMRA we have it only until previous month*/
/* Example if reporting Month is 201806 then we are taking the vaule from 201906 and update it also for 202006 as for 2020 we have it populated until 202005 from PMRA*/
/*changed now we copy month 24 over month 36*/
drop table if exists  tmp_for_24automation;
create table tmp_for_24automation as
select distinct ct_forecastquantity_customer,dd_forecastdate,dd_partnumber,dd_plantcode,dd_reporting_company_code,dd_country_destination_code, dd_forecastrank,f.dd_reportingdate,
concat(year(to_date(f.dd_reportingdate,'DD MON YYYY'))+3,case when length(month(to_date(f.dd_reportingdate,'DD MON YYYY')))=1 then concat('0',month(to_date(f.dd_reportingdate,'DD MON YYYY'))) else month(to_date(f.dd_reportingdate,'DD MON YYYY')) end) as next2yearsforecast
 from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r
where TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and concat(year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')), case when length(month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')))=1 then concat('0',month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))) else month(to_date(to_char(dd_forecastdate),'YYYYMMDD')) end) =concat(year(to_date(f.dd_reportingdate,'DD MON YYYY'))+2,case when length(month(to_date(f.dd_reportingdate,'DD MON YYYY')))=1 then concat('0',month(to_date(f.dd_reportingdate,'DD MON YYYY'))) else month(to_date(f.dd_reportingdate,'DD MON YYYY')) end);


update  fact_fosalesforecastcortex f
set f.ct_forecastquantity_customer=t.ct_forecastquantity_customer
from fact_fosalesforecastcortex f,tmp_for_24automation t
where f.dd_partnumber=t.dd_partnumber
and f.dd_plantcode=t.dd_plantcode
and f.dd_reporting_company_code=t.dd_reporting_company_code
and f.dd_country_destination_code=t.dd_country_destination_code
and f.dd_reportingdate=t.dd_reportingdate
and f.dd_forecastrank=t.dd_forecastrank
and concat(year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')), case when length(month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')))=1 then concat('0',month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD'))) else month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) end) =t.next2yearsforecast
and f.ct_forecastquantity_customer=0;  

/*28 Aug 2018 Georgiana changes adding new MAT measure according to APP-9991*/
/* 21 Jan 2020 Vali changed buom_quantity to buom_quantityusd */
drop table if exists tmp_movinganualtotal;
create table tmp_movinganualtotal as
select distinct t1.sales_uin,t1.sales_cocd,t1.calendarmonthid,t1.reporting_company_code,t1.country_destination_code,
sum(t2.salesquantityusd) as salesmovingtotalMAT
from
(select sales_uin, sales_cocd,d.calendarmonthid,d.calendaryear,d.calendarmonthnumber,reporting_company_code,case when sales_cocd in ('NL10','XX20') then country_destination_code else 'Not Set' end as country_destination_code,sum(buom_quantityusd) as salesquantityusd
from atlas_forecast_sales_merck_DC,dim_date d
where d.datevalue=to_date(case when sales_reporting_period is null then '00010101' else sales_reporting_period||'01' end ,'YYYYMMDD')
and d.companycode='Not Set'
and d.plantcode_factory='Not Set'
group by sales_uin, sales_cocd,d.calendarmonthid,d.calendaryear,d.calendarmonthnumber,reporting_company_code,case when sales_cocd in ('NL10','XX20') then country_destination_code else 'Not Set' end) t1,
(select sales_uin, sales_cocd,d.calendarmonthid,d.calendaryear,d.calendarmonthnumber,reporting_company_code,case when sales_cocd in ('NL10','XX20') then country_destination_code else 'Not Set' end as country_destination_code,sum(buom_quantityusd) as salesquantityusd
from atlas_forecast_sales_merck_DC,dim_date d
where d.datevalue=to_date(case when sales_reporting_period is null then '00010101' else sales_reporting_period||'01' end ,'YYYYMMDD')
and d.companycode='Not Set'
and d.plantcode_factory='Not Set'
group by sales_uin, sales_cocd,d.calendarmonthid,d.calendaryear,d.calendarmonthnumber,reporting_company_code,case when sales_cocd in ('NL10','XX20') then country_destination_code else 'Not Set' end) t2
where
t1.sales_uin=t2.sales_uin and t1.sales_cocd=t2.sales_cocd
and t1.reporting_company_code=t2.reporting_company_code
and t1.country_destination_code=t2.country_destination_code
and ((t2.calendaryear=t1.calendaryear and t2.calendarmonthnumber<=t1.calendarmonthnumber) or
( t2.calendarmonthnumber > case when t1.calendarmonthnumber = 12 then 1 else t1.calendarmonthnumber end
and t2.calendaryear = case when t1.calendarmonthnumber =12 then t1.calendaryear else t1.calendaryear-1 end))
group by t1.sales_uin,t1.sales_cocd,t1.calendarmonthid,t1.reporting_company_code,t1.country_destination_code;

merge into fact_fosalesforecastcortex f
using (select distinct fact_fosalesforecastcortexid,t.salesmovingtotalMAT
from fact_fosalesforecastcortex f, tmp_movinganualtotal t, tmp_maxrptdate_cortex r
where dd_partnumber=sales_uin
and dd_plantcode=sales_cocd
and dd_reporting_company_code=reporting_company_code
and dd_country_destination_code=country_destination_code
and year(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(f.dd_forecastdate),'YYYYMMDD')),2,0) =t.calendarmonthid
and  TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate) t
on t.fact_fosalesforecastcortexid=f.fact_fosalesforecastcortexid
when matched then update set CT_SALESDELIVMOVING_NEW=t.salesmovingtotalMAT;

/* 29 Mar 2017 Georgiana Changes adding Sales Amount YTD according to BI-4363*/
drop table if exists tmp_for_upd_salesdelivactualytd;
create table tmp_for_upd_salesdelivactualytd as
select f.dim_partid,dt.datevalue,dd_reporting_company_code AS reporting_company_code,dim_dateidreporting,dd_version,(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end) as country_destination_code_upd,
f.ct_actualsYTD_USD as col01 /* MRC-860 */
from fact_atlaspharmlogiforecast_merck f,dim_part dp,dim_Date dt
where f.dim_partid=dp.dim_partid
/*and dp.partnumber ='141332' and dp.plant='ZA20'
and dt.datevalue='2016-02-01'*/
and dim_dateidreporting=dt.dim_dateid
and  dd_version='SFA';
/*group by f.dim_partid,dt.datevalue,reporting_company_code,dim_dateidreporting,dd_version,(case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then country_destination_code else 'Not Set' end)*/


merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
sum(f.col01) as col01,
dt.calendarmonthid
from tmp_for_upd_salesdelivactualytd f, fact_fosalesforecastcortex fos,dim_date dt,dim_part dp, tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull(f.country_destination_code_upd,'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
/*and dp.partnumber ='141332' and dp.plant='ZA20'
and dd_version='SFA'
and dt.calendarmonthid like '2017%'
and dd_reportingdate='08 Feb 2017'
and dd_forecastrank='1'
and dd_forecastdate='20160229'*/
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
dt.calendarmonthid) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update set
fos.ct_salesdelivactualytd  = ifnull(t.col01,0);
/*28 Mar 2017 End of changes*/

/* 07 Sept 2018 IDP skill changes: default the Forecast Qunatity selected to  CF  */
update fact_fosalesforecastcortex fos
set fos.CT_FORECASTQUANTITYSELECTED=fos.ct_forecastquantity_customer,
    fos.CT_FORECASTQUANTITYSELECTEDFINAL=fos.ct_forecastquantity_customer
from fact_fosalesforecastcortex fos, tmp_maxrptdate_cortex r
where  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate;

/* APP-10678 IDP changes: insert missing previous month for new materials, to ones for which the forecast is starting from current month*/
/* Example for January ones, they have data from Jan 2019 until Dec 2020 and we are inserting Dec 2018 month with 0'es*/

drop table if exists tmp_for_insertpreviousmonthfornewmaterials;
create table tmp_for_insertpreviousmonthfornewmaterials as
select * from (
select distinct dd_partnumber,dd_plantcode,dd_sales_cocd, f.dd_reportingdate,dd_forecastrank,dd_forecasttype,dd_reporting_company_code,dd_country_destination_code,dd_hei_code,/*dd_market_grouping,*/dd_planttitlemerck,dd_plantcodemerck,dim_partid,dim_plantid,dim_dateidreporting,dd_holdout,dd_latestreporting, min(dd_forecastdate)  as dd_forecastdate
from fact_fosalesforecastcortex f, tmp_maxrptdate_cortex r
where  TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
and dd_forecasttype='Manual'
group by dd_partnumber,dd_plantcode,dd_sales_cocd, f.dd_reportingdate,dd_forecastrank,dd_forecasttype,dd_reporting_company_code,dd_country_destination_code,dd_hei_code,/*dd_market_grouping,*/dd_planttitlemerck,dd_plantcodemerck,dim_partid,dim_plantid,dim_dateidreporting,dd_holdout,dd_latestreporting
)
where month(to_date(to_char(dd_forecastdate),'YYYYMMDD'))=month(TO_DATE(dd_reportingdate,'DD MON YYYY'))
and year(to_date(to_char(dd_forecastdate),'YYYYMMDD'))=year(TO_DATE(dd_reportingdate,'DD MON YYYY'));

delete from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex';

insert into number_fountain
select  'fact_fosalesforecastcortex',
ifnull(max(d.fact_fosalesforecastcortexid),
	ifnull((select min(s.dim_projectsourceid * s.multiplier)
			from dim_projectsource s),0))
from fact_fosalesforecastcortex d
WHERE d.fact_fosalesforecastcortexid <> 1;

drop table if exists tmp_for_insertpreviousmonthfornewmaterials2;
create table tmp_for_insertpreviousmonthfornewmaterials2
as
select dd_partnumber,dd_plantcode,dd_sales_cocd, dd_reportingdate,dd_forecastrank,dd_forecasttype,dd_reporting_company_code,dd_country_destination_code,dd_hei_code,/*dd_market_grouping,*/dd_planttitlemerck,dd_plantcodemerck,dim_partid,dim_plantid,dim_dateidreporting, to_char(ADD_MONTHS(to_date(to_char(dd_forecastdate),'YYYYMMDD'),-1),'YYYYMMDD') as dd_forecastdate,dd_holdout,dd_latestreporting from
 tmp_for_insertpreviousmonthfornewmaterials;

insert into fact_fosalesforecastcortex
(fact_fosalesforecastcortexid,dd_insertsource,dd_partnumber,dd_plantcode,dd_sales_cocd, dd_reportingdate,dd_forecastrank,dd_forecasttype,dd_reporting_company_code,dd_country_destination_code,dd_hei_code,/*dd_market_grouping,*/dd_planttitlemerck,dd_plantcodemerck,dim_partid,dim_plantid,dim_dateidreporting,  dd_forecastdate,dd_holdout,dd_latestreporting)
select  (select ifnull(m.max_id, 0) from number_fountain m WHERE m.table_name = 'fact_fosalesforecastcortex')
+ row_number() over(order by dd_partnumber,dd_SALES_COCD,dd_REPORTING_COMPANY_CODE,dd_HEI_CODE,dd_COUNTRY_DESTINATION_CODE,/*dd_MARKET_GROUPING,*/dd_reportingdate,dd_forecastdate,dd_forecasttype) as fact_fosalesforecastcortexid,
'Prev Missing Month' as dd_insertsource,t.*
from tmp_for_insertpreviousmonthfornewmaterials2 t;

merge into fact_fosalesforecastcortex f
using (
select distinct fact_fosalesforecastcortexid, min(dim_dateid) as dim_dateid
from fact_fosalesforecastcortex f,dim_date
where

(case when f.dd_forecastdate is null then '0001-01-01' else

cast(concat(substring(f.dd_forecastdate,1,4) , '-' ,
substring(f.dd_forecastdate,5,2) , '-' ,
substring(f.dd_forecastdate,7,2) ) as date)

end)
=datevalue
and companycode='Not Set'
and (dim_dateidforecast=1 or dim_dateidforecast is null)
and dd_insertsource='Prev Missing Month'
group by fact_fosalesforecastcortexid


) t
on f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid
when matched then update set dim_dateidforecast=t.dim_dateid;

update fact_fosalesforecastcortex
set dd_market_grouping = 'Not Set'
where dd_market_grouping is null;

update fact_fosalesforecastcortex f
set dd_market_grouping = 'Not Set'
WHERE EXISTS ( SELECT 1 FROM tmp_maxrptdate_cortex t WHERE TO_DATE(f.dd_reportingdate,'DD MON YYYY') = t.dd_reportingdate )
and dd_forecasttype='Manual';


/*APP-11668 - delete the materials that are not in the last PMRA file*/
merge into fact_fosalesforecastcortex f using
(
	select distinct fact_fosalesforecastcortexid
		from fact_fosalesforecastcortex f
		inner join  tmp_maxrptdate_cortex r on TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
inner join dim_date d on dim_dateidreporting=dim_dateid
	where not exists ( select 1 from atlas_forecast_pra_forecasts_merck_DC a
							where a.pra_uin = f.dd_partnumber
								and a.plant_code = f.dd_plantcode
								 and a.reporting_company_code = f.dd_reporting_company_code
								and case when a.plant_code <>'NL10' then 'Not Set' else a.country_destination_code end = f.dd_country_destination_code
and substr(a.pra_reporting_period,1,4)=substr(calendarmonthid,1,4)	)
	and f.dd_forecasttype='Manual'

) d
on f.fact_fosalesforecastcortexid =  d.fact_fosalesforecastcortexid
when matched then delete;

/*APP-11708 - Madalina - Adding logic to get the previous last working day of a month*/
/*get all working days*/
drop table if exists tmp_fo_getworkingdays;
create table tmp_fo_getworkingdays as
select datevalue,monthyear, isapublicholiday,isaweekendday,plantcode_factory,companycode,calendarmonthid, row_number() over (partition by plantcode_factory,companycode,calendarmonthid order by datevalue asc) as rn
from dim_date where
isapublicholiday=0 and isaweekendday=0;

/*get the Position of Last Working Date - 1 (previous last working date from the month)*/
drop table if exists tmp_fo_getMaxWorkingDayPosition_PerMonth;
create table tmp_fo_getMaxWorkingDayPosition_PerMonth as
select month(datevalue) month_datevalue,year(datevalue) year_datevalue, plantcode_factory,companycode, max(rn) - 1 max_rn
from tmp_fo_getworkingdays
group by  month(datevalue),year(datevalue), plantcode_factory,companycode;

/*get the Last Working Date - 1 for each month*/
drop table if exists tmp_fo_getMaxWorkingDay_PerMonth;
create table tmp_fo_getMaxWorkingDay_PerMonth as
select wd.datevalue, wd.plantcode_factory,wd.companycode
from tmp_fo_getworkingdays wd
inner join tmp_fo_getMaxWorkingDayPosition_PerMonth max_wd
	on wd.plantcode_factory = max_wd.plantcode_factory
		and wd.companycode = max_wd.companycode
		and wd.rn = max_wd.max_rn
		and month(wd.datevalue)    = max_wd.month_datevalue
		and year(wd.datevalue)  = max_wd.year_datevalue;

/* 23 Nov 2018 Georgiana Changes calculating the months between last change date and current date whenever we have a new run according to Bug found on APP-10678*/
/*25 Feb 2019 Calculating months since last change only for materials that had a P model selected for CF it should remain 99*/
/*APP-11708 - Change logic for Months since last change measure*/
drop table if exists fact_fosalesforecastcortexformonthssincelastchange;
create table fact_fosalesforecastcortexformonthssincelastchange as
select distinct fosf.dd_partnumber,fosf.dd_plantcode, wd.companycode, max(changed_date) as changed_date
from fact_fosalesforecastcortex fosf
inner join dim_autidtrailforpredictiveforecastcortex atfp on atfp.dd_partnumber=fosf.dd_partnumber
													and atfp.dd_plantcode=fosf.dd_plantcode
inner join tmp_fo_getMaxWorkingDay_PerMonth wd
			on year(changed_date) = year(wd.datevalue)
				and month(changed_date) = month(wd.datevalue)
			   and fosf.dd_plantcode = wd.plantcode_factory
where dd_holdout=10
and (forecast_rank <>'CF' OR audit_message LIKE 'Comments:%')
and changed_date not between wd.datevalue and trunc( changed_date + interval '1' MONTH, 'MM')  - 1 
group by fosf.dd_partnumber,fosf.dd_plantcode, wd.companycode;

merge into fact_fosalesforecastcortex fosf
using ( select distinct fosf.fact_fosalesforecastcortexid, months_between(trunc(current_date,'MM'),trunc(changed_date,'MM'))  as ct_monthssincelastchange
from fact_fosalesforecastcortexformonthssincelastchange f, fact_fosalesforecastcortex fosf, tmp_maxrptdate_cortex r
where dd_holdout=10
and TO_DATE(fosf.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate /*and atfp.last_entry='Y'*/
and fosf.dd_partnumber=f.dd_partnumber and fosf.dd_plantcode=f.dd_plantcode ) t
on t.fact_fosalesforecastcortexid=fosf.fact_fosalesforecastcortexid
when matched then update set fosf.ct_monthssincelastchange=t.ct_monthssincelastchange;

/* New Logic for months since last predictive*/
drop table if exists fact_fosalesforecastcortexformonthssincelastpredictive;
create table fact_fosalesforecastcortexformonthssincelastpredictive as
select distinct fosf.dd_partnumber,fosf.dd_plantcode,wd.companycode, max(changed_date) as changed_date
from fact_fosalesforecastcortex fosf
inner join dim_autidtrailforpredictiveforecastcortex atfp on atfp.dd_partnumber=fosf.dd_partnumber
													and atfp.dd_plantcode=fosf.dd_plantcode
inner join tmp_fo_getMaxWorkingDay_PerMonth wd
			on year(changed_date) = year(wd.datevalue)
				and month(changed_date) = month(wd.datevalue)
			   and fosf.dd_plantcode = wd.plantcode_factory
where dd_holdout=10
and forecast_rank NOT IN ('CF','1')
and changed_date not between wd.datevalue and trunc( changed_date + interval '1' MONTH, 'MM')  - 1
group by fosf.dd_partnumber,fosf.dd_plantcode, wd.companycode;

merge into fact_fosalesforecastcortex fosf
using ( select distinct fosf.fact_fosalesforecastcortexid, months_between(trunc(current_date,'MM'),trunc(changed_date,'MM'))  as CT_MONTHSSINCELASTPREDICTIVE
from fact_fosalesforecastcortexformonthssincelastpredictive f, fact_fosalesforecastcortex fosf, tmp_maxrptdate_cortex r
where dd_holdout=10
and TO_DATE(fosf.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate /*and atfp.last_entry='Y'*/
and fosf.dd_partnumber=f.dd_partnumber and fosf.dd_plantcode=f.dd_plantcode ) t
on t.fact_fosalesforecastcortexid=fosf.fact_fosalesforecastcortexid
when matched then update set fosf.CT_MONTHSSINCELASTPREDICTIVE=t.CT_MONTHSSINCELASTPREDICTIVE;

/* App- 10710 Georgiana Changes adding currency details based on csv file from Merck*/
/* add period */
merge into fact_fosalesforecastcortex f
using(
select distinct f.fact_fosalesforecastcortexid,currency_code,actual_exchange_rate_pl from fact_fosalesforecastcortex f,tmp_maxrptdate_cortex r, dim_date d,
(select distinct plant_code, currency_code, PERIOD, first_value(actual_exchange_rate_pl) over(partition by plant_code, currency_code, PERIOD order by period desc) as actual_exchange_rate_pl from PMRA_LOCAL_CURRENCY_RATES
where actual_exchange_rate_pl<>0) t1
where  DD_PLANTCODE=plant_code
and d.datevalue = to_date( to_char(f.DD_FORECASTDATE), 'yyyymmdd')
and period = d.calendarmonthid
and TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate) t
on f.fact_fosalesforecastcortexid = t.fact_fosalesforecastcortexid
when matched then update set dd_currencycode =currency_code,
ct_exchangerate_pl = actual_exchange_rate_pl;

/* Add logic for future ct_exchangerate_pl */  
merge into fact_fosalesforecastcortex ff
using (
select distinct f.fact_fosalesforecastcortexid, currency_code, actual_exchange_rate_pl, dd_forecastdate, dd_plantcode
from fact_fosalesforecastcortex f
join  tmp_maxrptdate_cortex r
  on TO_DATE(f.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
join  dim_date d
 on  d.datevalue = to_date(to_char(f.DD_FORECASTDATE), 'yyyymmdd')
join  ( select t.*
	    from (  select distinct plant_code, currency_code, PERIOD,  actual_exchange_rate_pl , row_number() over(partition by plant_code, currency_code order by period desc)  rk
				from PMRA_LOCAL_CURRENCY_RATES
				where actual_exchange_rate_pl<>0 ) t
		where t.rk = 1
		)  
on  DD_PLANTCODE = PLANT_CODE 
where  d.CALENDARMONTHID > PERIOD ) f
on ff.fact_fosalesforecastcortexid = f.fact_fosalesforecastcortexid
when matched then 
update 
   set ff.dd_currencycode = f.currency_code,
	   ff.ct_exchangerate_pl = f.actual_exchange_rate_pl;
/*end logic for ct_exchangerate_pl*/

/* Add logic for future CT_NASP_PRAUSED */
create or replace table tmp_fact_fosalesforecastcortex_nasp_pra as
  select
	f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code,
	dd.datevalue,
	f.CT_NASP_PRAUSED
from
	fact_fosalesforecastcortex f,
	dim_date dd
where
	dd.dim_dateid = f.dim_dateidforecast
	AND f.DD_LATESTREPORTING = 'Yes';

CREATE OR REPLACE TABLE tmp_fact_fosalesforecastcortex_nasp_max_date as
  SELECT
	f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code,
	max(f.DATEVALUE) max_date
  FROM
	tmp_fact_fosalesforecastcortex_nasp_pra f
  WHERE f.CT_NASP_PRAUSED > 0
  GROUP BY f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code;

CREATE OR REPLACE TABLE tmp_fact_fosalesforecastcortex_nasp_max_value as
SELECT
	f.dd_partnumber,
	f.dd_plantcode,
	f.dd_reportingdate,
	f.dd_forecastrank,
	f.dd_reporting_company_code,
	f.dd_country_destination_code,
	fm.max_date,
	f.CT_NASP_PRAUSED
FROM
	tmp_fact_fosalesforecastcortex_nasp_pra f,
	tmp_fact_fosalesforecastcortex_nasp_max_date fm
WHERE fm.max_date = f.datevalue
  AND fm.dd_partnumber = f.dd_partnumber
  AND fm.dd_plantcode = f.dd_plantcode
  AND fm.dd_reportingdate = f.dd_reportingdate
  AND fm.dd_forecastrank = f.dd_forecastrank
  AND fm.dd_reporting_company_code = f.dd_reporting_company_code
  AND fm.dd_country_destination_code = f.dd_country_destination_code;
        
UPDATE tmp_fact_fosalesforecastcortex_nasp_pra f
   SET f.CT_NASP_PRAUSED = fv.CT_NASP_PRAUSED
  FROM tmp_fact_fosalesforecastcortex_nasp_pra f,
  	   tmp_fact_fosalesforecastcortex_nasp_max_value fv
 WHERE f.dd_partnumber = fv.dd_partnumber
   AND f.dd_plantcode = fv.dd_plantcode
   AND f.dd_reportingdate = fv.dd_reportingdate
   AND f.dd_forecastrank = fv.dd_forecastrank
   AND f.dd_reporting_company_code = fv.dd_reporting_company_code
   AND f.dd_country_destination_code = fv.dd_country_destination_code
   AND f.DATEVALUE > fv.max_date
   AND f.CT_NASP_PRAUSED = 0;

UPDATE fact_fosalesforecastcortex f
   SET f.CT_NASP_PRAUSED = fv.CT_NASP_PRAUSED
  FROM fact_fosalesforecastcortex f,
  	   tmp_fact_fosalesforecastcortex_nasp_pra fv
 WHERE f.dd_partnumber = fv.dd_partnumber
	   AND f.dd_plantcode = fv.dd_plantcode
	   AND f.dd_reportingdate = fv.dd_reportingdate
	   AND f.dd_forecastrank = fv.dd_forecastrank
	   AND f.dd_reporting_company_code = fv.dd_reporting_company_code
       AND f.dd_country_destination_code = fv.dd_country_destination_code
       AND substr(F.DD_FORECASTDATE ,1,6)= substr(fv.DD_FORECASTDATE ,1,6)
       AND f.CT_NASP_PRAUSED = 0
       AND f.CT_NASP_PRAUSED <> fv.CT_NASP_PRAUSED
       AND f.DD_LATESTREPORTING = 'Yes';
/*end logic for CT_NASP_PRAUSED*/

/* Add logic for future CT_NASP_PRAUSEDLC */
create or replace table tmp_fact_fosalesforecastcortex_nasp_pra as
  select
    f.dd_partnumber,
    f.dd_plantcode,
    f.dd_reportingdate,
    f.dd_forecastdate,
    f.dd_forecastrank,
    f.dd_reporting_company_code,
    f.dd_country_destination_code,
    dd.datevalue,
    f.CT_NASP_PRAUSEDLC
from
    fact_fosalesforecastcortex f,
    dim_date dd
where
    dd.dim_dateid = f.dim_dateidforecast
    AND f.DD_LATESTREPORTING = 'Yes';

CREATE OR REPLACE TABLE tmp_fact_fosalesforecastcortex_nasp_max_date as
  SELECT
    f.dd_partnumber,
    f.dd_plantcode,
    f.dd_reportingdate,
    f.dd_forecastrank,
    f.dd_reporting_company_code,
    f.dd_country_destination_code,
    max(f.DATEVALUE) max_date
  FROM
    tmp_fact_fosalesforecastcortex_nasp_pra f
  WHERE f.CT_NASP_PRAUSEDLC > 0
  GROUP BY f.dd_partnumber,
    f.dd_plantcode,
    f.dd_reportingdate,
    f.dd_forecastrank,
    f.dd_reporting_company_code,
    f.dd_country_destination_code;

CREATE OR REPLACE TABLE tmp_fact_fosalesforecastcortex_nasp_max_value as
SELECT
    f.dd_partnumber,
    f.dd_plantcode,
    f.dd_reportingdate,
    f.dd_forecastrank,
    f.dd_reporting_company_code,
    f.dd_country_destination_code,
    fm.max_date,
    f.CT_NASP_PRAUSEDLC
FROM
    tmp_fact_fosalesforecastcortex_nasp_pra f,
    tmp_fact_fosalesforecastcortex_nasp_max_date fm
WHERE fm.max_date = f.datevalue
  AND fm.dd_partnumber = f.dd_partnumber
  AND fm.dd_plantcode = f.dd_plantcode
  AND fm.dd_reportingdate = f.dd_reportingdate
  AND fm.dd_forecastrank = f.dd_forecastrank
  AND fm.dd_reporting_company_code = f.dd_reporting_company_code
  AND fm.dd_country_destination_code = f.dd_country_destination_code;
        
UPDATE tmp_fact_fosalesforecastcortex_nasp_pra f
   SET f.CT_NASP_PRAUSEDLC = fv.CT_NASP_PRAUSEDLC
  FROM tmp_fact_fosalesforecastcortex_nasp_pra f,
       tmp_fact_fosalesforecastcortex_nasp_max_value fv
 WHERE f.dd_partnumber = fv.dd_partnumber
   AND f.dd_plantcode = fv.dd_plantcode
   AND f.dd_reportingdate = fv.dd_reportingdate
   AND f.dd_forecastrank = fv.dd_forecastrank
   AND f.dd_reporting_company_code = fv.dd_reporting_company_code
   AND f.dd_country_destination_code = fv.dd_country_destination_code
   AND f.DATEVALUE > fv.max_date
   AND f.CT_NASP_PRAUSEDLC = 0;

UPDATE fact_fosalesforecastcortex f
   SET f.CT_NASP_PRAUSEDLC = fv.CT_NASP_PRAUSEDLC
  FROM fact_fosalesforecastcortex f,
       tmp_fact_fosalesforecastcortex_nasp_pra fv
 WHERE f.dd_partnumber = fv.dd_partnumber
       AND f.dd_plantcode = fv.dd_plantcode
       AND f.dd_reportingdate = fv.dd_reportingdate
       AND f.dd_forecastrank = fv.dd_forecastrank
       AND f.dd_reporting_company_code = fv.dd_reporting_company_code
       AND f.dd_country_destination_code = fv.dd_country_destination_code
       AND substr(F.DD_FORECASTDATE ,1,6)= substr(fv.DD_FORECASTDATE ,1,6)
       AND f.CT_NASP_PRAUSEDLC = 0
       AND f.CT_NASP_PRAUSEDLC <> fv.CT_NASP_PRAUSEDLC
       AND f.DD_LATESTREPORTING = 'Yes';
/*end logic for CT_NASP_PRAUSEDLC*/

UPDATE fact_fosalesforecastcortex f
  SET f.ct_nasp_pra = f.CT_NASP_PRAUSED
  WHERE DD_LATESTREPORTING = 'Yes';

 UPDATE fact_fosalesforecastcortex f
  SET f.CT_NASP_PRA_ADJUSTMENT = IFNULL(f.CT_NASP_PRAUSEDLC,0)  /*f.CT_NASP_PRAUSED * ifnull(f.CT_EXCHANGERATE_PL,1)*/
  WHERE DD_LATESTREPORTING = 'Yes'
  AND DD_REGIONFORDFA <> 'Asia Pacific';


 UPDATE fact_fosalesforecastcortex f
   SET f.CT_NASP_PRAUSED = f.CT_NASP_PRAUSED * ifnull(f.CT_EXCHANGERATE_PL,1)
  WHERE DD_LATESTREPORTING = 'Yes';

/*Vali 05 August 2020 Added logic for transformed sales Manual */
 create or replace table fact_fosalesforecastcortex_prevmonth_sales 
 as
 select dd_partnumber, dd_plantcode, dd_reportingdate, dd_forecastdate, dd_forecastrank,
        dd_reporting_company_code, dd_country_destination_code, dd_event_category, ct_salesquantity, ct_transformedsales
 from fact_fosalesforecastcortex f
 where trunc(TO_DATE(f.dd_reportingdate,'DD MON YYYY'),'mm') = ( select max(trunc( TO_DATE(ff_prev.dd_reportingdate,'DD MON YYYY'), 'mm'))
																 from fact_fosalesforecastcortex ff_prev
																 where TO_DATE(ff_prev.dd_reportingdate,'DD MON YYYY') < (select dd_reportingdate from tmp_maxrptdate_cortex))
 AND DD_FORECASTRANK = 1;  

UPDATE fact_fosalesforecastcortex f
   SET f.ct_transformedsales = nvl(fv.ct_transformedsales,fv.ct_salesquantity),
       F.dd_event_category 	 = fv.dd_event_category
  FROM fact_fosalesforecastcortex f,
  	   fact_fosalesforecastcortex_prevmonth_sales fv
 WHERE f.dd_partnumber = fv.dd_partnumber
	   AND f.dd_plantcode = fv.dd_plantcode 
	   AND f.dd_forecastrank = fv.dd_forecastrank
	   AND f.dd_reporting_company_code = fv.dd_reporting_company_code
       AND f.dd_country_destination_code = fv.dd_country_destination_code
       AND substr(F.DD_FORECASTDATE ,1,6)= substr(fv.DD_FORECASTDATE ,1,6)
       AND f.DD_LATESTREPORTING = 'Yes'
       AND F.DD_FORECASTTYPE = 'Manual'; 
      
UPDATE fact_fosalesforecastcortex f
   SET f.ct_transformedsales =  f.ct_salesquantity
 where f.DD_LATESTREPORTING = 'Yes'
   AND F.DD_FORECASTTYPE = 'Manual'
   and  nvl(f.ct_transformedsales,0)= 0 
   and nvl(f.ct_salesquantity,0) <> nvl(f.ct_transformedsales,0)
   and DD_FORECASTRANK = 1;

/*Georgiana 31 May 2019 - Performance Improvement for fact_fosalesforecastcortex
deleting runs that are older than 4 months and keeping them in the historical table
please leave this statement the last one and it's IMPORTANT that every column that is added in fact_fosalesforecastcortex
to be also added in fact_fosalesforecastcortex_history table in the same order in order to have the statements without error*/
/* we are always inserting in the historical table the previous run except of the current one in order to be sure that we have all the edit's
that are made in IDP in the historical table*/


insert into fact_fosalesforecastcortex_history
select f.* from fact_fosalesforecastcortex f, tmp_maxrptdate_cortex r
where year(TO_DATE(f.dd_reportingdate,'DD MON YYYY')) = year(add_months(r.dd_reportingdate,-1))
and month(TO_DATE(f.dd_reportingdate,'DD MON YYYY'))= month(add_months(r.dd_reportingdate,-1));

drop table if exists tmp_forfindingthe4monthsagoreportingdate;
create table tmp_forfindingthe4monthsagoreportingdate as
select distinct add_months(TO_DATE(dd_reportingdate,'DD MON YYYY'),-4) as dd_reportingdate from fact_fosalesforecastcortex f
where dd_latestreporting='Yes'
and dd_holdout='10';

merge into fact_fosalesforecastcortex f
using (
select distinct fact_fosalesforecastcortexid from fact_fosalesforecastcortex f1 inner join tmp_forfindingthe4monthsagoreportingdate f2
on
year(TO_DATE(f1.dd_reportingdate,'DD MON YYYY'))=year(f2.dd_reportingdate)
and month(TO_DATE(f1.dd_reportingdate,'DD MON YYYY')) =month(f2.dd_reportingdate)) t
on f.fact_fosalesforecastcortexid=t.fact_fosalesforecastcortexid
when matched then delete;

 UPDATE fact_fosalesforecastcortex f
   SET DD_PLANTTITLEMERCK = dp.PLANTTITLE_MERCK
  FROM  fact_fosalesforecastcortex f, dim_plant dp
 WHERE f.DD_PLANTCODE = dp.PLANTCODE;
 
 update fact_fosalesforecastcortex a
set a.DD_REGIONFORDFA = b.REGIONFORDFA_REGION
FROM fact_fosalesforecastcortex a, TMP_REGIONFORDFA b
where b.REGIONFORDFA_PLANT = a.DD_PLANTTITLEMERCK
and IFNULL(a.DD_REGIONFORDFA,'Not Set') <> IFNULL(b.REGIONFORDFA_REGION,'Not Set');
 

merge into fact_fosalesforecastcortex fos
using (
select distinct fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,
sum(f.ct_Salesrfbudget) as ct_Salesrfbudget,
sum(f.ct_Salesrfbudgetusd) as ct_Salesrfbudgetusd,
sum(f.ct_Salesrfbudgetlc) as ct_Salesrfbudgetlc,
sum(f.ct_salesbudgetlc)   as ct_salesbudgetlc,
dt.calendarmonthid
from fact_atlaspharmlogiforecast_merck f,fact_fosalesforecastcortex fos,dim_date dt,dim_part dp,tmp_maxrptdate_cortex r
where f.dim_partid = dp.dim_partid
and f.dim_partid=fos.dim_partid
AND f.dim_dateidreporting = dt.dim_dateid
AND year(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')) || lpad( month(to_date(to_char(fos.dd_forecastdate),'YYYYMMDD')),2,0) =dt.calendarmonthid
AND ifnull((case when (f.dd_version = 'SFA' and f.dim_plantid in (91,124)) or f.dd_version = 'DTA' then f.dd_country_destination_code else 'Not Set' end),'Not Set') = ifnull(fos.dd_COUNTRY_DESTINATION_CODE,'Not Set')
and ifnull(f.dd_reporting_company_code,'Not Set')= ifnull(fos.dd_REPORTING_COMPANY_CODE,'Not Set')
and  TO_DATE(fos.dd_reportingdate,'DD MON YYYY') = r.dd_reportingdate
group by fact_fosalesforecastcortexid,dd_forecasttype,dp.partnumber,dp.plant,dd_forecastrank,fos.dd_reportingdate,dd_forecastdate,dt.calendarmonthid
) t
on t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then
update set fos.CT_LATESTRF_BUDGET = t.ct_Salesrfbudget,
fos.CT_LATESTRF_BUDGETUSD = t.ct_Salesrfbudgetusd,
fos.CT_LATESTRF_BUDGETLC = t.ct_Salesrfbudgetlc,
fos.CT_LATEST_BUDGETLC = t.ct_salesbudgetlc;

/********* MRC- 414 - Vali 11-Nov-2019 *******/
delete from fact_SALESHISTORY_cortex where dd_reportingdate in (select dd_reportingdate from tmp_maxrptdate_cortex);
insert into fact_SALESHISTORY_cortex
(
fact_SALESHISTORY_cortexid  ,
DD_PARTNUMBER     ,
DD_PLANT,
DD_COMPANY,
DD_CALENDARMONTHID,
CT_SALESQUANTITY,
dd_reportingdate,
CT_TRANSFORMEDSALES,
dd_flag
)
select  (
select
ifnull(max(fact_SALESHISTORY_cortexid ), 0) from fact_SALESHISTORY_cortex m)+ row_number() over(order by '') as fact_SALESHISTORY_cortexID,
DD_PARTNUMBER     ,
COMOP         ,
COMPANYCODE       ,
YYYYMM ,
SALES_ORIGINAL,
dd_reportingdate,
SALES as CT_TRANSFORMEDSALES,
flag
from saleshistory_fromprd_dfsubjarea_3MHO, tmp_maxrptdate_cortex;

/** populate new fact to keep changes made in skillentry as fact_SALESHISTORY_cortex is truncated by DS **/
delete from fact_SALESHISTORY_Transformedsales_cortex where dd_reportingdate in (select dd_reportingdate from tmp_maxrptdate_cortex);
insert into fact_SALESHISTORY_Transformedsales_cortex
(
fact_SALESHISTORY_cortexid  ,
DD_PARTNUMBER     ,
DD_PLANT,
DD_COMPANY,
DD_CALENDARMONTHID,
CT_SALESQUANTITY,
dd_reportingdate,
CT_TRANSFORMEDSALES,
dd_flag,
dd_event_category 
)
select  (
select
ifnull(max(fact_SALESHISTORY_cortexid ), 0) from fact_SALESHISTORY_Transformedsales_cortex m)+ row_number() over(order by '') as fact_SALESHISTORY_cortexID,
DD_PARTNUMBER     ,
COMOP         ,
COMPANYCODE       ,
YYYYMM ,
SALES_ORIGINAL,
dd_reportingdate,
SALES as CT_TRANSFORMEDSALES,
flag,
dd_event_category
from saleshistory_fromprd_dfsubjarea_3MHO, tmp_maxrptdate_cortex;

 MERGE INTO fact_fosalesforecastcortex fos
USING ( SELECT f.FACT_FOSALESFORECASTCORTEXID, sum(s.BUOM_QUANTITY) BUOM_QUANTITY
 FROM  FACT_FOSALESFORECASTCORTEX F, ATLAS_FORECAST_SALES_MERCK_DC s
 WHERE f.DD_PARTNUMBER = s.SALES_UIN
   AND f.DD_PLANTCODE = s.SALES_COCD
   AND f.DD_REPORTING_COMPANY_CODE = s.REPORTING_COMPANY_CODE
   AND f.dd_COUNTRY_DESTINATION_CODE=case WHEN s.SALES_COCD <>'NL10' then 'Not Set' else s.COUNTRY_DESTINATION_CODE end
   AND TO_NUMBER(substr(f.DD_FORECASTDATE,1,6)) = s.SALES_REPORTING_PERIOD
   AND f.DD_LATESTREPORTING = 'Yes'
GROUP by f.FACT_FOSALESFORECASTCORTEXID) t
ON t.fact_fosalesforecastcortexid=fos.fact_fosalesforecastcortexid
when matched then update SET fos.CT_SALESQUANTITY_NET = t.BUOM_QUANTITY;

 /** added dd_monthslastreview - months since the material was last confirmed **/
 MERGE INTO fact_fosalesforecastcortex f
 USING ( SELECT DISTINCT max(drep.datevalue) DD_LASTCONFIRMEDDATE,
                 round(months_between(to_date(fosf.dd_reportingdate, 'dd Mon yyyy'),   max(drep.datevalue) )) dd_monthslastreview,
		         fosf.DD_PARTNUMBER,
		         fosf.DD_PLANTCODE,
		         fosf.DD_PLANTCODEMERCK,
		         fosf.DD_COUNTRY_DESTINATION_CODE
		FROM fact_fosalesforecastcortex fosf
		INNER JOIN Dim_Part AS fosfpa ON fosf.dim_partid = fosfpa.Dim_Partid  AND ((fosfpa.PartTypeDescription) IN (('Finished products'),('Trading goods')) )
		AND ((fosfpa.PlantMaterialStatus) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('TA')) )  AND ((fosfpa.PartNumber_NoLeadZero) NOT IN (('163333'),('189746'),('179195'),('194452')) )
		AND (LOWER(fosfpa.deletionflag) = LOWER('Not Set'))  AND ((fosfpa.crossmatplantsts) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('00'),('TA')) )
		INNER JOIN dim_plant pl ON pl.plantcode = fosf.dd_plantcode
		INNER JOIN dim_company dc ON dc.companycode = pl.companycode
		INNER JOIN Dim_Date as dd ON dd.dim_dateid = dim_dateidforecast /*AND dd.plantcode_factory = f.dd_plantcode*/ AND dc.companycode = dd.companycode
		LEFT join fact_fosalesforecastcortex_audittrail  aud
		on aud.dim_partid= fosf.dim_partid
		and aud.dim_plantid= fosf.dim_plantid
		AND  aud.action_taken = 'Confirmed'
		LEFT JOIN dim_date drep
		ON aud.DIM_DATEIDREPORTING = drep.dim_dateid
		WHERE fosf.dd_latestreporting='Yes'
		and fosf.dd_holdout='10'
		GROUP BY  fosf.DD_PARTNUMBER,
		          fosf.DD_PLANTCODE,
		          fosf.DD_PLANTCODEMERCK,
		          fosf.DD_COUNTRY_DESTINATION_CODE,
		          fosf.dd_reportingdate
		          ) x
 ON  f.DD_PARTNUMBER = x.DD_PARTNUMBER
 AND f.DD_PLANTCODE = x.DD_PLANTCODE
 AND f.DD_PLANTCODEMERCK = x.DD_PLANTCODEMERCK
 AND f.DD_COUNTRY_DESTINATION_CODE = x.DD_COUNTRY_DESTINATION_CODE
WHEN MATCHED THEN
   UPDATE set f.dd_monthslastreview = x.dd_monthslastreview
    WHERE f.dd_latestreporting='Yes'
	  AND f.dd_holdout='10'
	  AND nvl(f.dd_monthslastreview,-1) <> nvl(x.dd_monthslastreview,-1);


/* Temp Table for DFA Main Error Driver*/
/* MRC-681 Vali 21 Jan changed ct_nasp with ct_nasp_pra */
CREATE OR REPLACE TABLE TMP_DFA_MAIN_ERRORDRIVER AS
select      p.partnumber,
			l1.DominantSpeciesDescription_Merck,
			l1.ProductFamily_Merck  ,
            l1.prodfamilydescription_merck,
            l1.dd_planttitlemerck,
            /*l1.sub_product_family_code_pma,*/
            /*l1.DD_REGION,*/
			l1.dd_plantcode,
			dd_country_destination_code,
            l1.MonthYear 		       AS DD_REPORTINGDATE,
		    /********************** DFA MAIN ERROR DRIVER ***************************************/
		    max(CASE WHEN ((100.00-100 * cast(SUM_L3 / SUM_TOTAL as decimal (30,15)))) = 0  THEN 0

		       ELSE round ((
		        100.00000000 * cast(
				( case
					when (
					CASE
						when (SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 = 0
						and SUM_ct_salesmonth1 + SUM_ct_salesmonth2 = 0) then (100.00000000)
						when (SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 = 0) then (0.00000000)
						when (1-cast(abs(SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 -SUM_ct_salesmonth1-SUM_ct_salesmonth2)/
						(case
							when SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1 = 0 then 1.00000000
							else SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1
						end) as decimal (30,15)
						)) >1 then (0.00000000)
						when (1-cast(abs(SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 -SUM_ct_salesmonth1-SUM_ct_salesmonth2)/
									( case
										when SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1 = 0 then 1.00000000
										else SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1
									end) as decimal(30,15))) <0 then (0.00000000)
						else 100.00000000 *
						 (1.00000000- cast(abs(SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 - SUM_ct_salesmonth1 - SUM_ct_salesmonth2) /
						(
							case
							when SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1 = 0 then 1.00000000
							else SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1
						end) as decimal(30,15)  )
						)
					end) is null then 0.00000000
					else (1.00000000-(
					(case
						when (SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 = 0
						and SUM_ct_salesmonth1 + SUM_ct_salesmonth2 = 0) then (100.00000000)
						when (SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 = 0) then (0.00000000)
						when (1- cast(abs(SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 -SUM_ct_salesmonth1-SUM_ct_salesmonth2)/
								(case when SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1 = 0 then 1.00000000
									  else SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1
								end) as decimal (30,15))
								) > 1 then (0.00000000)
						when (1-cast(abs(SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 -SUM_ct_salesmonth1-SUM_ct_salesmonth2)/
						(
							case
							when SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1 = 0 then 1.00000000
							else SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1
						end) as decimal (30,15)
						)) <0 then (0.00000000)
						else 100.00000000 *(1.00000000-cast(abs(SUM_ct_forecast4mth1 + SUM_ct_forecast4mth2 -SUM_ct_salesmonth1-SUM_ct_salesmonth2) /
						(
							case
							when SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1 = 0 then 1.00000000
							else SUM_ct_forecast4mth2 + SUM_ct_forecast4mth1
						end) as decimal(30,15)
						))
					end)/ 100.00000000)) * ((100.00000000 * cast(SUM_ct_salesdeliv2mthsiru_ct_nasp / SUM_TOTAL as decimal (30,
					15)))-(100.00000000 * cast( SUM_ct_salesdeliv2mthsiru_ct_nasp2 / SUM_TOTAL as decimal (30,15))))
					                     / (100.00-100 * cast(SUM_L3 / SUM_TOTAL as decimal (30,15)))
				end) as decimal (30,15))
				), 2)
				END )  as DFA_MAIN_ERROR
		    /********************** DFA MAIN ERROR DRIVER ***************************************/
 from   ( select  case
                        when (f_iaplf.dd_version = 'SFA'
                        and f_iaplf.dim_plantid in (91,
                        124))
                        or f_iaplf.dd_version = 'DTA' then f_iaplf.dd_country_destination_code
                        else 'Not Set'
                    end   AS dd_country_destination_code,
                    repdt.MonthYear,
                    aplfpl.PlantCode,
                    aplfprt.DominantSpeciesDescription_Merck  ,
                    aplfprt.ProductFamily_Merck  ,
                    aplfprt.prodfamilydescription_merck  ,
                    aplfprt.sub_product_family_code_pma  ,
                    aplfprt.sub_product_family_pma  ,
                    f_iaplf.dd_plantcode,
                    f_iaplf.dd_planttitlemerck,
                    sum(ct_forecast4mth1)  SUM_ct_forecast4mth1,
                    sum(ct_forecast4mth2)  SUM_ct_forecast4mth2,
                    sum(ct_salesmonth1)    SUM_ct_salesmonth1,
                    sum(ct_salesmonth2)    SUM_ct_salesmonth2,
                    sum(cast(ct_salesdeliv2mthsIRU_USD as decimal (30, 15))) as SUM_ct_salesdeliv2mthsiru_ct_nasp,
                    sum(case when ifnull(dd_forecast4mthisnull, 0) = 1 then cast( ct_salesdeliv2mthsIRU_USD as decimal (30, 15)) else 0.00000000 end) SUM_ct_salesdeliv2mthsiru_ct_nasp2
                from  fact_atlaspharmlogiforecast_merck as f_iaplf
                inner join Dim_Date as repdt on
                    f_iaplf.dim_dateidreporting = repdt.Dim_Dateid
                inner join Dim_Part as aplfprt on
                    f_iaplf.dim_partid = aplfprt.Dim_Partid
                    and ((aplfprt.PartNumber_NoLeadZero) not in (('163333'),('189746'),('194452')) )
                inner join dim_date as x_varDate on
                    x_varDate.DateValue
                    = case
                        when day(current_date) >= 9 then (add_months(current_date,
                        -1))
                        else add_months(current_date,
                        -2)
                    end
                    and repdt.CompanyCode = x_varDate.CompanyCode
                    and repdt.PlantCode_factory = x_varDate.PlantCode_Factory
                    and ((repdt.CalendarMonthID) = (x_varDate.CalendarMonthID))
                inner join Dim_Plant as aplfpl on
                    f_iaplf.dim_plantid = aplfpl.Dim_Plantid
                where (lower( case when f_iaplf.dd_version = 'SFA' then 'DFA'
                                else f_iaplf.dd_version
                            end) = lower('DFA'))
                group by
                    case
                        when (f_iaplf.dd_version = 'SFA'
                        and f_iaplf.dim_plantid in (91,124))
                        or f_iaplf.dd_version = 'DTA' then f_iaplf.dd_country_destination_code
                        else 'Not Set'
                    end,
                    repdt.MonthYear,
                    repdt.CalendarMonthID,
                    aplfpl.PlantCode,
                    aplfprt.DominantSpeciesDescription_Merck,
                    aplfprt.ProductFamily_Merck,
                    aplfprt.prodfamilydescription_merck,
                    aplfprt.sub_product_family_code_pma,
                    aplfprt.sub_product_family_pma,
                    f_iaplf.dd_plantcode,
                    f_iaplf.dd_planttitlemerck
                    /*f_iaplf.DD_REGION*/   ) l1
join ( select   sum(case when ifnull(dd_forecast4mthisnull, 0) = 1 then cast( ct_salesdeliv2mthsIRU_USD as decimal (30, 15)) else 0.00000000 end) SUM_L3 ,
                repdt.MonthYear,
                f_iaplf.dd_plantcode
		from
					fact_atlaspharmlogiforecast_merck as f_iaplf
				inner join Dim_Date as repdt on
					f_iaplf.dim_dateidreporting = repdt.Dim_Dateid
				inner join Dim_Part as aplfprt on
					f_iaplf.dim_partid = aplfprt.Dim_Partid
					and ((aplfprt.PartNumber_NoLeadZero) not in (('163333'),('189746'),('194452')) )
				inner join dim_date as x_varDate on
					x_varDate.DateValue
					= case
						when day(current_date) >= 9 then (add_months(current_date,
						-1))
						else add_months(current_date,
						-2)
					end
					and repdt.CompanyCode = x_varDate.CompanyCode
					and repdt.PlantCode_factory = x_varDate.PlantCode_Factory
					and ((repdt.CalendarMonthID) = (x_varDate.CalendarMonthID))
				inner join Dim_Plant as aplfpl on
					f_iaplf.dim_plantid = aplfpl.Dim_Plantid
				where
					(lower(case
						when f_iaplf.dd_version = 'SFA' then 'DFA'
						else f_iaplf.dd_version
					end) = lower('DFA'))
				group by
					repdt.MonthYear,
					f_iaplf.dd_plantcode,
					repdt.CalendarMonthID
			HAVING sum(case when ifnull(dd_forecast4mthisnull, 0) = 1 then cast( ct_salesdeliv2mthsIRU_USD as decimal (30, 15)) else 0.00000000 end) > 0
        )  l3
      on l1.MonthYear  = l3.MonthYear
      AND   l1.dd_plantcode = l3.dd_plantcode
  join   (/******** total  for all records ***********/
		        select  sum(cast( ct_salesdeliv2mthsIRU_USD as decimal (30, 15))) as SUM_TOTAL,
		        		repdt.MonthYear,
		        		f_iaplf.dd_plantcode
			    from fact_atlaspharmlogiforecast_merck as f_iaplf
				inner join Dim_Date as repdt on
					f_iaplf.dim_dateidreporting = repdt.Dim_Dateid
				inner join Dim_Part as aplfprt on
					f_iaplf.dim_partid = aplfprt.Dim_Partid
					and ((aplfprt.PartNumber_NoLeadZero) not in (('163333'),('189746'),('194452')) )
				inner join dim_date as x_varDate on
					x_varDate.DateValue
					= case
						when day(current_date) >= 9 then (add_months(current_date,
						-1))
						else add_months(current_date,
						-2)
					end
					and repdt.CompanyCode = x_varDate.CompanyCode
					and repdt.PlantCode_factory = x_varDate.PlantCode_Factory
					and ((repdt.CalendarMonthID) = (x_varDate.CalendarMonthID))
				inner join Dim_Plant as aplfpl on
					f_iaplf.dim_plantid = aplfpl.Dim_Plantid
				where
					(lower ( case when f_iaplf.dd_version = 'SFA' then 'DFA'
							else f_iaplf.dd_version
						end) = lower('DFA'))
				group by
					repdt.MonthYear,
					repdt.CalendarMonthID,
					f_iaplf.dd_plantcode
			     HAVING  sum(cast( ct_salesdeliv2mthsIRU_USD as decimal (30, 15))) > 0
      ) tt
      ON   l1.MonthYear  = tt.MonthYear
      AND l1.dd_plantcode = tt.dd_plantcode
 JOIN dim_part p
 ON p.plant = l1.PLANTCODE
 AND p.sub_product_family_code_pma = l1.sub_product_family_code_pma
 AND p.ProductFamily_Merck = l1.ProductFamily_Merck
GROUP BY p.partnumber,
			l1.DominantSpeciesDescription_Merck,
			l1.ProductFamily_Merck  ,
            l1.prodfamilydescription_merck,
            l1.dd_planttitlemerck,
            /* l1.sub_product_family_code_pma, */
            /*l1.DD_REGION,*/
			l1.dd_plantcode,
			dd_country_destination_code,
            l1.MonthYear  ;

/*update for autoforecast values*/
UPDATE FACT_FOSALESFORECASTCORTEX F
   SET F.DD_AUTOPREDICTIVE = T.DD_AUTOPREDICTIVE
  FROM FACT_FOSALESFORECASTCORTEX F, TMP_AUTOFORECAST_ON T
 WHERE F.DD_PARTNUMBER = T.DD_PARTNUMBER
   AND F.DD_PLANTCODE = T.DD_PLANTCODE
   AND F.DD_REPORTING_COMPANY_CODE = T.DD_REPORTING_COMPANY_CODE
   AND F.DD_COUNTRY_DESTINATION_CODE = T.DD_COUNTRY_DESTINATION_CODE
   AND F.DD_LATESTREPORTING = 'Yes';

/*MRC-704-Octavian S- update for DS vs FF history comments and reasons values*/
UPDATE FACT_FOSALESFORECASTCORTEX F
   SET F.DD_COMMNENTS_DFVSFF = T.DD_COMMNENTS_DFVSFF,
       F.DD_REASON_OF_DIFFERENCE_DFVSFF = T.DD_REASON_OF_DIFFERENCE_DFVSFF
  FROM FACT_FOSALESFORECASTCORTEX F, TMP_DS_vs_FF T
 WHERE F.DD_PARTNUMBER = T.DD_PARTNUMBER
   AND F.DD_PLANTCODE = T.DD_PLANTCODE
   AND F.DD_REPORTING_COMPANY_CODE = T.DD_REPORTING_COMPANY_CODE
   AND F.DD_COUNTRY_DESTINATION_CODE = T.DD_COUNTRY_DESTINATION_CODE
   AND F.DD_LATESTREPORTING = 'Yes';

   UPDATE FACT_FOSALESFORECASTCORTEX F
   SET F.CT_SUPPLY_CONSTRAINT = T.CT_SUPPLY_CONSTRAINT,
       F.CT_NEW_PRODUCT = T.CT_NEW_PRODUCT,
       F.CT_COMMERCIAL_RISK = T.CT_COMMERCIAL_RISK,
       F.CT_FINANCIAL_ADJUSTMENT = T.CT_FINANCIAL_ADJUSTMENT
  FROM FACT_FOSALESFORECASTCORTEX F, TMP_DF_TO_FF T
 WHERE F.DD_PARTNUMBER = T.DD_PARTNUMBER
   AND F.DD_PLANTCODE = T.DD_PLANTCODE
   AND F.DD_REPORTING_COMPANY_CODE = T.DD_REPORTING_COMPANY_CODE
   AND F.DD_COUNTRY_DESTINATION_CODE = T.DD_COUNTRY_DESTINATION_CODE
   AND substr(F.DD_FORECASTDATE ,1,6)= substr(t.DD_FORECASTDATE ,1,6)
   AND F.DD_LATESTREPORTING = 'Yes';

    UPDATE FACT_FOSALESFORECASTCORTEX F
   SET F.CT_NASP_PRA_ADJUSTMENT = T.CT_NASP_PRA_ADJUSTMENT
  FROM FACT_FOSALESFORECASTCORTEX F, TMP_NASP_CHANGED T
 WHERE F.DD_PARTNUMBER = T.DD_PARTNUMBER
   AND F.DD_PLANTCODE = T.DD_PLANTCODE
   AND substr(F.DD_FORECASTDATE ,1,6)= substr(t.DD_FORECASTDATE ,1,6)
   AND F.DD_LATESTREPORTING = 'Yes'
   AND F.DD_REGIONFORDFA = 'Asia Pacific';

 MERGE INTO FACT_FOSALESFORECASTCORTEX f1
   USING (SELECT DD_PARTNUMBER,DD_PLANTCODE,DD_REPORTING_COMPANY_CODE,
           DD_REPORTINGDATE,DD_FORECASTDATE,DD_COUNTRY_DESTINATION_CODE,
           DD_FORECASTRANK,DD_FORECASTTYPE,CT_FORECASTQUANTITY,CT_FORECASTQUANTITYSELECTEDFINAL
   FROM FACT_FOSALESFORECASTCORTEX f2
  WHERE f2.DD_AUTOPREDICTIVE = 'On'
    AND f2.DD_FORECASTRANK = 1
    AND f2.DD_LATESTREPORTING = 'Yes') f2 ON
    f1.DD_PARTNUMBER = f2.DD_PARTNUMBER
    AND f1.DD_PLANTCODE = f2.DD_PLANTCODE
    AND f1.DD_REPORTING_COMPANY_CODE = f2.DD_REPORTING_COMPANY_CODE
    AND f1.DD_REPORTINGDATE = f2.DD_REPORTINGDATE
    AND f1.DD_FORECASTDATE = f2.DD_FORECASTDATE
    AND f1.DD_COUNTRY_DESTINATION_CODE = f2.DD_COUNTRY_DESTINATION_CODE
   WHEN MATCHED THEN
   UPDATE SET f1.CT_FORECASTQUANTITYSELECTED = f2.CT_FORECASTQUANTITY,
              f1.CT_FORECASTQUANTITYSELECTEDFINAL = f2.CT_FORECASTQUANTITY,
              f1.DD_FORECASTSELECTED = 1,
              f1.DD_FORECASTSELECTEDFINAL = 1
    WHERE f1.dd_latestreporting='Yes'
      AND f1.dd_holdout='10';

 UPDATE FACT_FOSALESFORECASTCORTEX 
      SET CT_FINANCIAL_FORECAST = IFNULL(CT_FORECASTQUANTITYSELECTEDFINAL,IFNULL(CT_FORECASTQUANTITY_CUSTOMER,0)) - ifnull(CT_SUPPLY_CONSTRAINT,0) - ifnull(CT_NEW_PRODUCT,0) - ifnull(CT_COMMERCIAL_RISK,0) - ifnull(CT_FINANCIAL_ADJUSTMENT,0)
    WHERE DD_LATESTREPORTING = 'Yes';

 UPDATE FACT_FOSALESFORECASTCORTEX 
      SET CT_FINANCIAL_FORECASTSELECTED = IFNULL(CT_FINANCIAL_FORECAST * CT_NASP_PRA_ADJUSTMENT,0)
    WHERE DD_LATESTREPORTING = 'Yes';

    CREATE OR REPLACE TABLE UK_INMARKET_SALES AS
SELECT
    PLANT_CODE,
    UIN,
    SALESMONTH,
    SUM(GROSS_SALES) TOTAL_GROSS_SALES,
    SUM(QUANTITY) TOTAL_QUANTITY
FROM
    STG_GDH_SALES_OUT_EMEA_UK
GROUP BY PLANT_CODE,
    UIN,
    SALESMONTH;
    
CREATE OR REPLACE TABLE BCKP_FACT_FOSALESFORCASTCORTEX_TRANSFORMEDSALES AS
SELECT dd_plantcode, DD_PARTNUMBER,DD_FORECASTDATE,CT_TRANSFORMEDSALES
FROM fact_fosalesforecastcortex
WHERE DD_LATESTREPORTING = 'Yes'
  AND DD_PLANTCODE = 'GB20';
 
 UPDATE fact_fosalesforecastcortex f
    SET f.ct_salesquantity = uk.TOTAL_QUANTITY
   FROM fact_fosalesforecastcortex f, UK_INMARKET_SALES UK
  WHERE f.DD_LATESTREPORTING = 'Yes'
    AND f.DD_PLANTCODE = UK.PLANT_CODE
    AND f.DD_PARTNUMBER = UK.UIN
    AND SUBSTR(f.DD_FORECASTDATE,1,6) = UK.SALESMONTH;
   
    UPDATE fact_fosalesforecastcortex f
    SET f.CT_TRANSFORMEDSALES = uk.ct_shippedQty
   FROM fact_fosalesforecastcortex f, uk_upd_from_old_shipping UK
  WHERE f.DD_LATESTREPORTING = 'Yes'
    AND f.DIM_PLANTID = UK.dim_plantid
    AND f.DIM_PARTID = UK.dim_partid
    AND SUBSTR(f.DD_FORECASTDATE,1,6) = UK.calendarmonthid;

update FACT_FOSALESFORECASTCORTEX
   set CT_SALESQUANTITY = null
WHERE DD_LATESTREPORTING = 'Yes'
AND DD_PLANTCODE = 'GB20'
and substr(DD_FORECASTDATE,1,6) in (202011,202012);

update FACT_FOSALESFORECASTCORTEX
   set CT_TRANSFORMEDSALES = 0
WHERE DD_LATESTREPORTING = 'Yes'
AND DD_PLANTCODE = 'GB20'
and substr(DD_FORECASTDATE,1,6) >= 202101;

/* MRC-1029 Vali 20200513 add ct_onhandqtyiru */
create or replace table tmp_inventory_cortex as
   select f.dim_partid, f.dim_plantid, ifnull(SUM(ct_onhandqtyiru),0) currentinventory
from fact_inventoryaging  f 
group by f.dim_partid, f.dim_plantid;
 
 merge into fact_fosalesforecastcortex  f
  using tmp_inventory_cortex  t
 on f.dim_partid = t.dim_Partid
 and f.dim_plantid = t.dim_plantid
 when matched then
 update set ct_onhandqtyiru = currentinventory
 where f.dd_latestreporting = 'Yes' ;

CREATE OR REPLACE table fact_idp as
SELECT
DISTINCT dd_partnumber,
fosfpa.dd_partnumber_description,
fosfpa.partdescription,
ifnull(f.dd_plantcode,'NA') as dd_plantcode,
f.dd_reportingdate,
dd_forecastdate,
dd.datevalue,
dd.calendarmonthid,
dd.plantcode_factory,
dd.CompanyCode,
dd_hei_code,
dd_reporting_company_code,
ifnull(f.dd_country_destination_code,'Not Set') as dd_country_destination_code,
dd_forecastrank,
dd_forecasttype,
dd_forecastselected,
dd_forecastselectedfinal,
f.dd_planttitlemerck,
dd_plantcodemerck,
dd_recommendation,
ifnull(dd_autopredictive,'Off') dd_autopredictive,
nvl(dd_monthslastreview, 99) dd_monthslastreview,
dd_forecast4mthisnull,
fosfpa.prodfamilydescription_merck,
fosfpa.dominantspeciesdescription_merck,
fosfpa.therapeuticalclass_merck,
fosfpa.productgroupdescription_merck,
fosfpa.productsegementdesc,
fosfpa.productfamilydescription_merck,
fosfpa.ProductFamily_Merck,
dd_latestreporting,
dd_holdout,
dd_confirmed,
dd_comments,
dd_insertsource,
dd_lowermape,
f.dd_region,
f.DD_REGIONFORDFA,
f.ct_residualscore,
f.dd_forecastability,
f.dim_partid,
f.dim_plantid,
f.dim_dateidreporting,
f.dim_dateidforecast,
f.dw_update_date,
ct_forecastquantity,
ct_forecastquantity_customer,
ct_forecastquantityselected,
ct_forecastquantityselectedfinal,
IFNULL(ct_salesquantity, 0)  ct_salesquantity,
IFNULL(ct_transformedsales, 0) as ct_transformedsales,
ct_highpi,
ct_lowpi,
ct_salesdelivmoving_new,
ct_nasp,
ct_nasp_pra,
CT_SALESDELIVEREDUSD,
ct_salesdelivactualytd,
CT_SALESDELIV2MTHSIRU,
ct_forecast4mth1,
ct_forecast4mth2,
ct_salesmonth1,
ct_salesmonth2,
ct_mape,
ct_mape_customerfcst,
ct_bias_error,
ct_monthssincelastchange,
ct_mape12months,
ct_predictivsuit,
dfa_main_error,
-999 dfa_rank,
DD_FORECASTBIASACTION,
DD_FORECASTBIASLAST3MONTHS,
DD_SEGMENTATION_LOCAL,
IFNULL(DD_EVENT_CATEGORY, 'Not Set')  DD_EVENT_CATEGORY,
DD_CONFIRMCATEGORY,
DD_CONFIRMSUBCATEGORY,
IFNULL(CT_LATESTRF_BUDGET,0) CT_LATESTRF_BUDGET,
IFNULL(CT_LATESTRF_BUDGETUSD,0) CT_LATESTRF_BUDGETUSD,
IFNULL(CT_LATESTRF_BUDGETLC,0) CT_LATESTRF_BUDGETLC,
IFNULL(CT_LATEST_BUDGETLC,0) CT_LATEST_BUDGETLC,
IFNULL(CT_DELIVEREDQTY_YTD,0)  CT_DELIVEREDQTY_YTD,
CT_MONTHSSINCELASTPREDICTIVE,
DD_COMMNENTS_DFVSFF,
DD_REASON_OF_DIFFERENCE_DFVSFF,
ifnull(CT_SUPPLY_CONSTRAINT,0) CT_SUPPLY_CONSTRAINT,
ifnull(CT_NEW_PRODUCT,0) CT_NEW_PRODUCT,
ifnull(CT_COMMERCIAL_RISK,0) CT_COMMERCIAL_RISK,
ifnull(CT_FINANCIAL_ADJUSTMENT,0) CT_FINANCIAL_ADJUSTMENT,
ifnull(CT_FINANCIAL_FORECAST,0) CT_FINANCIAL_FORECAST,
ifnull(CT_FINANCIAL_FORECASTSELECTED,0) CT_FINANCIAL_FORECASTSELECTED,
IFNULL(ct_onhandqtyiru,0) ct_onhandqtyiru
,CASE 
	 WHEN  ((fosfpa.PartTypeDescription) IN (('Finished products'),('Trading goods')) )
				AND ((fosfpa.PlantMaterialStatus) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('TA')) ) 
				AND (LOWER(fosfpa.deletionflag) = LOWER('Not Set')) 
				AND ((fosfpa.crossmatplantsts) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('00'),('TA')) )
				AND fosfpa.ProductGroupDescription_Merck  NOT IN ('Technology') /*MRC-1178*/
		THEN  'ACTIVE'
	WHEN  ((fosfpa.PartTypeDescription) IN (('Finished products'),('Trading goods')) )
				AND ((fosfpa.PlantMaterialStatus) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('TA')) ) 
				AND (LOWER(fosfpa.deletionflag) = LOWER('Not Set')) 
				AND ((fosfpa.crossmatplantsts) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('00'),('TA')) )
				AND (fosfpa.ProductGroupDescription_Merck IN ('Technology') AND pl.planttitle_merck IN ('Russia','France','Belgium','Netherlands','Portugal','Spain','Finland','Norway','Sweden','Denmark','Ireland','Germany','Switzerland','Austria','Greece','Turkey') AND fosfpa.GeneralItemCategory IN ('NORM'))
		THEN  'ACTIVE'
		ELSE  'INACTIVE'
end AS DD_ACTION_MESSAGE_STATE,
f.CT_NASP_PRAUSED ,
f.CT_NASP_PRAUSEDLC ,
f.CT_NASP_PRA_ADJUSTMENT,
f.NASP_FLAG,
ifnull(f.ct_exchangerate_pl,1) as ct_exchangerate_pl,
fosfpa.PlantMaterialStatus,
fosfpa.COMMERCIAL_GROUPING,
f.CT_SALESQUANTITY_MTD,
fosfpa.GeneralItemCategory
from fact_fosalesforecastcortex f
INNER JOIN Dim_Part AS fosfpa ON f.dim_partid = fosfpa.Dim_Partid 
/*
AND ((fosfpa.PartTypeDescription) IN (('Finished products'),('Trading goods')) )
AND ((fosfpa.PlantMaterialStatus) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6')) )  
AND (LOWER(fosfpa.deletionflag) = LOWER('Not Set'))  
AND ((fosfpa.crossmatplantsts) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('00')) )
*/
AND ((fosfpa.PartNumber_NoLeadZero) NOT IN (('163333'),('189746'),('179195'),('194452')) )
INNER JOIN dim_plant pl ON pl.plantcode = f.dd_plantcode
INNER JOIN dim_company dc ON dc.companycode = pl.companycode
INNER JOIN Dim_Date as dd ON dd.dim_dateid = dim_dateidforecast /*AND dd.plantcode_factory = f.dd_plantcode AND dc.companycode = dd.companycode*/
LEFT JOIN TMP_DFA_MAIN_ERRORDRIVER DFA ON f.dd_partnumber = DFA.PARTNUMBER AND f.dd_plantcode = dfa.dd_plantcode
                                       AND f.dd_country_destination_code = dfa.dd_country_destination_code AND dd.monthyear = dfa.DD_REPORTINGDATE
                                       AND fosfpa.dominantspeciesdescription_merck = dfa.dominantspeciesdescription_merck
                                       AND fosfpa.prodfamilydescription_merck = dfa.prodfamilydescription_merck
                                       AND f.dd_planttitlemerck = dfa.dd_planttitlemerck
                                       /*AND f.dd_region = dfa.dd_region*/
                                       AND fosfpa.ProductFamily_Merck = dfa.ProductFamily_Merck
where dd_latestreporting='Yes'
and dd_holdout='10'
and f.dd_plantcode <> 'NL10'
and to_date(to_char(f.dd_forecastdate),'YYYYMMDD') between to_date(to_char(concat(year(add_months( TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-36)),
case when length(month(add_months( TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-36)))=1 then concat('0',month(add_months( TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-36)))
else month(add_months( TO_DATE(f.dd_reportingdate,'DD MON YYYY'),-36)) end,'01')),'YYYYMMDD')
and add_months(TO_DATE(f.dd_reportingdate,'DD MON YYYY'),37);

/* MRC-1030  Vali 20200521 add tmp_idp_active_recommendations for tracking changes on dim_part statuses */
create or replace table tmp_idp_active_recommendations
as
select distinct f.DD_PARTNUMBER, 
                f.DD_PLANTCODEMERCK, 
                f.DD_REPORTINGDATE,
                f.DD_COUNTRY_DESTINATION_CODE, 
                fosfpa.PartTypeDescription,
				fosfpa.PlantMaterialStatus,
				fosfpa.deletionflag,
				fosfpa.crossmatplantsts,
				CURRENT_date as insert_date,
				2			 as is_updated 
from  fact_fosalesforecastcortex f
INNER JOIN dim_plant pl ON pl.plantcode = f.dd_plantcode
INNER JOIN Dim_Part AS fosfpa ON f.dim_partid = fosfpa.Dim_Partid 
AND ((fosfpa.PartTypeDescription) IN (('Finished products'),('Trading goods')) )
AND ((fosfpa.PlantMaterialStatus) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('TA')) )
AND (LOWER(fosfpa.deletionflag) = LOWER('Not Set'))  
AND ((fosfpa.crossmatplantsts) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('00'),('TA')) )
AND (fosfpa.ProductGroupDescription_Merck  NOT IN ('Technology') /*MRC-1178*/
OR (fosfpa.ProductGroupDescription_Merck IN ('Technology') AND pl.planttitle_merck IN ('Russia','France','Belgium','Netherlands','Portugal','Spain','Finland','Norway','Sweden','Denmark','Ireland','Germany','Switzerland','Austria','Greece','Turkey') AND fosfpa.GeneralItemCategory IN ('NORM')))
AND ((fosfpa.PartNumber_NoLeadZero) NOT IN (('163333'),('189746'),('179195'),('194452')) )
INNER JOIN dim_company dc ON dc.companycode = pl.companycode
INNER JOIN Dim_Date as dd ON dd.dim_dateid = dim_dateidforecast
where dd_latestreporting='Yes'
and dd_holdout='10'
and f.dd_plantcode <> 'NL10'; 

CREATE OR REPLACE TABLE tmp_dfa_rk
as
SELECT DISTINCT    b.dd_partnumber,
			       b.DD_PLANTCODE,
			       b.DD_REPORTINGDATE,
			       b.DD_COUNTRY_DESTINATION_CODE,
			       b.DD_PLANTTITLEMERCK,
			       p.sub_product_family_code_pma,
			       b.dfa_main_error
			       ,dense_rank() over(partition by b.DD_PLANTCODE order by b.dfa_main_error DESC, p.sub_product_family_code_pma) rk_dfa_main_error
 from fact_idp b
 JOIN dim_part p
 ON b.DIM_PARTID = p.DIM_PARTID
 and dfa_main_error > 0 ;

UPDATE fact_idp f
  SET dfa_rank = rk_dfa_main_error
FROM   fact_idp f, tmp_dfa_rk r
WHERE f.dd_partnumber = r.dd_partnumber
AND f.DD_PLANTCODE = r.DD_PLANTCODE
AND f.DD_REPORTINGDATE = r.DD_REPORTINGDATE
AND f.DD_COUNTRY_DESTINATION_CODE = r.DD_COUNTRY_DESTINATION_CODE
AND f.dfa_main_error> 0;


DROP TABLE IF EXISTS tmp_bias_indicator;
  CREATE TABLE tmp_bias_indicator AS
  SELECT
			        aplfprt.partnumber,
					f_iaplf.dd_plantcode  ,
					f_iaplf.dd_planttitlemerck ,
					aplfprt.ProductFamily_Merck,
					aplfprt.prodfamilydescription_merck,
					aplfprt.dd_partnumber_description,
					max(f_iaplf.DD_FORECASTBIASlast12MOnths)   DD_FORECASTBIASlast12Months,
					max(f_iaplf.DD_FORECASTBIAS3MONTHS)        DD_FORECASTBIASlast3Months,
					max(f_iaplf.DD_FORECASTBIASACTION)         DD_FORECASTBIASACTION
  FROM fact_atlaspharmlogiforecast_merck AS f_iaplf
   INNER JOIN Dim_Date AS repdt ON
	 f_iaplf.dim_dateidreporting = repdt.Dim_Dateid
  INNER JOIN Dim_Part AS aplfprt ON
 f_iaplf.dim_partid = aplfprt.Dim_Partid
  AND (lower(aplfprt.deletionflag) = lower('Not Set'))
  AND ((aplfprt.PlantMaterialStatus) IN (('Not Set'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('TA')) )
   AND ((aplfprt.crossmatplantsts) IN (('Not Set'),('Z0'),('Z1'),('Z2'),('Z3'),('Z4'),('Z5'),('Z6'),('00'),('TA')) )
	AND ((aplfprt.PartTypeDescription) IN (('Finished products'),('Trading goods')) )
	AND (lower(aplfprt.PartNumber_NoLeadZero) NOT LIKE lower('%X%'))
 INNER JOIN dim_date AS x_varDate ON
 x_varDate.DateValue = CURRENT_DATE
 AND repdt.CompanyCode = x_varDate.CompanyCode
 AND repdt.plantcode_factory = x_varDate.plantcode_factory
 AND ((repdt.CalendarMonthID) != (x_varDate.CalendarMonthID))
 WHERE
	  (lower
	( CASE
		WHEN f_iaplf.dd_version = 'SFA' THEN 'DFA'
		ELSE f_iaplf.dd_version
	END) = lower('DFA'))
	AND (
	   repdt.MonthYear =   to_char(add_months(CURRENT_DATE, -3), 'Mon yyyy')
	OR  repdt.MonthYear =  to_char(add_months(CURRENT_DATE, -2), 'Mon yyyy')
	OR  repdt.MonthYear =  to_char(add_months(CURRENT_DATE, -1), 'Mon yyyy')
	)
	 AND DD_FORECASTBIASACTION = 'Yes'
GROUP BY aplfprt.partnumber,
		f_iaplf.dd_plantcode,
		f_iaplf.dd_planttitlemerck,
		aplfprt.ProductFamily_Merck,
		aplfprt.prodfamilydescription_merck,
		aplfprt.dd_partnumber_description
;

 UPDATE fact_idp
    SET DD_FORECASTBIASACTION =  t.DD_FORECASTBIASACTION,
        DD_FORECASTBIASlast3Months = t.DD_FORECASTBIASlast3Months
  FROM fact_idp f, tmp_bias_indicator t
  WHERE f.dd_partnumber = t.PARTNUMBER
  AND f.dd_plantcode = t.DD_PLANTCODE
  AND f.DD_PLANTTITLEMERCK = t.DD_PLANTTITLEMERCK
  AND f.ProductFamily_Merck = t.ProductFamily_Merck
  AND f.prodfamilydescription_merck = t.prodfamilydescription_merck
  ;


/* MRC-681 Vali 21 Jan */
create or replace table tmp_idp_segmentation_local
AS
WITH tmp_sales AS
(

 SELECT
	fosf.DD_PLANTCODEMERCK,
	ROUND(SUM(CASE WHEN CT_SALESDELIVMOVING_NEW IS NULL THEN 0 ELSE CT_SALESDELIVMOVING_NEW END), 0) AS SalesAmountMovingAnnual,
	fosf.dim_partid  ,
	fosf.dim_plantID  ,
	fosf.dd_partnumber ,
	fosf.dd_plantcode,
	fosf.DD_COUNTRY_DESTINATION_CODE
FROM
	fact_idp AS fosf
INNER JOIN Dim_Date AS fosffd ON
	fosf.dim_dateidforecast = fosffd.Dim_Dateid
INNER JOIN Dim_Part AS fosfpa ON
	fosf.dim_partid = fosfpa.Dim_Partid
WHERE dd_forecastrank = '1'
	AND fosf.dd_latestreporting = 'Yes'
	AND trunc(to_date( to_char(fosf.DD_FORECASTDATE), 'YYYYMMDD'),'MM') =  trunc(ADD_MONTHS(to_date( fosf.dd_reportingdate, 'dd Mon yyyy'), -1 ), 'MM')
GROUP BY
	fosf.DD_PLANTCODEMERCK,
	fosf.dim_partid,
	fosf.dim_plantID,
	fosf.dd_plantcode,
	fosf.dd_partnumber,
	fosf.DD_COUNTRY_DESTINATION_CODE
),
tmp_sales_percentage AS
(
SELECT t.*,
       SUM(SalesAmountMovingAnnual) over(PARTITION BY dd_plantcode) SUM_PLANT,
       round( (SalesAmountMovingAnnual/ SUM(SalesAmountMovingAnnual) over(PARTITION BY dd_plantcode) )*100, 2)  sales_percentage
FROM tmp_sales t
WHERE SalesAmountMovingAnnual > 0 )
, tmp_sales_cum_percentage AS
(
SELECT x.*,
      round(
             SUM (sales_percentage) over(partition by x.dd_plantcode
									           ORDER BY x.SalesAmountMovingAnnual desc
								          ROWS BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW)
	        , 2) AS cum_percentage
FROM tmp_sales_percentage x)
SELECT  y.*,
        CASE WHEN y.cum_percentage BETWEEN 0 AND 80 THEN 'A'
             WHEN y.cum_percentage > 80  AND y.cum_percentage < 95 THEN 'B'
             WHEN y.cum_percentage  > 95 THEN 'C'
        END AS dd_segmentation_local
FROM tmp_sales_cum_percentage y;


 UPDATE fact_idp
    SET dd_segmentation_local =  nvl(t.dd_segmentation_local ,'Not Set')
  FROM fact_idp f, tmp_idp_segmentation_local t
  WHERE f.dd_partnumber = t.dd_partnumber
  AND f.dd_plantcode = t.DD_PLANTCODE
  AND f.DD_COUNTRY_DESTINATION_CODE = t.DD_COUNTRY_DESTINATION_CODE;
