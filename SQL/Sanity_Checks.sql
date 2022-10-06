--- select * from tmp_postproc_cleanupandrerank

select * from Temp_merck_gross_sales limit 10

-- to check count of grains from forecast rank from 1 to 5
select  DD_FORECASTRANK, 
count (distinct  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) cnt
from Temp_merck_gross_sales
group by DD_FORECASTRANK
order by DD_FORECASTRANK asc

-- select * from tmp_postproc_cleanupandrerank

--- which forecast algo comes rank one for how many grains
select DD_FORECASTTYPE, 
count (distinct  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) cnt
from Temp_merck_gross_sales
where DD_FORECASTRANK = 1
group by DD_FORECASTTYPE
order by cnt desc

/* Missing grain between sales and final forecast data */

select  count(distinct( DD_PARTNUMBER||DD_PLANT||DD_COMPANY)) , 'Input_vs_Output_Missing_Grains' from   Fact_SALESHISTORY_Cortex --- net sales table fact_saleshistory_cortex_net gross sales table - Fact_SALESHISTORY_Cortex
where (DD_PARTNUMBER||DD_PLANT||DD_COMPANY) not in
(
select DISTINCT concat( DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE)
from  Temp_merck_gross_sales
where dd_forecastrank = 1);

--- select * from Fact_SALESHISTORY_Cortex limit 5
--- Overall_Metric_Summary
/*select
        count (distinct  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) cnt,
        round(avg(ct_mae),2),
        round(median(ct_mae),2),
        'Overall_Metric_Summary'
from Temp_merck_gross_sales
where DD_FORECASTRANK = 1;*/

--- rank 1 grains count
select dd_forecasttype,count(DISTINCT  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) cnt
from  Temp_merck_gross_sales
where dd_forecastrank = 1 
GROUP BY DD_FORECASTTYPE order by cnt desc

-- percentage of flat forecast
WITH a1 as
(select count(*) cnt
from
(
select  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE grain, stddev(CT_FORECASTQUANTITY) std_forecast, dd_forecasttype
FROM   Temp_merck_gross_sales
WHERE dd_forecastrank = 1  
-- and dd_reportingdate = select to_char(current_date,'DD MON YYYY')  ReportingDate 
and  dd_forecasttype not in ('FALL_BACK_NAIVE'   )
and dd_forecastdate >= SELECT to_char(trunc(ADD_MONTHS(current_date,1),'mon') -1,'YYYYMMDD')
group by  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE, dd_forecasttype
having stddev(CT_FORECASTQUANTITY) = 0)),
a2 AS(SELECT SUM(cnt) AS total_grains
from
(
select dd_forecasttype, count(DISTINCT  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) cnt
from   Temp_merck_gross_sales
where dd_forecastrank = 1 
--- and dd_reportingdate = select to_char(current_date,'DD MON YYYY')  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC ))
SELECT round(a1.cnt * 100/a2.total_grains,2), 'percent_flat_Grains'  FROM a1,a2;

/* Percent of grain while zero flat forecast in horizon */

WITH a1 as
(select count(*) cnt 
from
(
select  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE grain, 
sum(CT_SALESQUANTITY) sum_sales, 
sum(CT_FORECASTQUANTITY) sum_forecast, dd_forecasttype
FROM Temp_merck_gross_sales
WHERE dd_forecastrank = 1 
--- and dd_reportingdate = select to_char(current_date,'DD MON YYYY')  ReportingDate 
and  dd_forecasttype not in ('FALL_BACK_NAIVE')
and dd_forecastdate >= SELECT to_char(trunc(ADD_MONTHS(current_date,1),'mon') -1,'YYYYMMDD')
 
group by  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE, dd_forecasttype
having sum(CT_FORECASTQUANTITY) = 0 and sum(CT_SALESQUANTITY) > 1)) ,
a2 AS(SELECT SUM(cnt) AS total_grains
from
(
select dd_forecasttype, count(DISTINCT  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) cnt
from  Temp_merck_gross_sales
where dd_forecastrank = 1 
-- and dd_reportingdate = select to_char(current_date,'DD MON YYYY')  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT ifnull(round(a1.cnt * 100/a2.total_grains,2 ),0), 'percent_of_grains_zero_flat_forecast_in_horizon' FROM a1,a2;

/* 75 percentile  */

/*SELECT distinct (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ct_mae) 
		OVER (PARTITION BY dd_reportingdate)), 'mae_75_percentile'
from  Temp_merck_gross_sales
where dd_forecastrank = 1*/
  
/* % of grains which have mape >100 */
/*with t1 as (
-- select count(DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) grain_mae_than100
select count(grain) grain_mae_than100 
from 
(
SELECT distinct  (DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) grain, ct_mae
from  Temp_merck_gross_sales
where dd_forecastrank = 1 
having ct_mae>=100
)),
t2 as (select count(distinct  DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE) total_grain_count
FROM  Temp_merck_gross_sales
WHERE dd_forecastrank = 1 
)
SELECT ifnull(round(t1.grain_mae_than100 * 100 /nullif(t2.total_grain_count,0),2),0), 'grain_percent_of_mae100' FROM t1,t2;*/


/* Grain where null value in date column*/

select count(DISTINCT (DD_PARTNUMBER||DD_SALES_COCD||'|'||DD_REPORTING_COMPANY_CODE||'|'||DD_HEI_CODE||'|'||DD_COUNTRY_DESTINATION_CODE||'|'||DD_MARKET_GROUPING||DD_COMPANYCODE)), 'grain_in_null_date'
from Temp_merck_gross_sales
where dd_forecastrank = 1
and dd_forecastdate is null ;

--- select * from Temp_merck_net_sales limit 10
--- select * from FACT_FORECASTOUTPUT_CORTEX_DATA where dd_jobid = 'ff4f8114efda4f8d95a01b60565fa36b' limit 5

select DISTINCT(DD_PARTNUMBER, DD_SALES_COCD, DD_REPORTING_COMPANY_CODE, DD_HEI_CODE, DD_COUNTRY_DESTINATION_CODE, DD_MARKET_GROUPING, DD_COMPANYCODE),
count( DISTINCT dd_forecastrank) cnount
from  Temp_merck_gross_sales
where dd_forecasttype not in ('FALL_BACK_NAIVE')
and dd_forecastrank < 5
group by DD_PARTNUMBER, DD_SALES_COCD, DD_REPORTING_COMPANY_CODE, DD_HEI_CODE, DD_COUNTRY_DESTINATION_CODE, DD_MARKET_GROUPING, DD_COMPANYCODE


/*select count(distinct dd_grain1, dd_grain2, dd_grain3
from FACT_FORECASTOUTPUT_CORTEX_DATA
where dd_jobid = 'ff4f8114efda4f8d95a01b60565fa36b'*/

--- select * from dim_jobmetadata_cortex_data where dd_jobid = 'ff4f8114efda4f8d95a01b60565fa36b'
--- select * from Temp_merck_gross_sales limit 5
--- select * from FACT_FORECASTOUTPUT_CORTEX_DATA where dd_jobid = 'bad23c036b7a4647a5bb82d63c7f734b' limit 5
--- select * from stg_currentrunparam limit 5
-- fact_aeraforecastanalysis_netsales
-- select * from fact_aeraforecastanalysis_grosssales limit 5
--- select count(*) from fact_aeraforecastanalysis_netsales limit 5
-- select * from fact_aeraforecastanalysis_grosssales limit 5
--- truncate table fact_aeraforecastanalysis_grosssales

--- truncate table fact_aeraforecastanalysis_netsales
--- select * from fact_aeraforecastanalysis_grosssales limit 5
--- select * from Temp_merck_net_sales limit 5

/*select distinct dd_grain1, dd_grain2,dd_grain3,dd_actualdatevalue,dd_forecastalgorithm,count(*)
from FACT_FORECASTOUTPUT_CORTEX_DATA
where dd_jobid = 'bad23c036b7a4647a5bb82d63c7f734b'
group by 1,2,3,4,5 having count(*) >1 
ORDER by 1,2,3,4,5 asc*/

--- select * from Temp_merck_net_sales limit 10

/*Select distinct DD_grain1, DD_grain2,DD_grain3, DD_FORECASTALGORITHM,DD_ACTUALDATEVALUE, count(*) 
from FACT_FORECASTOUTPUT_CORTEX_DATA
where dd_jobid = '648ab17d20ec4927b381d9b66547360a' 
GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1 
order by 1,2,3,4,5 asc*/

/*Select * from FACT_FORECASTOUTPUT_CORTEX_DATA 
where dd_jobid = '648ab17d20ec4927b381d9b66547360a'  limit 5*/

-- select * from DIM_JOBMETADATA_CORTEX_DATA where dd_jobid = '648ab17d20ec4927b381d9b66547360a'
/*update  FACT_CORTEX_FRAMEWORK_JOBS_STATUS
set DD_PROCESSINGRESULT = 'TaskAggregator has successfully updated status into exasol for JobId: 648ab17d20ec4927b381d9b66547360a'
where dd_jobid = '648ab17d20ec4927b381d9b66547360a'*/

--- select * from stg_currentrunparam limit 5

/*update stg_currentrunparam
set DD_JOBID = '69cf7099b9b2465b8e508bcaafb68291';
*/
select * from Temp_merck_gross_sales limit 5

--- select * from stg_currentrunparam

---- select * from FACT_CORTEX_FRAMEWORK_JOBS_STATUS where DD_JOBID = 'a29691ba619f4e129ccc165a47152ebe'

/*update  FACT_CORTEX_FRAMEWORK_JOBS_STATUS
set DD_PROCESSINGRESULT = 'TaskAggregator has successfully updated status into exasol for JobId: a29691ba619f4e129ccc165a47152ebe'
where dd_jobid = 'a29691ba619f4e129ccc165a47152ebe'*/ 


/*select dd_calendarmonthid ,count(distinct(DD_PARTNUMBER||DD_PLANT||DD_COMPANY)) as cnt_grain,
sum(ct_salesquantity) sum_sales, avg(ct_salesquantity) avg_sales
from Fact_SALESHISTORY_Cortex
group by dd_calendarmonthid
order by dd_calendarmonthid;
*/
select count(*) from Temp_merck_gross_sales limit 5
--- select  count(distinct( DD_PARTNUMBER||DD_PLANT||DD_COMPANY)) 'Input_vs_Output_Missing_Grains' from   Fact_SALESHISTORY_Cortex


select dd_calendarmonthid ,count(distinct (DD_PARTNUMBER,DD_PLANT,DD_COMPANY)) as cnt_grain,
sum(ct_salesquantity) sum_sales, avg(ct_salesquantity) avg_sales
from  Fact_SALESHISTORY_Cortex 
group by dd_calendarmonthid
order by dd_calendarmonthid;

select count(distinct(DD_PARTNUMBER,DD_PLANT,DD_COMPANY)) as cnt  from Fact_SALESHISTORY_Cortex 

select * from Temp_merck_gross_sales
where DD_PARTNUMBER = '138475'  and DD_SALES_COCD = 'AT30'
and DD_REPORTING_COMPANY_CODE = 'AT1467' and DD_HEI_CODE = 'Not Set'
and	DD_COUNTRY_DESTINATION_CODE = 'Not Set'
and dd_forecastrank = 1

select DD_reportingdate, to_date(dd_reportingdate,'DD MON YYYY') rptdate, count(distinct (DD_PARTNUMBER,DD_SALES_COCD, DD_REPORTING_COMPANY_CODE, DD_HEI_CODE,DD_COUNTRY_DESTINATION_CODE)) as cnt,
       round(avg(ct_mae),2) avg_mae,
       round(median(ct_mae),2) med_mae,
       -- round(avg(ct_mae),2) avg_,
       -- round(median(ct_mae),2) med_mape ,
       --- round(avg(CT_mae),2) avg_wape,
       --- round(median(CT_mae),2) med_wape,
       --- round(avg(CT_mae),2) cov,
       'Overall_Metric_Summary'
from FACT_AERAFORECASTANALYSIS_GROSSSALES
where dd_forecastrank = 1
  and dd_forecasttype not in( '3-MMA')
GROUP BY DD_reportingdate,to_date(dd_reportingdate,'DD MON YYYY')
ORDER BY to_date(dd_reportingdate,'DD MON YYYY') desc limit 5;

select * from FACT_AERAFORECASTANALYSIS_GROSSSALES limit 5

select count(distinct(DD_PARTNUMBER,DD_PLANT,DD_COMPANY)) as cnt  from Fact_SALESHISTORY_Cortex 
