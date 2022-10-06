
/* ******************************************************************************************************* */
/*  Script : sanity_check.sql                                                                            */
/*  Description : Monthly Zoetis Postproc Sanity Check script.                                             */
/*  Created On  : 23 Feb 2021                                                      	                 */
/*  Created By : Umesh                                                                                   */
/*********************************************************************************************************/


/* dropping the tmp table before using this */
drop table if exists tmp_fact_sanity_check_results;

/* Creating tables */
create table tmp_fact_sanity_check_results
(
    rptflag varchar(5),
    rptdate varchar(20),
    avg_mape decimal(18,2),
    median_mape decimal(18,2),
    avg_rmse decimal(18,2),
    median_rmse decimal(18,2),
    avg_wape decimal(18,2),
    median_wape decimal(18,2),
    cov decimal(18,2),
    repodate_grain_count varchar(20),
    forecasttype varchar(50),
    method_grain_count varchar(10),
    total_grain_count varchar(10),
    sanity_check_name varchar(30),
    missing_grains varchar(10),
    percent_flat_Grains decimal(18,2),
    percent_zero_flat_Grains decimal(18,2),
    mape_75_percentile decimal(18,2),
    percent_of_mape decimal(18,2),
    current_vs_last_snapshot_Grains varchar(100),
    null_date_grains varchar(100)

);

/*
create table fact_sanity_check_results
(
    fact_sanity_check_resultsID int,
    dd_rptflag varchar(5),
    dd_rptdate varchar(20),
    ct_avg_mape decimal(18,2),
    ct_median_mape decimal(18,2),
    ct_median_rmse decimal(18,2),
    ct_avg_rmse decimal(18,2),
    ct_median_wape decimal(18,2),
    ct_avg_wape decimal(18,2),
    ct_cov decimal(18,2),
    dd_snapshot_date varchar(20),
    dd_repodate_grain_count varchar(20),
    dd_name char(6),
    dd_forecasttype varchar(50),
    dd_method_grain_count varchar(10),
    dd_total_grain_count varchar(10),
    dd_sanity_check_name varchar(30),
    dd_missing_grains varchar(10),
    ct_percent_flat_Grains decimal(18,2),
    ct_percent_zero_flat_Grains decimal(18,2),
    ct_mape_75_percentile decimal(18,2),
    ct_percent_of_mape decimal(18,2),
    dd_current_vs_last_snapshot_Grains varchar(100),
    dd_null_date_grains varchar(100),
    dd_latest_reporting_flag char(3)
);
*/

 s.DD_PARTNUMBER = r.DD_PARTNUMBER
 
select count(distinct(DD_PARTNUMBER) ) cnt
from stage_fosalesforecast
where dd_jobid = '9bf1a0f688b6423f92d848f5981c0e4b' limit 10

/* Overall metric summary check */
insert into tmp_fact_sanity_check_results
(
    rptflag,
    rptdate,
    repodate_grain_count,
    avg_rmse,
    median_rmse,
    avg_mape,
    median_mape,
    avg_wape,
    median_wape,
    cov,
    sanity_check_name
)
select DD_LATESTREPORTING, to_date(dd_reportingdate,'DD MON YYYY') rptdate, count (distinct DD_COMBINED_PRODUCTBUSAREASALESORG) cnt,
       round(avg(ct_rmse_cv),2) avg_rmse,
       round(median(ct_rmse_cv),2) med_rmse,
       round(avg(ct_mape_cv),2) avg_mape,
       round(median(ct_mape_cv),2) med_mape ,
       round(avg(CT_WAPE_cv),2) avg_wape,
       round(median(CT_WAPE_cv),2) med_wape,
       round(avg(CT_COV_fcst),2) cov,
       'Overall_Metric_Summary'
from fact_fosalesforecast
where dd_forecastrank_mape_cv = 1
  and dd_forecasttype not in( '3-MMA')
GROUP BY DD_LATESTREPORTING,to_date(dd_reportingdate,'DD MON YYYY')
ORDER BY to_date(dd_reportingdate,'DD MON YYYY') desc limit 5;


/* Grain vs Algo count */
insert into tmp_fact_sanity_check_results
(
    forecasttype,
    method_grain_count,
    total_grain_count,
    sanity_check_name
)
WITH t1 as
(select dd_forecasttype,count(DISTINCT DD_COMBINED_PRODUCTBUSAREASALESORG) cnt
from  fact_fosalesforecast
where dd_forecastrank_mape_cv = 1 
-- and dd_forecasttype not in( '3-MMA')
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))
GROUP BY DD_FORECASTTYPE order by cnt desc ) ,
t2 AS(SELECT SUM(cnt) AS total_count
from
(
select dd_forecasttype, count(DISTINCT DD_COMBINED_PRODUCTBUSAREASALESORG) cnt
from  fact_fosalesforecast
where dd_forecastrank_mape_cv = 1 
-- and dd_forecasttype not in( '3-MMA')
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT t1.DD_FORECASTTYPE,t1.cnt,t2.total_count, 'Method_vs_Grains' FROM t1,t2;

/* Missing grain between sales and final forecast data */
insert into tmp_fact_sanity_check_results
(
    missing_grains ,
    sanity_check_name
)
select  count(distinct(DD_partnumber)) missing_grains, 'Input_vs_Output_Grains' from   Fact_SALESHISTORY_Cortex
where (DD_partnumber) not in
(
select DISTINCT concat(DD_COMBINED_PRODUCTBUSAREASALESORG)
from  fact_fosalesforecast
WHERE dd_reportingdate
    in  (select concat('01',' ', select to_char((current_date  ),'MON YYYY'))  ReportingDate )
  and dd_forecastrank_mape_cv = 1
    );

/* Percent of grains under flat value */
insert into tmp_fact_sanity_check_results
(
    percent_flat_Grains ,
    sanity_check_name
)
WITH t1 as
(select count(*) cnt 
from
(
select DD_COMBINED_PRODUCTBUSAREASALESORG grain, stddev(CT_FORECASTQUANTITY_FULLHISTORY) std_forecast, dd_forecasttype
FROM  fact_fosalesforecast
WHERE dd_forecastrank_mape_cv = 1  
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))  ReportingDate 
and  dd_forecasttype not in ('FALL_BACK_NAIVE', '3-MMA')
and dd_forecastdate >= SELECT to_char(trunc(ADD_MONTHS(current_date,1),'mon') -1,'YYYYMMDD')
group by DD_COMBINED_PRODUCTBUSAREASALESORG, dd_forecasttype
having stddev(CT_FORECASTQUANTITY_FULLHISTORY) = 0)),
t2 AS(SELECT SUM(cnt) AS sum_of_count
from
(
select dd_forecasttype, count(DISTINCT DD_COMBINED_PRODUCTBUSAREASALESORG) cnt
from  fact_fosalesforecast
where dd_forecastrank_mape_cv = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT round(t1.cnt * 100/t2.sum_of_count,2) percent_grains, 'percent_flat_Grains'  FROM t1,t2;

/* Percent of grain while zero flat forecast in horizon */
insert into tmp_fact_sanity_check_results
(
    percent_flat_Grains ,
    sanity_check_name
)

/* SELECT to_char(LAST_DAY(current_date), 'YYYYMMDD') -- to be replace with forecast date */
WITH t1 as
(select count(*) cnt 
from
(
select DD_COMBINED_PRODUCTBUSAREASALESORG grain, sum(CT_SALESQUANTITY) sum_sales, sum(CT_FORECASTQUANTITY_FULLHISTORY) sum_forecast, dd_forecasttype
FROM  fact_fosalesforecast
WHERE dd_forecastrank_mape_cv = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))  ReportingDate 
and  dd_forecasttype not in ('FALL_BACK_NAIVE','3-MMA')
and dd_forecastdate >= SELECT to_char(trunc(ADD_MONTHS(current_date,1),'mon') -1,'YYYYMMDD')
group by DD_COMBINED_PRODUCTBUSAREASALESORG, dd_forecasttype
having sum(CT_FORECASTQUANTITY_FULLHISTORY) = 0 and sum(CT_SALESQUANTITY) > 1)) ,
t2 AS(SELECT SUM(cnt) AS sum_of_count
from
(
select dd_forecasttype, count(DISTINCT DD_COMBINED_PRODUCTBUSAREASALESORG) cnt
from  fact_fosalesforecast
where dd_forecastrank_mape_cv = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT ifnull(round(t1.cnt * 100/t2.sum_of_count,2 ),0) percent_Grains, 'percent_zero_flat_forecast_grains' FROM t1,t2;


/* 75 percentile  */
insert into tmp_fact_sanity_check_results
(
    mape_75_percentile ,
    sanity_check_name
)
SELECT distinct (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ct_mape_cv) 
		OVER (PARTITION BY dd_reportingdate)) percent_grains, 'mape_75_percentile'
from  fact_fosalesforecast
where dd_forecastrank_mape_cv = 1
  and dd_reportingdate = select concat('01',' ', select to_char((current_date ),'MON YYYY'));


/* % of grains which have mape >100; if >25% then is problem (inflection forecast) */
insert into tmp_fact_sanity_check_results
(
    percent_of_mape,
    sanity_check_name
)
with t1 as (
select count(DD_COMBINED_PRODUCTBUSAREASALESORG) grain_mape_than100
from 
(
SELECT distinct  DD_COMBINED_PRODUCTBUSAREASALESORG, ct_mape_cv
from  fact_fosalesforecast
where dd_forecastrank_mape_cv = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))
having ct_mape_cv>=100
)),
t2 as (select count(distinct DD_COMBINED_PRODUCTBUSAREASALESORG) total_grain_count
FROM  fact_fosalesforecast
WHERE dd_forecastrank_mape_cv = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date  ),'MON YYYY'))
)
SELECT ifnull(round(t1.grain_mape_than100 * 100 /t2.total_grain_count,2),0) grain_percent, 'grain_percent_of_mape100' FROM t1,t2;

/* New grains (between latest and last snapshot */
insert into tmp_fact_sanity_check_results
(
    current_vs_last_snapshot_Grains ,
    sanity_check_name
)
select count(DISTINCT concat(DD_COMBINED_PRODUCTBUSAREASALESORG)) grains_no, 'current_vs_last_snap_Grains'
from  fact_fosalesforecast
WHERE dd_reportingdate
    in  (select concat('01',' ', select to_char((current_date  ),'MON YYYY'))  ReportingDate )
  and dd_forecastrank_mape_cv = 1
  and (DD_COMBINED_PRODUCTBUSAREASALESORG) not in
(
select DISTINCT concat(DD_COMBINED_PRODUCTBUSAREASALESORG)
from  fact_fosalesforecast
WHERE dd_reportingdate
    in  ( select concat('01',' ', select to_char((current_date - interval '1' month),'MON YYYY'))  ReportingDate )
  and dd_forecastrank_mape_cv = 1
    );

/* Grain where null value in date column*/
insert into tmp_fact_sanity_check_results
(
    null_date_grains,
    sanity_check_name
)
select count(DISTINCT (DD_COMBINED_PRODUCTBUSAREASALESORG)) Grains, 'grain_where_null_value_date'
from  fact_fosalesforecast
WHERE dd_reportingdate
    in  ( select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate )
  and dd_forecastrank_mape_cv = 1
  and dd_forecastdate is null ;

/* deleting lastet snapshot date data */
delete * from fact_sanity_check_results
where dd_snapshot_date = to_date(current_date,'DD MON YYYY');

/* Set flag to No for old data */
update fact_sanity_check_results
set latest_reporting_flag = 'No';

/* insert tmp table data into final sanity check fact table */
insert into fact_sanity_check_results
(
    fact_sanity_check_resultsID,
    dd_rptdate ,
    dd_rptflag ,
    dd_repodate_grain_count,
    ct_avg_mape ,
    ct_median_mape,
    ct_avg_rmse ,
    ct_median_rmse,
    ct_avg_wape,
    ct_median_wape,
    ct_cov ,
    dd_snapshot_date,
    dd_name,
    dd_forecasttype,
    dd_method_grain_count,
    dd_total_grain_count,
    dd_sanity_check_name ,
    dd_missing_grains,
    ct_percent_flat_Grains,
    ct_percent_zero_flat_Grains,
    ct_mape_75_percentile,
    ct_percent_of_mape,
    dd_current_vs_last_snapshot_Grains,
    dd_null_date_grains,
    dd_latest_reporting_flag
)
select  (
            select
            ifnull(max(fact_sanity_check_resultsID ), 0) from fact_sanity_check_results m)+ row_number()
    over(order by '') as fact_sanity_check_resultsID,
        rptdate,
        rptflag,
        repodate_grain_count,
        avg_mape,
        median_mape,
        avg_rmse,
        median_rmse,
        avg_wape,
        median_wape,
        cov,
        to_date(current_date,'DD MON YYYY'),
        'Zoetis',
        forecasttype,
        method_grain_count,
        total_grain_count,
        sanity_check_name,
        missing_grains,
        percent_flat_Grains,
        percent_zero_flat_Grains,
        mape_75_percentile,
        percent_of_mape,
        current_vs_last_snapshot_Grains,
        null_date_grains,
        'Yes'
from tmp_fact_sanity_check_results;




/* select * from tmp_fact_sanity_check_results */
/*
select * from fact_sanity_check_results
where dd_sanity_check_name = 'Overall_metric_summary'
select distinct sanity_check_name from tmp_fact_sanity_check_results

truncate table fact_sanity_check_results
drop table tmp_fact_sanity_check_results 
*/

