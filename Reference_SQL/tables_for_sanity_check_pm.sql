
/*********************************************************************************************************/
/*  Script : sanity_check.sql                                                                            */
/*  Description : Monthly EMDPM Postproc Sanity Check script.                                                   */
/*  Created On  :  23 Feb 2021                                                      	                 */
/*  Created By : Umesh                                                                                   */                                        */
/*********************************************************************************************************/





/* dropping the tmp table before using this */
drop table if exists tmp_sanity_check_results_pm;

/* Creating tables */
create table tmp_sanity_check_results_pm
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
create table fact_sanity_check_results_pm
(
fact_sanity_check_results_pmID int,
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



/* Overall metric summary check */
insert into tmp_sanity_check_results_pm
(
-- fact_sanity_check_results_pmID,
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
SELECT DD_LATESTREPORTING rptflag,to_date(dd_reportingdate,'DD MON YYYY') rptdate,count(DISTINCT dd_dmdunit||dd_loc||dd_businessunit) cnt,
       round(avg(ct_rmse),2) avg_rmse,
       round(median(ct_rmse),2) median_rmse,
       round(avg(CT_MAPE_NEW),2) avg_mape,
       round(median(CT_MAPE_NEW),2) median_mape,
       round(avg(CT_WAPE),2) avg_wape,
       round(median(CT_WAPE),2) median_wape,
       round(avg(CT_COV),2) COV,
       'Overall_metric_summary' name
FROM fact_fosalesforecast_pm
WHERE dd_forecastrank = 1
GROUP BY DD_LATESTREPORTING,to_date(dd_reportingdate,'DD MON YYYY')
ORDER BY to_date(dd_reportingdate,'DD MON YYYY') DESC
LIMIT 5;


/* Grain vs Algo count */
insert into tmp_sanity_check_results_pm
(
    forecasttype,
    method_grain_count,
    total_grain_count,
    sanity_check_name
)
WITH t1 as
(select dd_forecasttype,count(DISTINCT dd_dmdunit||dd_loc||dd_businessunit) cnt
from fact_fosalesforecast_pm
where dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))
GROUP BY DD_FORECASTTYPE order by cnt desc ) ,
t2 AS(SELECT SUM(cnt) AS total_count
from
(
select dd_forecasttype, count(DISTINCT dd_dmdunit||dd_loc||dd_businessunit) cnt
from fact_fosalesforecast_pm
where dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT t1.DD_FORECASTTYPE,t1.cnt,t2.total_count, 'Method_vs_Grains' FROM t1,t2;

/* Missing grain between sales and final forecast data */
insert into tmp_sanity_check_results_pm
(
    missing_grains ,
    sanity_check_name
)
select  count(distinct(dd_partnumber||dd_company||dd_plant)) missing_grains, 'Input_vs_Output_Grains' from  Fact_SALESHISTORY_Cortex
where (dd_partnumber||dd_company||dd_plant) not in 
( 
select DISTINCT concat(dd_dmdunit||dd_loc||dd_businessunit)
from fact_fosalesforecast_pm
WHERE dd_reportingdate 
in  (select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate )
and dd_forecastrank = 1
);

/* Percent of grains under flat value */
insert into tmp_sanity_check_results_pm
(
    percent_flat_Grains ,
    sanity_check_name
)
WITH t1 as
(select count(*) cnt 
from
(
select dd_dmdunit||dd_loc||dd_businessunit grain, stddev(CT_FORECASTQUANTITY) std_forecast, dd_forecasttype
FROM fact_fosalesforecast_pm
WHERE dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate 
and  dd_forecasttype not in ('FALL_BACK_NAIVE')
and dd_forecastdate >= select TO_CHAR(ADD_MONTHS(TO_DATE(current_date,'YYYYMM'),1)-1,'YYYYMMDD')
group by dd_dmdunit, dd_loc,dd_businessunit, dd_forecasttype
having stddev(CT_FORECASTQUANTITY) = 0)),
t2 AS(SELECT SUM(cnt) AS sum_of_count
from
(
select dd_forecasttype, count(DISTINCT dd_dmdunit||dd_loc||dd_businessunit) cnt
from fact_fosalesforecast_pm
where dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date ),'MON YYYY'))  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT ifnull(round(t1.cnt * 100/nullif(t2.sum_of_count,0),2),0) percent, 'percent_hz_flat_grains'  FROM t1,t2;

/* Percent of grain while zero flat forecast in horizon */
insert into tmp_sanity_check_results_pm
(
    percent_flat_Grains ,
    sanity_check_name
)
WITH t1 as
(select count(*) cnt 
from
(
select dd_dmdunit||dd_loc||dd_businessunit grain, sum(CT_SALESQUANTITY) sum_sales, sum(CT_FORECASTQUANTITY) sum_forecast, dd_forecasttype
FROM fact_fosalesforecast_pm
WHERE dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate 
and  dd_forecasttype not in ('FALL_BACK_NAIVE')
and dd_forecastdate >= select TO_CHAR(ADD_MONTHS(TO_DATE(current_date,'YYYYMM'),1)-1,'YYYYMMDD')
group by dd_dmdunit, dd_loc,dd_businessunit, dd_forecasttype
having sum(CT_FORECASTQUANTITY) = 0 and sum(CT_SALESQUANTITY) > 1)) ,
t2 AS(SELECT SUM(cnt2) AS sum_of_count
from
(
select dd_forecasttype, count(DISTINCT dd_dmdunit||dd_loc||dd_businessunit) cnt2
from fact_fosalesforecast_pm
where dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT ifnull(round(t1.cnt * 100/nullif(t2.sum_of_count,0),2 ),0) percent, 'percent_zero_flat_grains' FROM t1,t2;


/* 75 percentile  */
insert into tmp_sanity_check_results_pm
(
    mape_75_percentile ,
    sanity_check_name
)
SELECT distinct (PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY ct_mape_new) 
		OVER (PARTITION BY dd_reportingdate)) mape, 'mape_75_percentile'
from fact_fosalesforecast_pm
where dd_forecastrank = 1
  and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate ;


/* % of grains which have mape >100; if >25% then is problem (inflection forecast) */
insert into tmp_sanity_check_results_pm
(
    percent_of_mape,
    sanity_check_name
)
with t1 as (
select count(dd_dmdunit||dd_loc||dd_businessunit) grain_mape_than100
from 
(
SELECT distinct  dd_dmdunit, dd_loc,dd_businessunit, ct_mape_new
from fact_fosalesforecast_pm
where dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate 
having ct_mape_new>=1
)),
t2 as (select count(distinct dd_dmdunit||dd_loc||dd_businessunit) total_grain_count
FROM fact_fosalesforecast_pm
WHERE dd_forecastrank = 1 
and dd_reportingdate = select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate 
)
SELECT ifnull(round(t1.grain_mape_than100 * 100 /NULLIF(t2.total_grain_count,0)),0) grain_percent, 'grain_percent_of_mape100' FROM t1,t2;



/* New grains (between latest and last snapshot */
insert into tmp_sanity_check_results_pm
(
    current_vs_last_snapshot_Grains ,
    sanity_check_name
)
select count(DISTINCT concat(dd_dmdunit||dd_loc||dd_businessunit)) grains, 'current_vs_last_snap_Grains'
from fact_fosalesforecast_pm
WHERE dd_reportingdate
    in  (select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate )
  and dd_forecastrank = 1
  and (dd_dmdunit||dd_loc||dd_businessunit) not in 
( 
select DISTINCT concat(dd_dmdunit||dd_loc||dd_businessunit)
from fact_fosalesforecast_pm
WHERE dd_reportingdate 
in  ( select concat('01',' ', select to_char((current_date - interval '1' month),'MON YYYY'))  ReportingDate )
and dd_forecastrank = 1
);

/* Grain where null value in date column*/
insert into tmp_sanity_check_results_pm
(
    null_date_grains,
    sanity_check_name
)
select count(DISTINCT (dd_dmdunit||dd_loc||dd_businessunit)) grain, 'grain_where_null_value_date'
from fact_fosalesforecast_pm
WHERE dd_reportingdate
    in  ( select concat('01',' ', select to_char((current_date),'MON YYYY'))  ReportingDate )
  and dd_forecastrank = 1
  and dd_forecastdate is null ;

/* deleting lastet snapshot date data */
delete * from fact_sanity_check_results_pm
where dd_snapshot_date in to_date(current_date,'DD MON YYYY');

/* Set flag to No for old data */
update fact_sanity_check_results_pm
set latest_reporting_flag = 'No';

/* insert tmp table data into final sanity check fact table */
insert into fact_sanity_check_results_pm
(
    fact_sanity_check_results_pmID,
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
            ifnull(max(fact_sanity_check_results_pmID ), 0) from fact_sanity_check_results_pm m)+ row_number()
    over(order by '') as fact_sanity_check_results_pmID, --m.*
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
        'EMDPM',
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
from tmp_sanity_check_results_pm;



/* select * from tmp_sanity_check_results_pm */
/*
select * from fact_sanity_check_results_pm
where dd_sanity_check_name = 'Overall_metric_summary'
*/







