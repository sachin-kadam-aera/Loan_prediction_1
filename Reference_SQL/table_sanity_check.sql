
/* *******************************************************************************************************   */
/*  Script : sanity_check.sql                                                                               */
/*  Description : Monthly Exxon Postproc Sanity Check script.                                              */
/*  Created On  : 23 Feb 2021                                                      	                      */
/*  Created By : Umesh                                                                                   */
/*********************************************************************************************************/


/* dropping the tmp table before using this */
drop table if exists tmp_fact_sanity_check_results;

/* Creating tables */
create table tmp_fact_sanity_check_results
(
    rptflag varchar(5),
    rptdate varchar(20),
    avg_mae decimal(18,2),
    median_mae decimal(18,2),
    avg_mape decimal(18,2),
    median_mape decimal(18,2),
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
    ct_avg_mae decimal(18,2),
    ct_median_mae decimal(18,2),
    ct_median_mape decimal(18,2),
    ct_avg_mape decimal(18,2),
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
insert into tmp_fact_sanity_check_results
(
    rptflag,
    rptdate,
    repodate_grain_count,
    avg_mape,
    median_mape,
    avg_mae,
    median_mae,
    cov,
    sanity_check_name
)
SELECT DD_LATESTREPORTING rptflag,to_date(dd_reportingdate,'DD MON YYYY') rptdate,
       count(DISTINCT dd_Grain1) cnt,
       round(avg(CT_MAPE),2) avg_mape,
       round(median(CT_MAPE),2) med_mape,
       round(avg(ct_mae),2) avg_mae,
       round(median(ct_mae),2) med_mae,
       round(avg(CT_COV_fcst),2) avg_cov,
       'Overall_Metric_Summary'
FROM FACT_FOSALESFORECAST
WHERE dd_forecastrank = 1
-- and dd_reportingdate in ('2021-03-01', '2021-02-01','2021-01-04','2020-12-01')
GROUP BY DD_LATESTREPORTING,to_date(dd_reportingdate,'DD MON YYYY')
ORDER BY to_date(dd_reportingdate,'DD MON YYYY') DESC
LIMIT 5;

/* Grain vs Algo count */
insert into tmp_fact_sanity_check_results
(
    forecasttype,
    method_grain_count,
    total_grain_count,
    sanity_check_name
)
WITH t1 as
(select dd_forecasttype,count(DISTINCT dd_Grain1) cnt
from  FACT_FOSALESFORECAST
where dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
GROUP BY DD_FORECASTTYPE order by cnt desc) ,
t2 AS(SELECT SUM(cnt) AS total_count
from
(
select dd_forecasttype, count(DISTINCT dd_Grain1) cnt
from  FACT_FOSALESFORECAST
where dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT t1.DD_FORECASTTYPE,t1.cnt,t2.total_count, 'Method_vs_Grains' FROM t1,t2;


select * from FACT_FOSALESFORECAST limit 10
/* Missing grain between sales and final forecast data */
insert into tmp_fact_sanity_check_results
(
    missing_grains ,
    sanity_check_name
)
select  count(distinct(DD_FORECASTGRAIN)) grains , 'Input_vs_Output_Grains' from   FACT_PREPROCESSED_MONTHLY_SALESHISTORY 
where DD_LATESTCORTEXINPUT_FLAG = 'Y' and DD_LATESTSNAPSHOT_FLAG = 'Y'
and (DD_FORECASTGRAIN) not in
(
select DISTINCT  dd_Grain1
from  FACT_FOSALESFORECAST
WHERE dd_reportingdate
in  ('2021-08-02')
  and dd_forecastrank = 1);

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
select dd_Grain1 grain, stddev(CT_FORECASTQUANTITY) std_forecast, dd_forecasttype
FROM  FACT_FOSALESFORECAST
WHERE dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
and  dd_forecasttype not in ('FALL_BACK_NAIVE')
and dd_forecastdate >=select to_char(trunc(ADD_MONTHS(current_date,1),'mon') -1,'YYYYMM')
group by dd_Grain1, dd_forecasttype
having stddev(CT_FORECASTQUANTITY) = 0)),
t2 AS(SELECT SUM(cnt) AS sum_of_count
from
(
select dd_forecasttype, count(DISTINCT dd_Grain1) cnt
from  FACT_FOSALESFORECAST
where dd_forecastrank = 1 
and dd_reportingdate =  select '2021-08-02'  ReportingDate 
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT ifnull(round(t1.cnt * 100/nullif(t2.sum_of_count,0),2),0) percent, 'percent_flat_Grains'  FROM t1,t2;

/* Percent of grain while zero flat forecast in horizon */
insert into tmp_fact_sanity_check_results
(
    percent_flat_Grains ,
    sanity_check_name
)
WITH t1 as
(select count(*) cnt 
from
(
select dd_Grain1 grain, sum(CT_SALESQUANTITY) sum_sales, sum(CT_FORECASTQUANTITY) sum_forecast, dd_forecasttype
FROM  FACT_FOSALESFORECAST
WHERE dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
and  dd_forecasttype not in ('FALL_BACK_NAIVE')
and dd_forecastdate >= select to_char(trunc(ADD_MONTHS(current_date,1),'mon') -1,'YYYYMM')
group by dd_Grain1, dd_forecasttype
having sum(CT_FORECASTQUANTITY) = 0 and sum(CT_SALESQUANTITY) > 1 )),
t2 AS(SELECT SUM(cnt) AS sum_of_count
from
(
select dd_forecasttype, count(DISTINCT dd_Grain1) cnt
from  FACT_FOSALESFORECAST
where dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
GROUP BY DD_FORECASTTYPE
ORDER BY COUNT(*) DESC 
))
SELECT ifnull(round(t1.cnt * 100/nullif(t2.sum_of_count,0),2),0) percent_grains, 'percent_zero_flat_grains' FROM t1,t2;


/* 75 percentile  */
insert into tmp_fact_sanity_check_results
(
    mape_75_percentile ,
    sanity_check_name
)
SELECT distinct (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ct_mape) 
		OVER (PARTITION BY dd_reportingdate)) mape, 'mape_75_percentile'
from  FACT_FOSALESFORECAST
where dd_forecastrank = 1
  and dd_reportingdate = '2021-08-02';


/* % of grains which have mape >100; if >25% then is problem (inflection forecast) */
insert into tmp_fact_sanity_check_results
(
    percent_of_mape,
    sanity_check_name
)
with t1 as (
select count(dd_Grain1) grain_mape_than100
from 
(
SELECT distinct  dd_Grain1, ct_mape
from  FACT_FOSALESFORECAST
where dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
having ct_mape>=100
)),
t2 as (select count(distinct dd_Grain1) total_grain_count
FROM  FACT_FOSALESFORECAST
WHERE dd_forecastrank = 1 
and dd_reportingdate = '2021-08-02'
)
SELECT ifnull(round(t1.grain_mape_than100 * 100 /NULLIF(t2.total_grain_count,0)),0) grains, 'grain_percent_of_mape100' FROM t1,t2;


/* New grains (between latest and last snapshot */
insert into tmp_fact_sanity_check_results
(
    current_vs_last_snapshot_Grains ,
    sanity_check_name
)
select count(DISTINCT  dd_Grain1) grains, 'current_vs_last_snap_Grains'
from  FACT_FOSALESFORECAST
WHERE dd_reportingdate
    in  (select current_date - interval '1' day)
  and dd_forecastrank = 1
  and dd_Grain1 not in
(
select DISTINCT  dd_Grain1
from  FACT_FOSALESFORECAST
WHERE dd_reportingdate
    in  (select (current_date - interval '1' month - interval '1' day))
  and dd_forecastrank = 1
    );

/* Grain where null value in date column*/
insert into tmp_fact_sanity_check_results
(
    null_date_grains,
    sanity_check_name
)
select count(DISTINCT (dd_Grain1)) grains, 'grain_in_null_value_date'
from  FACT_FOSALESFORECAST
WHERE dd_reportingdate
    in  (current_date - interval '1' day)
  and dd_forecastrank = 1
  and dd_forecastdate is null ;


/* deleting lastet snapshot date data */
delete * from fact_sanity_check_results
where dd_snapshot_date = to_date(current_date,'DD MON YYYY');

/* Set flag to No for old data */
update fact_sanity_check_results
set dd_latest_reporting_flag = 'No';

/* insert tmp table data into final sanity check fact table */
insert into fact_sanity_check_results
(
    fact_sanity_check_resultsID,
    dd_rptdate ,
    dd_rptflag ,
    dd_repodate_grain_count,
    ct_avg_mae ,
    ct_median_mae,
    ct_avg_mape,
    ct_median_mape,
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
        avg_mae,
        median_mae,

        avg_mape,
        median_mape,
        cov,
        to_date(current_date,'DD MON YYYY'),
        'Exxon',
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

