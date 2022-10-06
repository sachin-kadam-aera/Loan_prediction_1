open schema merck;

/*
DROP TABLE IF EXISTS stg_currentrunparam

CREATE TABLE stg_currentrunparam
(
dd_jobid varchar(100),
dd_reportingdate date
)

insert into  stg_currentrunparam
(
dd_jobid,
dd_reportingdate  --1st of the next month(e.g lag-1)
)
SELECT '41bb0804a73e44edb811595da1c27742',
to_date('202106','YYYYMM')  dd_reportingdate
*/

DROP TABLE IF EXISTS tmp_postproc_cleanupandrerank;
CREATE TABLE tmp_postproc_cleanupandrerank
AS
SELECT f.*,
d.dd_holdout_date dd_endoftrainingoutperiod,
d.dd_last_date dd_endofholdoutperiod,
CASE WHEN f.DD_ACTUALDATEVALUE <= d.dd_holdout_date THEN 'Train'
WHEN f.DD_ACTUALDATEVALUE <= d.dd_last_date THEN 'Test'
ELSE 'Horizon'
END dd_forecastsample,
c.dd_reportingdate dd_snapshotdate_dateformat
FROM fact_cortex_forecastoutput_data f
INNER JOIN dim_jobmetadata_cortex_data d
ON d.dd_jobid = f.dd_jobid, stg_currentrunparam c
WHERE d.dd_jobid = c.dd_jobid;

UPDATE tmp_postproc_cleanupandrerank
SET dd_grain1 = 'Not Set'
WHERE dd_grain1 IS NULL;

UPDATE tmp_postproc_cleanupandrerank
SET dd_grain2 = 'Not Set'
WHERE dd_grain2 IS NULL;

UPDATE tmp_postproc_cleanupandrerank
SET dd_grain3 = 'Not Set'
WHERE dd_grain3 IS NULL;

/* Cleanup algorithms that have problems in output */
DROP TABLE IF EXISTS tmp_sanitychecks_sales;
CREATE TABLE tmp_sanitychecks_sales
AS
SELECT dd_grain1,
dd_grain2,
dd_grain3,
DD_FORECASTALGORITHM,
min(DD_FORECASTDATE) min_DD_FORECASTDATE,
max(DD_FORECASTDATE) max_DD_FORECASTDATE,
count(*) cnt,
sum(CT_SALESQUANTITY) sum_ct_salesquantity,
avg(CT_SALESQUANTITY) avg_ct_salesquantity,
median(CT_SALESQUANTITY) median_ct_salesquantity,
min(CT_SALESQUANTITY) min_ct_salesquantity,
max(CT_SALESQUANTITY) max_ct_salesquantity,
stddev(CT_SALESQUANTITY) stdev_ct_salesquantity,
stddev(CT_SALESQUANTITY)/case when avg(CT_SALESQUANTITY) <= 0 THEN 1 ELSE avg(CT_SALESQUANTITY) END cov_ct_salesquantity
FROM tmp_postproc_cleanupandrerank
WHERE dd_forecastdate >= to_date(dd_endofholdoutperiod, 'YYYYMM') - INTERVAL '11' MONTH
AND dd_forecastdate <= to_date(dd_endofholdoutperiod, 'YYYYMM')
GROUP BY dd_grain1, dd_grain2, dd_grain3, DD_FORECASTALGORITHM;


DROP TABLE IF EXISTS tmp_sanitychecks_fcst;
CREATE TABLE tmp_sanitychecks_fcst
AS
SELECT dd_grain1, dd_grain2, dd_grain3,
DD_FORECASTALGORITHM,
min(DD_FORECASTDATE) min_DD_FORECASTDATE,
max(DD_FORECASTDATE) max_DD_FORECASTDATE,
count(*) cnt,
sum(ct_forecastquantity) sum_ct_forecastquantity,
avg(ct_forecastquantity) avg_ct_forecastquantity,
median(ct_forecastquantity) median_ct_forecastquantity,
min(ct_forecastquantity) min_ct_forecastquantity,
max(ct_forecastquantity) max_ct_forecastquantity,
stddev(ct_forecastquantity) stdev_ct_forecastquantity,
stddev(ct_forecastquantity)/case when avg(ct_forecastquantity) <= 0 THEN 1 ELSE avg(ct_forecastquantity) END cov_ct_forecastquantity
FROM tmp_postproc_cleanupandrerank
WHERE dd_forecastdate >= to_date(dd_endofholdoutperiod, 'YYYYMM') + INTERVAL '1' MONTH
AND dd_forecastdate <= to_date(dd_endofholdoutperiod, 'YYYYMM') + INTERVAL '12' MONTH
GROUP BY dd_grain1, dd_grain2, dd_grain3, DD_FORECASTALGORITHM;


DROP TABLE IF EXISTS tmp_forecastmethod_exceptions;
CREATE TABLE tmp_forecastmethod_exceptions
AS
SELECT f.dd_grain1, f.dd_grain2,f.dd_grain3, f.dd_forecastalgorithm, f.avg_ct_forecastquantity,
f.max_ct_forecastquantity,
s.median_ct_salesquantity,
s.avg_ct_salesquantity, s.max_ct_salesquantity
FROM tmp_sanitychecks_fcst f, tmp_sanitychecks_sales s
WHERE f.dd_grain1 = s.dd_grain1
AND f.dd_grain2 = s.dd_grain2
AND f.dd_grain3 = s.dd_grain3
AND f.dd_forecastalgorithm = s.dd_forecastalgorithm
AND f.max_ct_forecastquantity > 2 * max_ct_salesquantity;


DELETE FROM tmp_postproc_cleanupandrerank f
WHERE EXISTS ( SELECT 1 FROM tmp_forecastmethod_exceptions e
WHERE f.dd_grain1 = e.dd_grain1
AND f.dd_grain2 = e.dd_grain2
AND f.dd_grain3 = e.dd_grain3
AND f.dd_forecastalgorithm = e.dd_forecastalgorithm)
AND f.dd_forecastalgorithm NOT IN ('FALL_BACK_NAIVE');


DROP TABLE IF EXISTS tmp_rerankforecast;
CREATE TABLE tmp_rerankforecast
AS
SELECT dd_grain1,
dd_grain2,
dd_grain3,
DD_FORECASTALGORITHM,
sum(ct_salesquantity) ct_salesquantity,
avg(abs(CT_RANKINGMODE_FORECASTQUANTITY - ct_salesquantity)) ct_mae,
avg((CT_RANKINGMODE_FORECASTQUANTITY - ct_salesquantity)) ct_bias,
SQRT(AVG(POWER((CT_RANKINGMODE_FORECASTQUANTITY - ct_salesquantity),2))) ct_rmse,
RANK() OVER(PARTITION BY dd_grain1, dd_grain2, dd_grain3 ORDER BY
avg(abs(CT_RANKINGMODE_FORECASTQUANTITY - ct_salesquantity)),
avg((CT_RANKINGMODE_FORECASTQUANTITY - ct_salesquantity)),
SQRT(AVG(POWER((CT_RANKINGMODE_FORECASTQUANTITY - ct_salesquantity),2))),
DD_FORECASTALGORITHM) dd_forecastranknew
FROM tmp_postproc_cleanupandrerank
WHERE dd_forecastsample = 'Test'
AND dd_forecastalgorithm <> 'FALL_BACK_NAIVE'
GROUP BY 1,2,3,4
;




DROP TABLE IF EXISTS tmp_distinctgrains_rankedfcst;
CREATE TABLE tmp_distinctgrains_rankedfcst
AS
SELECT distinct dd_grain1, dd_grain2, dd_grain3
FROM tmp_rerankforecast;

UPDATE tmp_postproc_cleanupandrerank f
SET f.dd_forecastrank = -1;

UPDATE tmp_postproc_cleanupandrerank f
SET f.dd_forecastrank = t.dd_forecastranknew,
f.CT_RANKINGMETRICVALUE = t.ct_mae
FROM tmp_postproc_cleanupandrerank f, tmp_rerankforecast t
WHERE f.dd_grain1 = t.dd_grain1
AND f.dd_grain2 = t.dd_grain2
AND f.dd_grain3 = t.dd_grain3
AND f.dd_forecastalgorithm = t.dd_forecastalgorithm;


/* Grains that don't have any other algorithm should use Naive */
UPDATE tmp_postproc_cleanupandrerank f
SET f.dd_forecastrank = 1
FROM tmp_postproc_cleanupandrerank f
WHERE f.dd_forecastalgorithm = 'FALL_BACK_NAIVE'
AND NOT EXISTS ( SELECT 1 FROM tmp_distinctgrains_rankedfcst t
WHERE t.dd_grain1 = f.dd_grain1
AND t.dd_grain2 = f.dd_grain2
AND t.dd_grain3 = f.dd_grain3);