
/* v1.0 : Script to extract sales history from merck AH table "merck.tmp_atlas_sales_GS" */
/* It does some pre-processing(e.g imputing 0's to have continuous time-series) and has some checks like min. no of data points */

DROP TABLE IF EXISTS tmp_stage_saleshistory_input_prd_gross;
CREATE TABLE tmp_stage_saleshistory_input_prd_gross
(
    dd_partnumber varchar(30),
    dd_level2_plant_comp_hei_ctrydest_mktgrp varchar(200),
    dd_companycode_dummy_ALL varchar(30),
    dd_yearmonth varchar(10),
    ct_salesqty decimal(18,2),
    flag varchar(30)
);

/* Get all historic data from merck.tmp_atlas_sales_GS */
/* Only retrieve data till the last date of the previous month */

CREATE OR REPLACE TABLE tmp_for_upd_from_shipping AS 
SELECT dp1.dim_partid, pl.DIM_PLANTID ,t.calendarmonthid, sum(t.ct_shippedQty) AS ct_shippedQty from
(select dpp.partnumber,
(CASE WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Austria' THEN 'AT40'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Germany' THEN 'DE40'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Switzerland' THEN 'CH20'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'France' THEN 'FR30'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Belgium' THEN 'BE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Luxembourg' THEN 'BE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Netherlands' THEN 'NL20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Czech Republic' THEN 'CZ20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Hungary' THEN 'HU20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Poland' THEN 'PL20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Romania' THEN 'RO20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Slovakia' THEN 'CZ20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Portugal' THEN 'PT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Spain' THEN 'ES20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Denmark' THEN 'DK20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Finland' THEN 'FI20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Iceland' THEN 'DK20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Norway' THEN 'NO20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Sweden' THEN 'SE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Croatia' THEN 'AT30'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Estonia' THEN 'LT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Latvia' THEN 'LT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Lithuania' THEN 'LT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Slovenia' THEN 'AT30'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Cyprus' THEN 'GR20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Greece' THEN 'GR20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Ireland' THEN 'IE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Italy' THEN 'IT20'
ELSE dp.PLANTCODE END) AS plantcode,
d1.calendarmonthid,
(CASE WHEN s.Dim_DateidActualGI_Original <> 1 THEN s.ct_QtyDeliveredBaseUoMIRU ELSE 0 END) AS ct_shippedQty
from fact_salesorderdelivery  s
inner join dim_date d1 on s.dim_dateidactualgoodsissue=d1.dim_dateid
INNER JOIN dim_deliverytype dd ON dd.DIM_DELIVERYTYPEID = s.DIM_DELIVERYTYPEID AND dd.DELIVERYTYPE NOT IN ('DIG','DTR','EG','EL','HID','HTP','JF','LFKO','LR','NCR','NK','NKR','NL','NLCC','NLR','RL','RLL','SRCL','SRNP','SRUP','UL','VNL','WID','WIG','WMPP','WNL','WRD','WTR','ZNL','ZR','Not Set')
INNER JOIN dim_plant dp on dp.DIM_PLANTID  = s.DIM_PLANTID 
INNER JOIN dim_part dpp ON dpp.dim_partid = s.dim_partid
inner JOIN dim_customer dc on dc.DIM_CUSTOMERID = s.DIM_CUSTOMERIDSHIPTO
where s.dim_salesdocumenttypeid not in (select dim_salesdocumenttypeid from dim_salesdocumenttype where documenttype = 'RE')
AND d1.calendarmonthid <> 0
AND s.dim_plantidordering = 1
AND s.dim_plantid NOT IN (SELECT dim_plantid FROM dim_plant WHERE plantcode = 'GB20')
AND d1.CALENDARMONTHID < to_number(to_char(CURRENT_DATE,'YYYYMM'))
) t 
INNER JOIN dim_plant pl ON pl.plantcode = t.plantcode
INNER JOIN dim_part dp1 ON dp1.partnumber = t.partnumber AND dp1.plant = t.plantcode
GROUP BY dp1.dim_partid, pl.DIM_PLANTID ,t.calendarmonthid 
UNION 
SELECT dp1.dim_partid, pl.DIM_PLANTID ,t.calendarmonthid, sum(t.ct_shippedQty) AS ct_shippedQty from
(select dpp.partnumber,
(CASE WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Austria' THEN 'AT40'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Germany' THEN 'DE40'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Switzerland' THEN 'CH20'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'France' THEN 'FR30'
      WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Belgium' THEN 'BE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Luxembourg' THEN 'BE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Netherlands' THEN 'NL20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Czech Republic' THEN 'CZ20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Hungary' THEN 'HU20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Poland' THEN 'PL20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Romania' THEN 'RO20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Slovakia' THEN 'CZ20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Portugal' THEN 'PT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Spain' THEN 'ES20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Denmark' THEN 'DK20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Finland' THEN 'FI20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Iceland' THEN 'DK20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Norway' THEN 'NO20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Sweden' THEN 'SE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Croatia' THEN 'AT30'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Estonia' THEN 'LT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Latvia' THEN 'LT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Lithuania' THEN 'LT20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Slovenia' THEN 'AT30'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Cyprus' THEN 'GR20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Greece' THEN 'GR20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Ireland' THEN 'IE20'
WHEN dp.PLANTCODE ='1NLA' AND dc.COUNTRYNAME = 'Italy' THEN 'IT20'
ELSE dp.PLANTCODE END) AS plantcode,
d1.calendarmonthid,
(CASE WHEN s.Dim_DateidActualGI_Original <> 1 THEN s.ct_QtyDeliveredBaseUoMIRU ELSE 0 END) AS ct_shippedQty
from fact_salesorderdelivery  s
inner join dim_date d1 on s.dim_dateidactualgoodsissue=d1.dim_dateid
INNER JOIN dim_deliverytype dd ON dd.DIM_DELIVERYTYPEID = s.DIM_DELIVERYTYPEID AND dd.DELIVERYTYPE NOT IN ('DIG','DTR','EG','EL','HID','HTP','JF','LFKO','LR','NCR','NK','NKR','NL','NLCC','NLR','RL','RLL','SRCL','SRNP','SRUP','UL','VNL','WID','WIG','WMPP','WNL','WRD','WTR','ZNL','ZR','Not Set')
INNER JOIN dim_plant dp on dp.DIM_PLANTID  = s.DIM_PLANTID 
INNER JOIN dim_part dpp ON dpp.dim_partid = s.dim_partid
inner JOIN dim_customer dc on dc.DIM_CUSTOMERID = s.DIM_CUSTOMERIDSHIPTO
where s.dim_salesdocumenttypeid not in (select dim_salesdocumenttypeid from dim_salesdocumenttype where documenttype = 'RE')
AND d1.calendarmonthid <> 0
AND s.dim_plantidordering = 1
AND s.dim_plantid IN (SELECT dim_plantid FROM dim_plant WHERE plantcode = 'GB20')
AND d1.CALENDARMONTHID >= 202101
AND d1.CALENDARMONTHID < to_number(to_char(CURRENT_DATE,'YYYYMM'))
) t 
INNER JOIN dim_plant pl ON pl.plantcode = t.plantcode
INNER JOIN dim_part dp1 ON dp1.partnumber = t.partnumber AND dp1.plant = t.plantcode
GROUP BY dp1.dim_partid, pl.DIM_PLANTID ,t.calendarmonthid 
UNION
select dp.dim_partid,dpl.dim_plantid, to_number(uk.salesmonth) as calendarmonthid, total_quantity as ct_shippedqty
from uk_inmarket_sales uk
inner join dim_part dp on dp.partnumber = uk.uin and uk.plant_code = dp.plant
inner join dim_plant dpl on dpl.plantcode = uk.plant_code
WHERE to_number(uk.salesmonth) < 202011;

CREATE OR REPLACE TABLE tmp_for_salesmtd_from_shipping as
select distinct s.dim_partid,s.dim_plantid,d1.calendarmonthid,sum((CASE WHEN s.Dim_DateidActualGI_Original <> 1 THEN s.ct_QtyDeliveredBaseUoMIRU ELSE 0 END)) AS ct_shippedQty
from fact_salesorderdelivery  s
inner join dim_date d1 on s.dim_dateidactualgoodsissue=d1.dim_dateid
INNER JOIN dim_deliverytype dd ON dd.DIM_DELIVERYTYPEID = s.DIM_DELIVERYTYPEID AND dd.DELIVERYTYPE NOT IN ('DIG','DTR','EG','EL','HID','HTP','JF','LFKO','LR','NCR','NK','NKR','NL','NLCC','NLR','RL','RLL','SRCL','SRNP','SRUP','UL','VNL','WID','WIG','WMPP','WNL','WRD','WTR','ZNL','ZR','Not Set')
where s.dim_salesdocumenttypeid not in (select dim_salesdocumenttypeid from dim_salesdocumenttype where documenttype = 'RE')
AND d1.calendarmonthid <> 0
AND s.dim_plantidordering = 1
AND s.dim_plantid NOT IN (SELECT dim_plantid FROM dim_plant WHERE plantcode = 'GB20')
AND d1.CALENDARMONTHID <= to_number(to_char(CURRENT_DATE,'YYYYMM'))
GROUP BY s.dim_partid,s.dim_plantid,d1.calendarmonthid
UNION
select distinct s.dim_partid,s.dim_plantid,d1.calendarmonthid,sum((CASE WHEN s.Dim_DateidActualGI_Original <> 1 THEN s.ct_QtyDeliveredBaseUoMIRU ELSE 0 END)) AS ct_shippedQty
from fact_salesorderdelivery  s
inner join dim_date d1 on s.dim_dateidactualgoodsissue=d1.dim_dateid
INNER JOIN dim_deliverytype dd ON dd.DIM_DELIVERYTYPEID = s.DIM_DELIVERYTYPEID AND dd.DELIVERYTYPE NOT IN ('DIG','DTR','EG','EL','HID','HTP','JF','LFKO','LR','NCR','NK','NKR','NL','NLCC','NLR','RL','RLL','SRCL','SRNP','SRUP','UL','VNL','WID','WIG','WMPP','WNL','WRD','WTR','ZNL','ZR','Not Set')
where s.dim_salesdocumenttypeid not in (select dim_salesdocumenttypeid from dim_salesdocumenttype where documenttype = 'RE')
AND d1.calendarmonthid <> 0
AND s.dim_plantidordering = 1
AND s.dim_plantid IN (SELECT dim_plantid FROM dim_plant WHERE plantcode = 'GB20')
AND d1.CALENDARMONTHID >= 202101
AND d1.CALENDARMONTHID <= to_number(to_char(CURRENT_DATE,'YYYYMM'))
GROUP BY s.dim_partid,s.dim_plantid,d1.calendarmonthid
UNION
select distinct dp.dim_partid,dpl.dim_plantid, to_number(uk.salesmonth) as calendarmonthid, total_quantity as ct_shippedqty
from uk_inmarket_sales uk
inner join dim_part dp on dp.partnumber = uk.uin and uk.plant_code = dp.plant
inner join dim_plant dpl on dpl.plantcode = uk.plant_code
WHERE to_number(uk.salesmonth) < 202011;

CREATE OR REPLACE TABLE tmp_atlas_Sales_gs_gross as
SELECT sales_uin,
sales_reporting_period,
sales_cocd, 
Reporting_company_code,
hei_code,
Country_destination_Code,
Market_grouping,
region,
sum(BUOM_QUantity) BUOM_QUantity,
'original' AS flag
FROM 
(SELECT DISTINCT ifnull(c.new_uin, ifnull(b.new_uin,DD_PARTNUMBER)) as sales_uin,D.CALENDARMONTHID sales_reporting_period,
ifnull(c.new_plant_code, ifnull(b.new_plant_code,DD_PLANTCODE)) as sales_cocd, 
(s.ct_shippedQty) BUOM_QUantity,
ifnull(c.New_Reporting_Company_Code, ifnull(b.New_Reporting_Company_Code,f.dd_Reporting_company_code)) as Reporting_company_code,
'Not Set' hei_code,
'Not Set' Country_destination_Code,
'Not Set' Market_grouping,
f.DD_REGION region ,
max(case when c.new_uin is not null then 'level 2' 
  when b.new_uin is not null then 'level 1' 
    else 'original' end) as flag
FROM 
FACT_FOSALESFORECASTCORTEX F
INNER JOIN dim_date d ON f.DIM_DATEIDFORECAST = d.DIM_DATEID
INNER JOIN tmp_for_upd_from_shipping s
 ON f.DIM_PARTID = s.dim_partid
 AND f.DIM_PLANTID = s.dim_plantid
 AND d.calendarmonthid = s.calendarmonthid
left join  UIN_Historical_Mappings b ON f.DD_PARTNUMBER=b.old_uin
AND f.DD_PLANTCODE=b.plant_code
and f.dd_Reporting_company_code = b.Old_Reporting_Company_Code
left join  UIN_Historical_Mappings c on c.old_uin=b.new_uin
and b.new_plant_code=c.plant_code
and b.New_Reporting_Company_Code = c.Old_Reporting_Company_Code
WHERE f.dd_latestreporting = 'Yes' AND f.dd_forecastrank = 1
Group by ifnull(c.new_uin, ifnull(b.new_uin,DD_PARTNUMBER)), D.CALENDARMONTHID, ifnull(c.new_plant_code, ifnull(b.new_plant_code,DD_PLANTCODE)), ifnull(c.New_Reporting_Company_Code, ifnull(b.New_Reporting_Company_Code,dd_Reporting_company_code)),/* Market_grouping,*/dd_Region,s.ct_shippedQty
) T 
GROUP BY sales_uin,
sales_reporting_period,
sales_cocd, 
Reporting_company_code,
hei_code,
Country_destination_Code,
Market_grouping,
region;
/* OZ BI-5395: End Greg preprocess part */

INSERT INTO tmp_stage_saleshistory_input_prd_gross
(
dd_partnumber,
dd_level2_plant_comp_hei_ctrydest_mktgrp,
dd_companycode_dummy_ALL,
dd_yearmonth,
ct_salesqty,
flag
)
SELECT SALES_UIN dd_partnumber,
SALES_COCD || '|' || REPORTING_COMPANY_CODE || '|' || HEI_CODE || '|' || COUNTRY_DESTINATION_CODE || '|' || MARKET_GROUPING dd_level2_plant_comp_hei_ctrydest_mktgrp,
'ALL' dd_companycode_dummy_ALL,
SALES_REPORTING_PERIOD dd_yearmonth,
(BUOM_QUANTITY) ct_salesqty,
flag
from merck.tmp_atlas_Sales_gs_gross
WHERE SALES_REPORTING_PERIOD is not null and SALES_UIN is not null
AND SALES_REPORTING_PERIOD <= to_char(current_date-interval '1' month,'YYYYMM');

DROP TABLE IF EXISTS tmp_bckp_merckah_saleshist_gross;
CREATE TABLE tmp_bckp_merckah_saleshist_gross
AS
SELECT dd_partnumber dd_partnumber,
dd_level2_plant_comp_hei_ctrydest_mktgrp comop,
'ALL' companycode,
cast(dd_yearmonth as int) YYYYMM,
sum(ct_salesqty) Sales,
flag
FROM tmp_stage_saleshistory_input_prd_gross
group by dd_partnumber , dd_level2_plant_comp_hei_ctrydest_mktgrp,cast(dd_yearmonth as int),flag;

----------------------------------------------------------------------
/* Check the numbers in Source table */
/*select  sales_reporting_period, sum(buom_quantity) sales , count(distinct sales_uin) cnt_of_parts,
from MERCK.ATLAS_FORECAST_SALES_MERCK_DC
group by sales_reporting_period
order by sales_reporting_period desc*/

/* Check the numbers in input data table */
/*select YYYYMM, sum(sales) sales, count(distinct dd_partnumber) cnt_of_parts,count(distinct dd_partnumber||comop)  cnt_of_grain
from tmp_bckp_merckah_saleshist_gross
group by YYYYMM
order by YYYYMM desc*/
-----------------------------------------------------------------------

/* Check the latest month with non-zero sale */
DROP TABLE IF EXISTS tmp_cnt_min_saleshist_gross;
CREATE TABLE tmp_cnt_min_saleshist_gross
AS
SELECT dd_partnumber,comop,count(*) cnt,max(to_number(yyyymm)) max_yyyymm
from tmp_bckp_merckah_saleshist_gross
WHERE Sales > 0
GROUP BY dd_partnumber,comop;

/* There should be atleast one non-zero sale month in last 3 months */
/* This may be commented out if needed for the ad-hoc runs as they are all in the past and some of those parts may not have sales in recent 3 months(though they will have sales in respective holdout period)   */
DELETE FROM tmp_bckp_merckah_saleshist_gross f
WHERE EXISTS ( select 1 from tmp_cnt_min_saleshist_gross m where m.dd_partnumber = f.dd_partnumber and m.comop = f.comop and max_yyyymm not in (to_char(current_date-interval '1' month,'YYYYMM'),to_char(current_date-interval '2' month,'YYYYMM'),to_char(current_date-interval '3' month,'YYYYMM')));

/* Remove grain(e.g part+comop) that have all zero's */
DROP TABLE IF EXISTS tmp_cnt_min_saleshist_gross;
CREATE TABLE tmp_cnt_min_saleshist_gross
AS
SELECT dd_partnumber,comop,max(Sales) max_sales
from tmp_bckp_merckah_saleshist_gross
GROUP BY dd_partnumber,comop;

DELETE FROM tmp_bckp_merckah_saleshist_gross f
WHERE EXISTS ( select 1 from tmp_cnt_min_saleshist_gross m where m.dd_partnumber = f.dd_partnumber and m.comop = f.comop and max_sales = 0);

/* Impute 0 sales in missing months */

/* Get the min AND max Sched Dlvry Date */
drop table if exists tmp_saleshistory_daterange_gross;
create table tmp_saleshistory_daterange_gross as
select max(YYYYMM) maxdate, min(YYYYMM) mindate
from tmp_bckp_merckah_saleshist_gross fs1;

drop table if exists tmp_saleshistory_yyyymm_gross;
create table tmp_saleshistory_yyyymm_gross as
select distinct calendarmonthid --, datevalue
from dim_date, tmp_saleshistory_daterange_gross
WHERE calendarmonthid between mindate AND maxdate
order by calendarmonthid;


/* Insert zero for all intermediate months */
INSERT INTO tmp_bckp_merckah_saleshist_gross
SELECT DISTINCT dd_partnumber,comop,companycode,t.calendarmonthid YYYYMM,0,'original' as flag
FROM tmp_saleshistory_yyyymm_gross t,
(SELECT DISTINCT dd_partnumber,comop,companycode from tmp_bckp_merckah_saleshist_gross) s
WHERE NOT EXISTS ( SELECT 1 FROM tmp_bckp_merckah_saleshist_gross s2 where s2.dd_partnumber = s.dd_partnumber AND s2.comop = s.comop AND s2.companycode = s.companycode AND s2.YYYYMM = t.calendarmonthid);

/* Delete all zero sales before the first positive sale */

DROP TABLE IF EXISTS tmp_bckp_merckah_saleshist_gross1;
CREATE TABLE tmp_bckp_merckah_saleshist_gross1
AS
SELECT dd_partnumber,comop,min(YYYYMM) yyyymm
FROM tmp_bckp_merckah_saleshist_gross
WHERE Sales > 0
group by dd_partnumber,comop;

/* Delete leading 0's e.g all 0's before the first month with +ve sales qty */
DELETE FROM tmp_bckp_merckah_saleshist_gross f
WHERE EXISTS ( SELECT 1 FROM tmp_bckp_merckah_saleshist_gross1 f2 WHERE f.dd_partnumber = f2.dd_partnumber AND f.comop = f2.comop AND f.yyyymm < f2.yyyymm);

/* Delete parts that have all zeros */
DELETE FROM tmp_bckp_merckah_saleshist_gross f
WHERE NOT EXISTS ( SELECT 1 FROM tmp_bckp_merckah_saleshist_gross1 f2 WHERE f.dd_partnumber = f2.dd_partnumber AND f.comop = f2.comop );


/* Check for min data points */
DROP TABLE IF EXISTS tmp_cnt_min_saleshist_gross;
CREATE TABLE tmp_cnt_min_saleshist_gross
AS
SELECT dd_partnumber,comop,count(*) cnt,max(to_number(yyyymm)) max_yyyymm
from tmp_bckp_merckah_saleshist_gross
GROUP BY dd_partnumber,comop;

/* Delete part if there are less than 22 non-zero data points in the time-series(This assumes 10-month holdout.
Change this to 18 if you need 6-month holdout)*/
DELETE FROM tmp_bckp_merckah_saleshist_gross f
WHERE EXISTS ( select 1 from tmp_cnt_min_saleshist_gross m where m.dd_partnumber = f.dd_partnumber and m.comop = f.comop and cnt <= 22);

DROP TABLE IF EXISTS saleshistory_fromprd_dfsubjarea_3MHO_IDP20_GROSS;
CREATE TABLE saleshistory_fromprd_dfsubjarea_3MHO_IDP20_GROSS
AS
SELECT *
FROM tmp_bckp_merckah_saleshist_gross 
ORDER BY dd_partnumber,YYYYMM;

DROP TABLE IF EXISTS saleshistory_fromprd_dfsubjarea_3MHO;
CREATE TABLE saleshistory_fromprd_dfsubjarea_3MHO
AS
SELECT t.*, 
       t.sales    as sales_original,
       cast('Not Set' as varchar(100)) as dd_event_category
FROM tmp_bckp_merckah_saleshist_gross t
ORDER BY dd_partnumber,YYYYMM;

/* Vali - set sales value to ct_transformedsales, dd_event_category */
MERGE INTO saleshistory_fromprd_dfsubjarea_3MHO stg
USING (SELECT
             distinct 
             dd_partnumber,
             dd_plant,
             dd_company,
             dd_calendarmonthid,
             ct_transformedsales,
             dd_event_category
      FROM  fact_SALESHISTORY_Transformedsales_cortex c
      where c.dd_reportingdate = (select max(dd_reportingdate) 
                                  from fact_SALESHISTORY_Transformedsales_cortex)
      and ( nvl(c.ct_transformedsales,0) <> nvl(c.ct_salesquantity,0)
            and  nvl(c.ct_transformedsales,0) <> 0 
           or nvl(c.dd_event_category, 'Not Set') <>  'Not Set' )) f
ON  stg.dd_partnumber = f.dd_partnumber
AND stg.comop = f.dd_plant
AND stg.companycode = f.dd_company
AND stg.yyyymm = f.dd_calendarmonthid
WHEN MATCHED THEN 
 UPDATE
   SET stg.sales = f.ct_transformedsales,
       stg.dd_event_category = f.dd_event_category
 WHERE nvl(stg.sales, 0) <>  nvl(f.ct_transformedsales, 0)
   OR nvl(stg.dd_event_category, 'Not Set') <>  nvl(f.dd_event_category, 'Not Set');

/* Export data to a csv file. This csv file can be consumed by the forecasting scripts on forecast server */
/* This should have 5 comma-separated columns. part-plant-companycode-yyyymm-salesqty
EXPORT (SELECT * FROM merck.saleshistory_fromprd_dfsubjarea_3MHO_GROSS ORDER BY DD_PARTNUMBER,COMOP,COMPANYCODE,YYYYMM)
INTO LOCAL CSV FILE '<path>' COLUMN SEPARATOR = ','
*/

DROP TABLE IF EXISTS tmp_stage_saleshistory_input_prd_gross;
DROP TABLE IF EXISTS tmp_bckp_merckah_saleshist_gross;
DROP TABLE IF EXISTS tmp_cnt_min_saleshist_gross;
drop table if exists tmp_saleshistory_daterange_gross;
drop table if exists tmp_saleshistory_yyyymm_gross;
DROP TABLE IF EXISTS tmp_bckp_merckah_saleshist_gross1;

truncate table fact_SALESHISTORY_cortex;

insert into fact_SALESHISTORY_cortex
(
fact_SALESHISTORY_cortexid  ,
    DD_PARTNUMBER     ,
    DD_PLANT,
DD_COMPANY,
DD_CALENDARMONTHID,
CT_SALESQUANTITY
)
select  (
select
ifnull(max(fact_SALESHISTORY_cortexid ), 0) from fact_SALESHISTORY_cortex m)+ row_number() over(order by '') as fact_SALESHISTORY_cortexID,
DD_PARTNUMBER     ,
    COMOP         ,
    COMPANYCODE       ,
    YYYYMM ,
    SALES
from saleshistory_fromprd_dfsubjarea_3MHO;
