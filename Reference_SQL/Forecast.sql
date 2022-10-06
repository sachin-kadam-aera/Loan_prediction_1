
select * from temp_Safetystock_TestData_FORECAST_SNAPSHOT limit 5
select * from fact_Safetystock_TestData_FORECAST_SNAPSHOT limit 5

create table if not exists temp_Safetystock_TestData_FORECAST_SNAPSHOT (
dd_MATERIAL varchar(50),
dd_LOCATION varchar(50),
dd_Forecast_Week varchar(10),
dd_Snapshot_Date varchar(10),
ct_Forecast_Quantity decimal(18,0)
)

IMPORT INTO temp_Safetystock_TestData_FORECAST_SNAPSHOT FROM LOCAL CSV FILE '/Users/sachinkadam/Safety_stock_work/SS_Reports/FORECAST_SNAPSHOT_afterDataMasked.csv' 
ENCODING = 'UTF-8' 
COLUMN SEPARATOR = ',' 
COLUMN DELIMITER = '"' 
SKIP = 1 
REJECT LIMIT 0;


create table if not exists fact_Safetystock_TestData_FORECAST_SNAPSHOT(
fact_Safetystock_TestData_FORECAST_SNAPSHOTID decimal(36,6),
dd_MATERIAL varchar(50),
dd_LOCATION varchar(50),
dd_Forecast_Week varchar(10),
dd_Snapshot_Date varchar(10),
ct_Forecast_Quantity decimal(18,0)
)

insert into fact_Safetystock_TestData_FORECAST_SNAPSHOT
(
    fact_Safetystock_TestData_FORECAST_SNAPSHOTID, 
	DD_MATERIAL, 
	DD_LOCATION, 
	dd_Forecast_Week,
    dd_Snapshot_Date,
    ct_Forecast_Quantity
)
select  (select ifnull(max(fact_Safetystock_TestData_FORECAST_SNAPSHOTID),
                       0) from fact_Safetystock_TestData_FORECAST_SNAPSHOT m)
+ row_number() over(order by '') as fact_Safetystock_TestData_FORECAST_SNAPSHOTID,
    DD_MATERIAL, 
	DD_LOCATION, 
	dd_Forecast_Week,
    dd_Snapshot_Date,
    ct_Forecast_Quantity
from temp_Safetystock_TestData_FORECAST_SNAPSHOT;
