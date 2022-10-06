
select * from temp_Safetystock_TestData_DEMAND_HISTORY limit 5
select * from fact_Safetystock_TestData_DEMAND_HISTORY limit 5

create table if not exists temp_Safetystock_TestData_DEMAND_HISTORY (
dd_ORDERID varchar(10),
dd_MATERIAL varchar(50),
dd_LOCATION varchar(50),
dd_DATE varchar(10),
ct_DEMAND decimal(18,0)
)

IMPORT INTO temp_Safetystock_TestData_DEMAND_HISTORY FROM LOCAL CSV FILE '/Users/sachinkadam/Safety_stock_work/SS_Reports/DEMAND_HISTORY_afterDataMasked.csv' 
ENCODING = 'UTF-8' 
COLUMN SEPARATOR = ',' 
COLUMN DELIMITER = '"' 
SKIP = 1 
REJECT LIMIT 0;


create table if not exists fact_Safetystock_TestData_DEMAND_HISTORY(
fact_Safetystock_TestData_DEMAND_HISTORYID decimal(36,6),
dd_ORDERID varchar(10),
dd_MATERIAL varchar(50),
dd_LOCATION varchar(50),
dd_DATE varchar(10),
ct_DEMAND decimal(18,0)
)

insert into fact_Safetystock_TestData_DEMAND_HISTORY
(   fact_Safetystock_TestData_DEMAND_HISTORYID,
    dd_ORDERID, 
	DD_MATERIAL, 
	DD_LOCATION, 
	dd_DATE,
    ct_DEMAND
)
select  (select ifnull(max(fact_Safetystock_TestData_DEMAND_HISTORYID),
                       0) from fact_Safetystock_TestData_DEMAND_HISTORY m)
+ row_number() over(order by '') as fact_Safetystock_TestData_DEMAND_HISTORYID,
    dd_ORDERID, 
	DD_MATERIAL, 
	DD_LOCATION, 
	dd_DATE,
    ct_DEMAND 
from temp_Safetystock_TestData_DEMAND_HISTORY;