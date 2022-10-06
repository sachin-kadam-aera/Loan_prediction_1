

select * from temp_ImputedInspection_PLIndia_Noida_Combined
select * from fact_ImputedInspection_PLIndia_Noida_Combined

create table if not exists temp_ImputedInspection_PLIndia_Noida_Combined(
dd_ProductLine varchar(100),
dd_SBU varchar(50),
dd_Operating_Office varchar(50),
dd_Year_Month varchar(10),
ct_Sum_Of_Mandays decimal(18,6)
);

IMPORT INTO temp_ImputedInspection_PLIndia_Noida_Combined FROM LOCAL CSV FILE '/Users/sachinkadam/BV/_Filled_Updated_Inspection_PL_OO_India_Noida_Combined.csv' 
ENCODING = 'UTF-8' 
COLUMN SEPARATOR = ',' 
COLUMN DELIMITER = '"' 
SKIP = 1 
REJECT LIMIT 0;

create table if not exists fact_ImputedInspection_PLIndia_Noida_Combined(
fact_ImputedInspection_PLIndia_Noida_CombinedID decimal(36,6),
dd_ProductLine varchar(100),
dd_SBU varchar(50),
dd_Operating_Office varchar(50),
dd_Year_Month varchar(10),
ct_Sum_Of_Mandays decimal(18,6)
)

insert into fact_ImputedInspection_PLIndia_Noida_Combined
(   FACT_IMPUTEDINSPECTION_PLINDIA_NOIDA_COMBINEDID, 
	DD_PRODUCTLINE, 
	DD_SBU, 
	DD_OPERATING_OFFICE, 
	DD_YEAR_MONTH, 
	CT_SUM_OF_MANDAYS
)
select  (select ifnull(max(fact_ImputedInspection_PLIndia_Noida_CombinedID),
                       0) from fact_ImputedInspection_PLIndia_Noida_Combined m)
+ row_number() over(order by '') as fact_ImputedInspection_PLIndia_Noida_CombinedID, 
	DD_PRODUCTLINE, 
	DD_SBU, 
	DD_OPERATING_OFFICE, 
	DD_YEAR_MONTH, 
	CT_SUM_OF_MANDAYS
from temp_ImputedInspection_PLIndia_Noida_Combined;

Imputed Inspection PL India Noida Combined 
f_iplo


