----- calculate WMAPE for rank1 and naive with old set of methods 
select round(sum(sum_of_abs_error)/sum(sum_of_sales),4) WMAPE
from
    (
     select dd_Grain1,dd_Grain2,dd_Grain3, sum(abs(CT_FORECASTQUANTITY - nullif(CT_SALESQUANTITY,1))) OVER(PARTITION BY dd_Grain1,dd_Grain2,dd_Grain3) sum_of_abs_error,
            sum(abs(CT_FORECASTQUANTITY - nullif(CT_SALESQUANTITY,1))) OVER(PARTITION BY dd_Grain1,dd_Grain2,dd_Grain3) sum_of_error,
            sum(nullif(CT_SALESQUANTITY,1))  OVER(PARTITION BY dd_Grain1,dd_Grain2,dd_Grain3)  sum_of_sales
     from  --- table name
     where dd_jobid = 'a68e9069aad6436a892a1daae9a0ddb7'
     --- and dd_Grain1 = '140754'
     --- and dd_Grain2 = 'RU20|RU1445|Not Set|Not Set|Not Set'
     --- and dd_Grain3 = 'ALL'
      and DD_ACTUALDATEVALUE  > '202202'
         and DD_ACTUALDATEVALUE <= '202203'
     and dd_forecastrank = 1
    )

--------- calculate WBIAS for rank1 and naive with old set of methods 

select round(sum(sum_of_error)/sum(sum_of_sales),4) WBISA
from
    (
     select dd_Grain1,dd_Grain2,dd_Grain3, sum(abs(CT_FORECASTQUANTITY - nullif(CT_SALESQUANTITY,1))) OVER(PARTITION BY dd_Grain1,dd_Grain2,dd_Grain3) sum_of_abs_error,
            sum(CT_FORECASTQUANTITY - nullif(CT_SALESQUANTITY,1)) OVER(PARTITION BY dd_Grain1,dd_Grain2,dd_Grain3) sum_of_error,
            sum(nullif(CT_SALESQUANTITY,1))  OVER(PARTITION BY dd_Grain1,dd_Grain2,dd_Grain3) sum_of_sales
     from --- table name
     where dd_jobid = 'a68e9069aad6436a892a1daae9a0ddb7'
      --- and DD_ACTUALDATEVALUE in ('202203','202204','202205')
        and DD_ACTUALDATEVALUE  > '202202'
         and DD_ACTUALDATEVALUE <= '202203'
         and dd_forecastrank = 1
        )
