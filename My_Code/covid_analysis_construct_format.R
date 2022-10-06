rm(list = ls())

############ Libraries ############
library(data.table)
library(lubridate)
library(dplyr)
library(tidyr)
library(forecast)
library(dummies)
library(futile.logger)
flog.appender(appender.file('logs.log'))
flog.threshold(DEBUG)

# Read data



# new_cases_significant_pos_impact = unique(corr_analysis$DD_GRAIN[corr_analysis$new_cases_flag==1])
# share_of_significant_pos_impact = sum(abc_maping[DD_GRAIN %in% new_cases_significant_pos_impact,CT_VOLUME_SHARE])
# cat('New Cases: number of grains with significant +ve correlation =>',length(new_cases_significant_pos_impact),'\n')
# cat('Share of these grains =>',share_of_significant_pos_impact,'\n')

#fwrite(corr_analysis,'/Users/sandeepdhankhar/OneDrive - Aera Technology/23 Pulmuone/3 Data/5 COVID Analysis/1 correlation_op.csv')
#fwrite(corr_analysis,'/Users/sandeepdhankhar/OneDrive - Aera Technology/23 Pulmuone/3 Data/5 COVID Analysis/2 correlation_op_long_period.csv')
#fwrite(corr_analysis,'/Users/sandeepdhankhar/OneDrive - Aera Technology/23 Pulmuone/3 Data/5 COVID Analysis/3 correlation_op_outlier_treated.csv')

########################## 1 Pre-processing functions ##########################
top_products = function(data,korean_channel_maping,start_date,end_date){
  
  ######################### Convert Korean to English #########################
  setnames(data,c("Channel","Material Number","Week Starting","Order Item Quantity"),
           c('CUSTOMER_NEW_CHANNEL','INVENTORY_ITEM_ID','Week Starting','ORDERED_QUANTITY'))
  data = merge(data,korean_channel_maping)
  
  ######################### Limit dataset #########################
  #1. Keep limited time period
  # start_date = '2016-10-03'
  # end_date = '2021-10-04'
  data = data[(`Week Starting` >= start_date) & (`Week Starting` <= end_date)]
  data = data[order(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID,`Week Starting`)] #order
  
  #cat('Total Channels ', length(unique(data$CUSTOMER_NEW_CHANNEL_ENG)),'\n')
  #cat('Total SKUs ',length(unique(data$INVENTORY_ITEM_ID)),'\n')
  #cat('Total Channel SKU combination ',nrow(unique(data[,.(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID)])),'\n')
  
  #2. Keep Grains with non-zero sales in past 12 months - 52 weeks & atleast 2 non-zero sales week
  twelve_mth_end_date = max(data$`Week Starting`)
  twelve_mth_start_date = twelve_mth_end_date - 12*31
  last_52_weeks = data[,tail(.SD,52),by=.(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID)]
  latest_twelve_mths_data = data[(`Week Starting` >= twelve_mth_start_date) & (`Week Starting` <= twelve_mth_end_date)]
  latest_twelve_mths_data[,Non_zero_sales := ifelse(ORDERED_QUANTITY > 0,1,0)] #non-zero sales
  latest_twelve_mths_data = latest_twelve_mths_data[,.(T_Sales = sum(ORDERED_QUANTITY),
                                                       Non_zero_weeks = sum(Non_zero_sales)),
                                                    by=.(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID)]
  active_grains_in_latest_twelve_mths = latest_twelve_mths_data[(T_Sales != 0) & (Non_zero_weeks > 1)]
  grains_with_1_sales_in_12_mths = latest_twelve_mths_data[(T_Sales > 0) & (Non_zero_weeks == 1)] # for later
  
  #cat('Total Channel SKU combination ',nrow(active_grains_in_latest_twelve_mths),'\n')
  
  # Update data
  data_with_1_sales_in_12_mths = merge(data,grains_with_1_sales_in_12_mths,by = c('CUSTOMER_NEW_CHANNEL_ENG','INVENTORY_ITEM_ID'))
  data = merge(data,active_grains_in_latest_twelve_mths,by = c('CUSTOMER_NEW_CHANNEL_ENG','INVENTORY_ITEM_ID'))
  
  #3. Keep top Channel-SKU by sales in last 2 years
  two_yrs_end_date = max(data$`Week Starting`)
  two_yrs_start_date = two_yrs_end_date - 24*31
  latest_2_years = data[(`Week Starting` >= two_yrs_start_date) & (`Week Starting` <= two_yrs_end_date)]
  agg_sales_2_yrs = latest_2_years[,.(T_Sales = sum(ORDERED_QUANTITY)),by=.(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID)]
  
  # Keep top 95% portfolio by volume
  agg_sales_2_yrs = agg_sales_2_yrs[order(T_Sales,decreasing = T)]
  agg_sales_2_yrs[,Vol_share := T_Sales*100/sum(agg_sales_2_yrs$T_Sales)]
  agg_sales_2_yrs[,Cum_share := cumsum(Vol_share)]
  agg_sales_2_yrs_95 = agg_sales_2_yrs[Cum_share <= 95] #95% filter
  
  #cat('Number of grains in top 95% portfolio => ',nrow(agg_sales_2_yrs_95),'\n')
  #cat('Number of grains in 100% portfolio => ',nrow(agg_sales_2_yrs),'\n')
  
  data_top_95_pc_portfolio = merge(data,agg_sales_2_yrs_95,by=c('CUSTOMER_NEW_CHANNEL_ENG','INVENTORY_ITEM_ID'))
  data_top_95_pc_portfolio = data_top_95_pc_portfolio[,.(CUSTOMER_NEW_CHANNEL,CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID,
                                                         `Week Starting`,ORDERED_QUANTITY)]
  data_top_95_pc_portfolio = data_top_95_pc_portfolio[order(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID,`Week Starting`)] #order
  top_products = unique(data_top_95_pc_portfolio[,.(CUSTOMER_NEW_CHANNEL_ENG,CUSTOMER_NEW_CHANNEL,INVENTORY_ITEM_ID)])
  
  data_top_95_pc_portfolio[,`Week Starting` := ymd(`Week Starting`)] # to lubridate date format
  
  return(data_top_95_pc_portfolio)
  
}

remove_leading_zeros = function(grain,single_grain){
  cumOrderQty = cumsum(single_grain$ORDERED_QUANTITY)
  if (cumOrderQty[1] == 0){
    ##print(grain)
  }
  return(single_grain[cumOrderQty> 0])
}

convert_covid_indx_from_daily_to_week = function(covid_daily_indices){
  # Roll up data
  covid_daily_indices = covid_daily_indices[order(Date)]
  covid_daily_indices[,week_day := weekdays(Date)]
  covid_daily_indices[,Week_starting := floor_date(Date,unit='week',week_start = 1)] # like aera
  weekly_indices = covid_daily_indices[,.(new_cases = sum(new_cases),
                                          new_deaths = sum(new_deaths),
                                          death_perc = round(sum(new_deaths)*100/sum(new_cases),2),
                                          `School Closing` = median(`School Closing`),
                                          `Stringency Index` = median(`Stringency Index`),
                                          `Workplace Closing` = median(`Workplace Closing`),
                                          `Close Public Transport` = median(`Close Public Transport`),
                                          `Cancel Public Events` = median(`Cancel Public Events`)),
                                       by=.(country_code,country_name,Week_starting)]
  # Calculate rate of change
  rate_of_change = function(ts){
    #ts = weekly_indices$new_cases
    lag_ts = shift(ts,1)
    rate_of_change = round((ts - lag_ts)*100/lag_ts,2)
    return(rate_of_change)
  }
  # Add derived features
  weekly_indices[,new_cases_roc := rate_of_change(new_cases)]
  weekly_indices[,new_deaths_roc := rate_of_change(new_deaths)]
  weekly_indices[,deaths_perc_roc := rate_of_change(death_perc)]
  # Delete na
  weekly_indices = na.omit(weekly_indices)
  
  return(weekly_indices)
}


get_preproc_data_for_covid_analysis = function(access_token=NULL){
  
  #0. Read data
  ip_data = get_report(access_token=NULL)
  data = ip_data$weekly_data
  covid_daily_indices = ip_data$covid_daily_indices
  korean_channel_maping = ip_data$korean_channel_maping
  rm(ip_data)
  #1. Keep top 95%ile data
  master_top_prods = top_products(data,korean_channel_maping,start_date = '2016-10-03',end_date = '2021-10-04')
  master_top_prods = master_top_prods[CUSTOMER_NEW_CHANNEL_ENG != '기타직매출 (Other direct sales)']
  #2. Remove leading zeros
  top_prods = master_top_prods[,remove_leading_zeros(.BY,.SD),by=.(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID)]
  #3. Limit data to snapshot
  snapshot_date = max(top_prods$`Week Starting`)
  top_prods = top_prods[`Week Starting` <= snapshot_date]
  #4. Add missing weeks and fill missing values by 0
  max_date = max(top_prods$`Week Starting`) # same as snapshot date
  top_prods = top_prods %>% group_by(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID,CUSTOMER_NEW_CHANNEL) %>% 
    complete(`Week Starting` = seq(min(`Week Starting`), max_date, by = "week"),fill = list(ORDERED_QUANTITY = 0)) %>%
    as.data.table()
  #5. Convert daily to weekly covid indices
  weekly_indices = convert_covid_indx_from_daily_to_week(covid_daily_indices)
  #6. Output
  covid_cor_ip = list('top_prods' = top_prods,'weekly_indices' = weekly_indices)
  return(covid_cor_ip)
}

########################## 2 COVID Analysis functions ##########################
get_non_intermittent_grains = function(data,level=0.2){
  df = data[,.(num_of_zeros = sum(CT_VOLUME == 0),
               num_of_pts = .N),by=.(DD_GRAIN)]
  df[,intermittency := num_of_zeros/num_of_pts]
  non_intermittent_grains = df$DD_GRAIN[df$intermittency <= level]
  return(non_intermittent_grains)
}

get_covid_index_correlation = function(x,y,p_level=0.05,corr_level=0.5){
  tryCatch({
    #browser()
    res = cor.test(x, y, method = 'pearson')
    corr_coef = res$estimate[[1]]
    p_value = res$p.value
    if (p_value < p_level & abs(corr_coef) > corr_level){
      return(corr_coef)
    } else{ return(double())}
  }, error=function(error_message) { return(double())}
  )
}

get_abc_class = function(data){
  vol_per_grain = data[,.(tot_vol = sum(CT_VOLUME)),by=.(DD_GRAIN)]
  vol_per_grain[,vol_share := tot_vol/sum(vol_per_grain$tot_vol)]
  vol_per_grain = vol_per_grain[order(vol_share, decreasing = T)]
  vol_per_grain[,cumshare := cumsum(vol_share)]
  vol_per_grain[,DD_ABC_CLASS := ifelse(cumshare <= 0.8,"A",ifelse(cumshare <= 0.95,"B","C"))]
  return(vol_per_grain[,.(DD_GRAIN,DD_ABC_CLASS)])
}

get_xyz_class = function(input_ts){
  avg = mean(input_ts)
  stdev = sd(input_ts)
  coef_of_var = stdev/avg
  if (coef_of_var <= 0.5) {
    return('X')
  } else if (coef_of_var <= 0.75) {
    return('Y')
  } else {
    return('Z')
  }
}

run_covid_corr_analysis = function(access_token=NULL){
  
  #0. Data
  ip_data = get_preproc_data_for_covid_analysis(access_token=NULL)
  preproc_sales = ip_data$top_prods
  weekly_indices = ip_data$weekly_indices
  
  #1. Rename cols
  setnames(preproc_sales,c('Week Starting','ORDERED_QUANTITY'), c('DD_DATE','CT_VOLUME'))
  preproc_sales[,DD_GRAIN := paste(CUSTOMER_NEW_CHANNEL_ENG,INVENTORY_ITEM_ID,sep="_")]
  
  #2. Keep post covid data only
  period_start = ymd('2019-02-01')
  period_end = ymd('2042-02-01') # arbitrary large value
  data = preproc_sales[DD_DATE >= period_start & DD_DATE <= period_end] # period definition
  cat('number of grains =>',length(unique(data$DD_GRAIN)),'\n')
  
  #3. Create ABC-XYZ Classification
  abc_info = get_abc_class(data)
  xyz_info = data[,.(DD_XYZ_CLASS = get_xyz_class(CT_VOLUME)),by=.(DD_GRAIN)]
  
  #4. Keep AB-XY only
  data = merge(data,abc_info,by='DD_GRAIN')
  data = merge(data,xyz_info,by='DD_GRAIN')
  data = data[DD_ABC_CLASS %in% c('A','B')]
  data = data[DD_XYZ_CLASS %in% c('X','Y')]
  cat('number of grains (AB-XY segment) =>',length(unique(data$DD_GRAIN)),'\n')
  
  #5. Remove grains with more than 20% intermittency
  non_intermittent_grains = get_non_intermittent_grains(data) # non-itermit grains
  data = data[DD_GRAIN %in% non_intermittent_grains] # keep non-intermitent grains
  cat('number of non-intermittent grains =>',length(unique(data$DD_GRAIN)),'\n')
  
  #6. Attach COVID indices
  weekly_indices[,Week_starting := as_date(Week_starting)]
  data[,DD_DATE := as_date(DD_DATE)]
  covid_data = merge(data[,.(DD_GRAIN,DD_DATE,DD_ABC_CLASS,DD_XYZ_CLASS,CT_VOLUME)],weekly_indices,by.x = 'DD_DATE',by.y = 'Week_starting',all.x=T)
  covid_data[is.na(covid_data)] = 0
  
  #7. Correlation analysis
  corr_op = covid_data[,.(cor_new_cases = get_covid_index_correlation(CT_VOLUME,new_cases,p_level=0.05),
                          cor_new_deaths = get_covid_index_correlation(CT_VOLUME,new_deaths,p_level=0.05),
                          cor_death_perc = get_covid_index_correlation(CT_VOLUME,death_perc,p_level=0.05),
                          cor_school_closing = get_covid_index_correlation(CT_VOLUME,`School Closing`,p_level=0.05),
                          cor_strng_idx = get_covid_index_correlation(CT_VOLUME,`Stringency Index`,p_level=0.05),
                          cor_wplace_close = get_covid_index_correlation(CT_VOLUME,`Workplace Closing`,p_level=0.05),
                          cor_close_pub_transp = get_covid_index_correlation(CT_VOLUME,`Close Public Transport`,p_level=0.05),
                          cor_cancel_pub_events = get_covid_index_correlation(CT_VOLUME,`Cancel Public Events`,p_level=0.05),
                          cor_new_cases_roc = get_covid_index_correlation(CT_VOLUME,new_cases_roc,p_level=0.05),
                          cor_new_deaths_roc = get_covid_index_correlation(CT_VOLUME,new_deaths_roc,p_level=0.05),
                          cor_death_perc_roc = get_covid_index_correlation(CT_VOLUME,deaths_perc_roc,p_level=0.05)),
                       by=.(DD_GRAIN)]
  
  #8. Create flags - -1/0/1
  corr_op[,new_cases_flag := ifelse(is.na(cor_new_cases),0,ifelse(cor_new_cases>0,1,-1))]
  corr_op[,new_deaths_flag := ifelse(is.na(cor_new_deaths),0,ifelse(cor_new_deaths>0,1,-1))]
  corr_op[,new_cases_roc_flag := ifelse(is.na(cor_new_cases_roc),0,ifelse(cor_new_cases_roc>0,1,-1))]
  corr_op[,new_deaths_roc_flag := ifelse(is.na(cor_new_deaths_roc),0,ifelse(cor_new_deaths_roc>0,1,-1))]
  corr_op[,school_close_flag := ifelse(is.na(cor_school_closing),0,ifelse(cor_school_closing>0,1,-1))]
  corr_op[,string_idx_flag := ifelse(is.na(cor_strng_idx),0,ifelse(cor_strng_idx>0,1,-1))]
  corr_op[,wplace_close_flag := ifelse(is.na(cor_wplace_close),0,ifelse(cor_wplace_close>0,1,-1))]
  corr_op[,close_pub_transp_flag := ifelse(is.na(cor_close_pub_transp),0,ifelse(cor_close_pub_transp>0,1,-1))]
  corr_op[,cancel_pub_events_flag := ifelse(is.na(cor_cancel_pub_events),0,ifelse(cor_cancel_pub_events>0,1,-1))]
  
  #9. Prepare final output
  flag_cols = names(corr_op)[grepl(pattern = "*_flag",x = names(corr_op))]
  corr_cols = names(corr_op)[grepl(pattern = "cor_*",x = names(corr_op))]
  indicator = unique(corr_op[,c('DD_GRAIN',corr_cols,flag_cols),with=F]) 
  #channel_material = str_split_fixed(indicator$DD_GRAIN, "\\|\\|", 2)
  #indicator[,Channel := channel_material[,1]]
  #indicator[,Material := channel_material[,2]]
  indicator[,c("Channel","Material") := tstrsplit(indicator$DD_GRAIN, "_", fixed=T)]
  final_op = indicator[,c('Channel','Material',corr_cols,flag_cols),with = F]
  
  return(final_op)
  
}

########################## Construct Code Structure ##########################

get_report = function(access_token=NULL) {
  # Read Data
  weekly_data = fread('/Users/sachinkadam/Downloads/1 Sales_Order_Weekly.csv')
  covid_daily_indices = fread('/Users/sachinkadam/Downloads/2 Korea Covid Indices.csv')
  korean_channel_maping = fread('/Users/sachinkadam/Downloads/2 Customer Channel Korean.csv')
  ip_data = list('weekly_data' = weekly_data,'covid_daily_indices' = covid_daily_indices,'korean_channel_maping' = korean_channel_maping)
  return(ip_data)
}

validateR =function(df,input_data=NULL){
  result = tryCatch({
    list(msg='Validated')
  }, error = function(err) {
    err
  })
  return (result)
}

predictR = function(df=NULL,input_data=NULL){
  result = tryCatch({
    flog.info("Entered predictR function")
    df = run_covid_corr_analysis(access_token=NULL)
    flog.info("predictR execution - successful")
    data.frame(df)
  }, error = function(err) {
    flog.fatal("Error in predictR = %s",err)
  })
  return (result)
}

########################## Construct Code Structure ##########################
covid_analysis = predictR()
write.csv(covid_analysis,"/Users/sachinkadam/Pulmuone/OutputShared_27Apr.csv",row.names = FALSE)
