rm(list = ls())

library(dplyr)
library(data.table)
library(forecast)
library(forecastHybrid)
library(plotly)
library(lubridate)

################################### Seasonality detection functions ###################################

# Seasonality test function - thetam
is_bucket_test_satisfied_thetam <- function(grain, forecast_column_name, forecast_frequency){
  # grain = data.frame(preproc_sales[DD_GRAIN == 'affiliate-1223376'])
  # forecast_column_name = "CT_VOLUME"
  # forecast_frequency = "Weekly"
  grain <- dplyr::select(grain,c(forecast_column_name))
  if(forecast_frequency=='Weekly'){
    freq = 52
  }else if(forecast_frequency=='Monthly'){
    freq = 12
  }else{
    freq = 365
  }
  
  tryCatch( {
    if (class(grain)!='ts') {
      grain <- ts(grain[forecast_column_name],frequency = freq)
    } else {
      grain <- grain
    }
    seasonal_flag <- FALSE
    thetam_model <- forecastHybrid::thetam(grain)
    seasonal_flag <- thetam_model$seasonal
    return(seasonal_flag)
  },
  error=function(error_message) {
    return(FALSE)
  }
  )
  
}

# Seasonality test function - tbats
is_bucket_test_satisfied_tbats <- function(grain, forecast_column_name, forecast_frequency){
  grain <- dplyr::select(grain,c(forecast_column_name))
  if(forecast_frequency=='Weekly'){
    freq = 365/7
  }else if(forecast_frequency=='Monthly'){
    freq = 12
  }else{
    freq = 365
  }
  
  tryCatch( {
    if (class(grain)!='ts') {
      grain <- ts(grain[forecast_column_name],frequency = freq)
    } else {
      grain <- grain
    }
    seasonal_flag <- FALSE
    tbats_model <- forecast::tbats(grain)
    seasonal_flag <- !is.null(tbats_model$seasonal)
    print(seasonal_flag)
    return(seasonal_flag)
  },
  error=function(error_message) {
    return(FALSE)
  }
  )
}

# Linearity test function
is_linear_bucket_test_satisfied <- function(grain, forecast_column_name, forecast_frequency){
  
  # grain = singleGrain
  # forecast_column_name = 'CT_VOLUME'
  # forecast_frequency = 'Weekly'
  
  
  grain <- dplyr::select(grain,c(forecast_column_name))
  if(forecast_frequency=='Weekly'){
    freq = 52
  }else if(forecast_frequency=='Monthly'){
    freq = 12
  }else{
    freq = 365
  }
  
  tryCatch( {
    if (class(grain)!='ts') {
      grain <- ts(grain,frequency = freq)
    } else {
      grain <- grain
    }
    P_Value <- nonlinearTseries::nonlinearityTest(grain)
    value1 <- P_Value$Terasvirta$p.value
    value2 <- P_Value$White$p.value
    value3 <- P_Value$Keenan$p.value
    value4<-P_Value$McLeodLi$p.value
    value5 <- P_Value$Tsay$p.value
    value6 <- P_Value$TarTest$p.value
    Terasvirta_Test<-ifelse(value1[1]>=0.05,1,0)
    White_Test <- ifelse(value2[1]>=0.05,1,0)
    Keenan_Test <- ifelse(value3[1]>=0.05,1,0)
    McLeodLi_Test <- ifelse(value4[1]>=0.05,1,0)
    Tsay_Test <- ifelse(value5>=0.05,1,0)
    
    Terasvirta_Test = ifelse(is.na(Terasvirta_Test),0,Terasvirta_Test)
    White_Test = ifelse(is.na(White_Test),0,White_Test)
    Keenan_Test = ifelse(is.na(Keenan_Test),0,Keenan_Test)
    McLeodLi_Test = ifelse(is.na(McLeodLi_Test),0,McLeodLi_Test)
    Tsay_Test = ifelse(is.na(Tsay_Test),0,Tsay_Test)
    
    Count<- Terasvirta_Test+White_Test+Keenan_Test+McLeodLi_Test+Tsay_Test
    Result <- ifelse(Count>=3,TRUE,FALSE)
    return(Result)
    
  },
  error=function(error_message) {
    return(FALSE)
  }
  )
  
}


################################### Example ###################################
df = fread('/Users/sandeepdhankhar/Downloads/Standard codes/Input data.csv')
df[,DD_DATE := dmy(DD_DATE)]
df = df[order(-DD_GRAIN,DD_DATE)]

# Seasonality test - thetam - weekly - outlier treated sales
thetam_seasonal_flag = df[,.(is_bucket_test_satisfied_thetam(as.data.frame(.SD),"CT_OUTLIER_TREATED_VOLUME","Weekly")),by = .(DD_GRAIN)]
cat('Number of linear grains according to linearity test => ',sum(thetam_seasonal_flag$V1),'/',nrow(thetam_seasonal_flag),'\n')

# Seasonality test - tbats - weekly - outlier treated sales
tbats_seasonal_flag = df[,.(is_bucket_test_satisfied_tbats(as.data.frame(.SD),"CT_OUTLIER_TREATED_VOLUME","Weekly")),by = .(DD_GRAIN)]
cat('Number of linear grains according to linearity test => ',sum(tbats_seasonal_flag$V1),'/',nrow(tbats_seasonal_flag),'\n')

# Linearity test
linear_df = df[,.(is_linear_bucket_test_satisfied(as.data.frame(.SD),'CT_OUTLIER_TREATED_VOLUME','Weekly')),by = .(DD_GRAIN)]
names(linear_df) = c('Grain','Is_Linear')
cat('Number of linear grains according to linearity test => ',sum(linear_df$Is_Linear),'/',nrow(linear_df),'\n')

# Visualize the output - thetam
thetam_seasonal_grains = thetam_seasonal_flag$DD_GRAIN[thetam_seasonal_flag$V1 == TRUE]
for (grain in thetam_seasonal_grains){
  singleGrain = df[DD_GRAIN == grain]
  fig <- plot_ly(singleGrain, type = 'scatter', mode = 'lines')%>%
    add_trace(x = ~DD_DATE, y = ~CT_OUTLIER_TREATED_VOLUME, name = grain)
  print(fig)
}

# Visualize the output - TBATS
tbats_seasonal_grains = tbats_seasonal_flag$DD_GRAIN[tbats_seasonal_flag$V1 == TRUE]
for (grain in tbats_seasonal_grains){
  singleGrain = df[DD_GRAIN == grain]
  fig <- plot_ly(singleGrain, type = 'scatter', mode = 'lines')%>%
    add_trace(x = ~DD_DATE, y = ~CT_OUTLIER_TREATED_VOLUME, name = grain)
  print(fig)
}

# Visualize the output - linearity
linear_grains = linear_df$Grain[linear_df$Is_Linear == T]

for (grain in linear_grains){
  singleGrain = df[DD_GRAIN == grain]
  fit <- lm(CT_OUTLIER_TREATED_VOLUME ~ DD_DATE, data = singleGrain)
  fig <- plot_ly(singleGrain, type = 'scatter', mode = 'lines')%>%
    add_trace(x = ~DD_DATE, y = ~CT_OUTLIER_TREATED_VOLUME, name = grain) %>% 
    add_lines(x = ~DD_DATE, y = fitted(fit), name = 'Regression line') %>%
    layout(legend = list(x = 0.1, y = 1))
  print(fig)
}



