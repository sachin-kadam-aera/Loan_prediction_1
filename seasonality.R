library(data.table)
library(dplyr)
library(lubridate)
library(tseries)
library(forecast)
library(forecastML)
library(stringr)
library(dplyr)
library(data.table)
library("doParallel")
library('nonlinearTseries')
library(snow)

input = read.csv("/Users/sachinkadam/Downloads/airpass1.csv")
#input = filter(input,input$grains == 'three_fourth')

grains1 = data.frame(unique(input[c('grains')],index = NULL))
#grains1 = filter(grains1,grains1$grains == 'three_fourth')

bucketing <- function(i){
  set.seed(200)
  gr = grains1$grains[i]
  if(forecast_frequency=='Weekly'){
    freq = 52
  }else if(forecast_frequency=='Monthly'){
    freq = 12
  }else{
    freq = 365
  }
  sample = filter(input,input$grains == gr)
  #grains <- dplyr::select(grains,c(forecast_column_name))
  grains = sample
  grains <- data.frame(grains)
  if (class(grains)!='ts') {
    grains <- ts(grains['Sales'],frequency = freq)
  } else {
    grains <- grains
  }
  tryCatch( {
    seasonal_flag <- FALSE
    thetam_model <- forecastHybrid::thetam(grains)
    seasonal_flag <- thetam_model$seasonal
  },
  error=function(error_message) {
    seasonal_flag <- FALSE
  }
  )
  
  tryCatch( {
    linearity_flag <- FALSE
    P_Value <- nonlinearTseries::nonlinearityTest(grains)
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
    Count<- Terasvirta_Test+White_Test+Keenan_Test+McLeodLi_Test+Tsay_Test
    linearity_flag <- ifelse(Count>=3,TRUE,FALSE)
    
  },
  error=function(error_message) {
    linearity_flag <- FALSE
  }
  )
  
  tryCatch( {
    intermittent_flag <- FALSE
    x <- grains
    total_len <- length(x)
    x<-x[ min( which ( x != 0 )) :total_len ]
    if(total_len>freq){
      zero_len  <- sum(x == 0)
      ratio <- zero_len/total_len
      if(ratio>=0.30){
        intermittent_flag = TRUE
      }
      if(intermittent_flag == FALSE){
        x <- tail(x,n=freq)
        zero_len  <- sum(x == 0)
        total_len <- length(x)
        ratio <- zero_len/total_len
        if(ratio>=0.30){
          intermittent_flag = TRUE
          
        }
      }
    }
    else{
      zero_len  <- sum(x == 0)
      ratio <- zero_len/total_len
      if(ratio>=0.30){
        intermittent_flag = TRUE
      }
      
      
    }
  },
  error=function(error_message) {
    intermittent_flag <- FALSE
  }
  )
  
  df = data.frame(cbind(Grain = gr,linearity_flag,intermittent_flag,seasonal_flag))
  #bucketing <- list(linearity_flag,intermittent_flag,seasonal_flag)
  #names(bucketing) <- c("linearity_flag","intermittent_flag", "seasonal_flag")
  #bucketing <- as.data.frame(bucketing)
  return(df)
}

bucketing_fn <- function(grains,forecast_column_name,forecast_frequency){
  forecast_frequency = 'Monthly'
  forecast_column_name = 'Sales'
  no_cores <- 2
  cl <- makeCluster(no_cores,"SOCK")
  doParallel::registerDoParallel(cl)
  bucketing_df = data.frame()
  bucketing_df <- foreach::foreach(p = 1:nrow(grains1), .combine = rbind,.export = c('input','forecast_frequency','forecast_column_name'),.packages = c("dplyr", "doParallel", "data.table","forecastHybrid","nonlinearTseries")) %dopar% bucketing(p)
  stopCluster(cl = NULL)
  bucketing_df<- grains %>%
    group_by(grains) %>%
    do(data.frame(val=bucketing(.)))
  bucketing_df<- data.frame(bucketing_df)
  names(bucketing_df)[2]<-"linear"
  names(bucketing_df)[3]<-"intermittent"
  names(bucketing_df)[4]<-"seasonal"
  bucketing_df[is.na(bucketing_df)] <- FALSE
  return(bucketing_df)
  
}


test_df <-bucketing_fn (grains,forecast_column_name,forecast_frequency)
