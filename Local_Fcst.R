library(data.table)
library(dplyr)
library(lubridate)
library(tseries)
library(forecast)
library(forecastML)
library(stringr)
library(smooth)

rm(list=ls())
library(data.table)
x = fread('https://insightqd.aeratechnology.com/ispring/client/v3/reports/sales-order-daily?accessToken=82624c68c3fdab72d2431c9528112e7f')

input = read.csv("/Users/sachinkadam/Downloads/SalesHistoryCortexNew_07-06-2022-13-56-22.csv")
input$Part.Number = as.character(input$Part.Number)
input$Part.Number  = str_pad(input$Part.Number, 6, pad = "0")

#input$grains = paste0(input$COMOP,"-",input$`Company Code`,"-",sprintf("%06d",input$`Part Number`))
abc_xyz <- input %>%
  group_by(COMOP, Company.Code,Part.Number) %>%
  summarise(Total.Volume = sum(Sales.Quantity)) %>%
  arrange(desc(Total.Volume)) %>%
  mutate(volume.share = round(Total.Volume*100/sum(Total.Volume),4), cum.sum = cumsum(Total.Volume)) %>%
  mutate(abc_class = ifelse(cum.sum <= 0.8 * sum(Total.Volume),
                            "A",
                            ifelse((cum.sum > 0.8 * sum(Total.Volume)) & (cum.sum <= 0.95 * sum(Total.Volume)),
                                   "B",
                                   "C")))
abc_xyz = data.frame(abc_xyz)
abc_xyz$grains = paste0(abc_xyz$COMOP,"-",abc_xyz$Company.Code,"-",abc_xyz$Part.Number)
write.csv(abc_xyz,"/Users/sachinkadam/Downloads/MAHABC_XYZ_UPDATE.csv",row.names = FALSE)


################### forecast on local ##############

library(data.table)
library(dplyr)
library(lubridate)
library(tseries)
library(forecast)
library(forecastML)
library(stringr)
library(smooth)


concatFn <- function(data) {
  purrr::reduce(data, c)  %>%
    data.frame(forecast = .)
}

################################# holtwinters ########################
input_df <- read.csv("/Users/sachinkadam/Downloads/SalesHistory068571.csv",skip = 3)
head(input_df)
dim(input_df)
input_df <- filter(input_df,input_df$YYYYMM<=202205)
dim(input_df)

tsdata<-ts(input_df$Sales.Quantity,frequency = 12)
print(tsdata)
forecast_model.hw <- function(tsdata) {
  testing_data_length <- 43
  msg <- "SU"
  #attributes(tsdata)$class <- "ts"
  modelF <- HoltWinters(tsdata)
  com <- forecast::forecast(modelF, h = testing_data_length, level = 95)  %>% {
    fit <- as.numeric(.$model$fitted[, "xhat"])
    meanVal <- as.numeric(.$mean)
    fitM <- c(fit, meanVal)
    rmLen <- length(tsdata) - length(fit)
    fit2 <- as.numeric(smooth::sma(head(tsdata, rmLen))$fitted)
    c(fit2, fitM)
  }  %>%
    concatFn
  list(com = com, model = modelF, errorMsg= msg)
}

data = forecast_model.hw(tsdata)

data$com$forecast = ifelse(data$com$forecast<0,0,data$com$forecast)
write.csv(data.frame(data$com$forecast),"/Users/sachinkadam/Downloads/Holt-Winters.csv",row.names = FALSE)
#plot(decompose(tsdata))

#################################### THETAM ###############################################
input_df <- read.csv("/Users/sachinkadam/Downloads/SalesHistory068571.csv",skip = 3)
head(input_df)
dim(input_df)
input_df <- filter(input_df,input_df$YYYYMM<=202205)
dim(input_df)
tsdata<-ts(input_df$Sales.Quantity,frequency = 12)
plot(decompose(tsdata))

forecast_model.Thetam <- function(tsdata, train_x, test_x,  output_frequency, forecast_frequency,  config, ...) {
  # default hyperparameters:
  # params = {
  #      "confidence_level_lower":80,
  #      "confidence_level_upper":95
  # } 
  # prepare hyperparameters and prepare a response
  conf_upper <- 80
  conf_lower <- 95
  level <- c(conf_lower,conf_upper)
  testing_data_length <-43
  msg <- "SU"
  #attributes(tsdata)$class <- "ts"
  modelF <- forecastHybrid::thetam(tsdata)
  if(modelF$seasonal!=TRUE){
    fitted <- modelF$fitted
  }else{
    fitted<- modelF$fitted*modelF$seasadjhist
    
  }
  com <- forecast::forecast(modelF, h = testing_data_length, level=level)
  mean <- com$mean
  com <- c(fitted, mean)
  com <- data.frame(com)
  names(com)[1]<- "forecast"
  list(com = com, model = modelF, errorMsg= msg)
  
}
df = data.frame(com$forecast)
df$com.forecast = ifelse(df$com.forecast<0,0,df$com.forecast)
write.csv(df,"/Users/sachinkadam/Downloads/thetam.csv",row.names = FALSE)

###### --------------- Auto ARIMA -----------------------

input_df <- read.csv("/Users/sachinkadam/Downloads/701613_IT20.csv",skip = 4)
head(input_df)
dim(input_df)
input_df <- filter(input_df,input_df$Actual.Date.Value<=202206)
dim(input_df)
tsdata<-ts(input_df$Sales.Quantity,frequency = 12)
plot(decompose(tsdata))


forecast_model.auto.arima <- function(tsdata, train_x, test_x,  output_frequency, forecast_frequency,  config, ...) {
  levels <-c(90,95)
  d <- NA
  start_p <- 2
  start_q <-2
  max_p <-5
  max_q <-5
  testing_data_length <- 37
  msg <- "SU"
  class(tsdata)
  #attributes(tsdata)$class <- "ts"
  modelF <- forecast::auto.arima(tsdata, d=d, max.q = max_q, max.p=max_p, start.q=start_q, start.p=start_p)
  com <- forecast::forecast(modelF, h =  testing_data_length, level = levels)
  com
  com <- forecast::forecast(modelF, h =  testing_data_length, level = levels)  %>% {
    .[c('fitted', 'mean')]
  }  %>%
    concatFn
  
  list(com = com, model = modelF, errorMsg= msg)
}

ts.plot(com$mean)
