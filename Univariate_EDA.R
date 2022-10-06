---
title: "Auto EDA - Univariate Time Series"
date: "`r Sys.Date()`"
output:
  html_document:
    df_print: paged
    toc: true
    theme: united
---

In this notebook, we are performing exploratory data analysis on univariate time series. The main purpose of the this report is to give data scientist capability to perform a quick analysis of data when they recieve it. 

The notebook is divided into three sections - 

1. All Grains - The summary statistics would be performed on full data, this will give a view on properties of each grains.

2. Sample Grains - Based on the ABC-XYZ categorization, we work with a sample of data.

3. Time Windows - Analysis on each of the time 

# Prerequisites

1. The panel data should be balanced. Balanced here means that for each grain there should be T periods. An easy check for knowning if your data is balanced or not is to check if - n = N x T, where n is the total number of rows, N is number of unique grains and T is the time periods.

2. User needs to update few parameters before running this report. 
  - Grains
  - Date
  - Date Format
  - Aggregation Frequency
  - Forecast column

# General Clean Up

There are few clean up that are being performed on the data sets before we start analyzing and would like the user to take a note of it.


1. Combined all the grains into a single column called "Grain". The order of the grains will be same as provided but will be seperated by a dash symbol.

2. Renamed the date column to "Date" and Forecast column to "Volume".

3. The dates format is converted to YYYY-MM-DD for ease of exploration.

4. Based on the aggregation frequency provided we have converted the date to that frequency and aggregated the data on the grain and date column. Aggregation function applied is a SUM.

```{r}
# Check for installed packages, and install them
rm(list = ls())
list.of.packages <- c("ggplot2", "dplyr", "lubridate",
                      "tsibble", "feasts", "gridExtra", "ggpmisc", "tseries", "forecast",
                      "forecastML", "tidyr", "dtwclust", "pracma", "knitr","stringr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
```


```{r load_libraries, echo=FALSE, message=FALSE}
library(data.table)
library(ggplot2)
library(dplyr)
library(lubridate)
library(tsibble)
library(feasts)
library(gridExtra)
library(ggpmisc)
library(tseries)
library(forecast)
library(forecastML)
library(tidyr)
library(dtwclust)
library(pracma)
library(knitr)
library(data.table)
library(stringr)

```

```{r monthly_config_exxon, echo=TRUE}

############################### User Inputs ###############################
data_path = "/Users/sachinkadam/EDA/Exxon"
raw.dat <- read.csv(paste(data_path,"Preprocessed_Monthly_Sales_History_Exxon_Updated.csv",sep = '/'))
#raw.dat = raw.dat[raw.dat$ABC %in% c('A','B'),]

grain.cols <- c('Forecast.Grain')
date.col <- "Calendar.Month"
date.format <- "ym" # ymd/dmy/mdy/ym/my

aggregation.frequency <- "monthly"
volume.col <- "Modified.Sales...After.Outlier.treatment"

snapshots <- c("2021-12-01","2021-11-01","2021-10-01")
validation_single_period = c(1,2,3)
validation_agg_period = c(3,6)
############################### User Inputs ###############################

freq <- ifelse(aggregation.frequency == "monthly",
                    12,
                    ifelse(aggregation.frequency == "weekly",
                           52,
                           365))
moving.average.period.1 <- round(freq/4)
moving.average.period.2 <- round(freq/2)
```


```{r clean_data, echo=FALSE}
# Start time
time.start = Sys.time()

# Combine cols into a single column
if(length(grain.cols) > 1){
  raw.dat[,"Grain"] <- apply(raw.dat[, grain.cols], 1, paste0, collapse = "-")
} else {
  raw.dat[,"Grain"] = as.character(raw.dat[,grain.cols])
}


raw.dat <- raw.dat %>% select(Grain, "Date" = all_of(date.col), "Volume" = all_of(volume.col))

dat <- raw.dat %>%
  select("Date", 
         "Grain",
         "Volume") %>%
  mutate(Date = get(date.format)(Date))

# Convert the data to requested frequency
if(aggregation.frequency == "monthly"){
  dat <- dat %>%
    mutate(Date = floor_date(Date, unit = "month")) %>%
    group_by(Date, Grain) %>%
    summarise(Volume = sum(Volume), .groups = 'drop') %>%
    arrange(Grain, Date)
}else if(aggregation.frequency == "weekly"){
  dat <- dat %>%
    mutate(Date = floor_date(Date, unit = "week")) %>%
    group_by(Date, Grain) %>%
    summarise(Volume = sum(Volume), .groups = 'drop') %>%
    arrange(Grain, Date)
}


# Remove the raw data as it will be redundant.
rm(raw.dat)
```


# Data-set Summary

In this section, we explore the data set as a whole and try to summarize important characteristics of each time series.

```{r fill_gaps_in_mid, echo=FALSE}

if(aggregation.frequency == "monthly"){
  dat <- dat %>%
    forecastML::fill_gaps(date_col = 1,
    frequency = '1 month',
    groups = c("Grain")) %>%
    replace_na(list(Volume = 0))
}else if(aggregation.frequency == "weekly"){
  dat <- dat %>%
    forecastML::fill_gaps(date_col = 1,
    frequency = '1 week',
    groups = c("Grain")) %>%
    replace_na(list(Volume = 0))
}
```

```{r Aggregated Time series, echo=FALSE}
agg.time.series <- dat %>%
  group_by(Date) %>%
  summarise(Total.Volume = sum(Volume),
            n.grains = length(unique(Grain)))
```

## Quick Look (Cleaned Data)

```{r show_cleaned_data, echo=FALSE}
# A quick look on the processed data.
head(dat)
```

## Quick Summary

### Total Volume by Time Period

Based on the frequency of the time series we aggregate all the Volume of all the grains on Date and find Total Volume. 

```{r monthly_summary, echo=FALSE}
agg.time.series
```

```{r aggregated time series, echo=FALSE}
ggplot(agg.time.series, aes(x = Date, y = Total.Volume)) +
  geom_line(color = "steelblue") +
  geom_smooth(method = "loess",
              formula = y ~ x,
              color = "#FC4E07",
              fill = "#FC4E07") +
  labs(x = "Date", 
       y = "Total Volume",
       title = paste0("Aggregated Time Series - ", aggregation.frequency))
```
   
### Summary Statistics

```{r quick_summary, echo=FALSE}
# A quick look at the summary of the data
quick.summary <- dat %>% 
  group_by(Grain) %>%
  summarise(min.date = min(Date),
            max.date = max(Date),
            observations = n(),
            n.zeros = sum(Volume == 0),
            min.volume = min(Volume),
            mean.volume = mean(Volume),
            median.volume = median(Volume),
            max.volume = max(Volume),
            std.dev = sd(Volume),
            CoV = std.dev/mean.volume,
            stability = ifelse(CoV < 0.5,
                               "Stable",
                               ifelse((CoV >= 0.5) & (CoV < 0.75),
                                      "Mildly Stable",
                                      "Unstable"))) %>% # Standard is 10%, 25% and 25+, need to understand more about stabality thresholds.
  arrange(desc(observations), desc(mean.volume)) %>%
  mutate(stability = ifelse(is.na(stability), "Unstable", stability))

quick.summary
```


### Yearly statistics

In this section we will summarize our time series based on the years, this will give use much granular information on how time series changing across years. 

```{r quick_yearly_summary, echo=FALSE}
yearly.stats <- dat %>% 
  group_by(Grain, Year = year(Date)) %>%
  summarise(min.date = min(Date),
            max.date = max(Date),
            total.volume = sum(Volume),
            observations = n(),
            min.volume = min(Volume),
            mean.volume = mean(Volume),
            median.volume = median(Volume),
            max.volume = max(Volume),
            std.dev = sd(Volume),
            CoV = std.dev/mean.volume,
            stability = ifelse(CoV < 0.5,
                               "Stable",
                               ifelse((CoV >= 0.5) & (CoV < 0.75),
                                      "Mildly Stable",
                                      "Unstable")), .groups = 'drop') %>%
  arrange(Grain, Year)

yearly.stats
```

** Yearly Variation **

```{r}
yearly.volume <- yearly.stats %>%
  group_by(Year) %>%
  summarise(total = sum(total.volume), .groups = 'drop')

ggplot(yearly.volume, aes(x = Year, y = total)) +
  geom_bar(position = 'dodge', stat = 'identity')
  
```

### Percentage of Grains vs Length of Time Series

```{r histogram_grains_observations, echo=FALSE}
ggplot(quick.summary, aes(x = observations, y = ..count../sum(..count..))) +
  geom_bar(stat = "count", fill="steelblue") +
  labs(x = "Length of Time Series",
       y = "% of Grains",
       title = "% of Grains vs Length of Time Series") +
  scale_y_continuous(labels=scales::percent)
```

## Stability of Time Series

* The above graph shows the number of times series that are stable or not. Stability of the times series is calculated using Coeffiecient of Variation (CoV), which is mathematically defined as the ratio of standard deviation and mean of the time series. 

* The current CoV is calculated on the full time series, not on a segment of time series. You should find the segment wise CoV in one of the following sections.

* Stable time series are easier to forecast and has lower CoV. Higher CoV would mean that time series has varies across the time periods.

```{r plot_stability_counts, echo=FALSE}
ggplot(quick.summary, aes(x = stability, y = ..count../sum(..count..))) +
  geom_bar(stat = "count", fill="steelblue") +
  labs(x = "Stability",
       y = "% of Grains",
       title = "Stability of Time Series") +
  scale_y_continuous(labels=scales::percent) +
  geom_text(stat='count', aes(label=paste0(round(..count..*100/sum(..count..),2),"%")), vjust=-0.25, size=3.5)

grain.stability <- quick.summary %>% select(Grain, stability) %>% distinct()
```

## Sample Entropy

This measures the “forecastability” of a time series, where low values indicate a high signal-to-noise ratio, and large values occur when a series is difficult to forecast.

Rule: 

ifelse(entropy < 0.3, "High", ifelse(entropy >= 0.3 & entropy < 0.6, "Medium", "Low"))
```{r forecastability_based_on_sample_entropy, echo=FALSE}
forecastability.entropy <- dat %>% 
  group_by(Grain) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  filter(n > 10) %>%
  group_by(Grain) %>%
  summarise(entropy = approx_entropy(ts(Volume, frequency = freq))) %>%
  arrange(entropy) %>%
  mutate(forecastability = ifelse(entropy < 0.3, "High", ifelse(entropy >= 0.3 & entropy < 0.6, "Medium", "Low")))

grain.entropy <- forecastability.entropy %>% select(Grain, forecastability) %>% distinct()
```

```{r sample_entropy_plot, echo=FALSE}
ggplot(forecastability.entropy, aes(x = forecastability, y = ..count../sum(..count..))) +
  geom_bar(stat = "count", fill="steelblue") +
  labs(x = "Forecastability",
       y = "% of Grains",
       title = "Forecastability of Time Series based on Sample Entropy") +
  scale_y_continuous(labels=scales::percent) +
  geom_text(stat='count', aes(label=paste0(round(..count..*100/sum(..count..),2),"%")), vjust=-0.25, size=3.5)
```

## Demand Pattern

Smooth Demand (regular demand over time, with limited variation in quantity)
CV2 <= 0.49 and ADI <= 1.32

Intermittent Demand (Sporadic demand with limited variation in quantity)
CV2 <= 0.49 and ADI > 1.32

Erratic Demand (Regular demand over time, but large variation in quantity)
CV2 > 0.49 and ADI <= 1.32

Lumpy Demand (Sporadic demand with large variation in quantity)
CV2 > 0.49 and ADI > 1.32

```{r demand_pattern, echo=FALSE}
demand.pattern.dat <- dat %>% 
  group_by(Grain) %>%
  summarise(observations = n(),
            n.zeros = sum(Volume == 0),
            mean.volume = mean(Volume),
            std.dev = sd(Volume),
            CoV = std.dev/mean.volume,
            CV2 = CoV**2,
            ADI = round(observations/(observations - n.zeros), 3)) %>% # Standard is 10%, 25% and 25+, need to understand more about stabality thresholds.
  arrange(desc(observations), desc(mean.volume)) %>%
  mutate(demand.pattern = ifelse((CV2 <= 0.49 &(ADI <= 1.32)), "Smooth",
                                 ifelse(((CV2 <= 0.49) &(ADI > 1.32)), "Intermittent",
                                        ifelse(((CV2 > 0.49) &(ADI <= 1.32)), "Erratic", "Lumpy")))) %>%
  replace_na(list(demand.pattern = "Low Observations"))

grain.demand.pattern <- demand.pattern.dat %>% select(Grain, demand.pattern)

ggplot(grain.demand.pattern, aes(x = demand.pattern, y = ..count../sum(..count..))) +
  geom_bar(stat = "count", fill="steelblue") +
  labs(x = "Demand Pattern",
       y = "% of Grains",
       title = "Demand Pattern") +
  scale_y_continuous(labels=scales::percent) +
  geom_text(stat='count', aes(label=paste0(round(..count..*100/sum(..count..),2),"%")), vjust=-0.25, size=3.5)
```

## ABC-XYZ Categorization

We classify each time series in one of the 9 categories. Where we define the categories as ABC and sub-divide them into XYZ. 

1. ABC is the standard categorization based on the cumulative Volume. We classify a grain as class A when they contribute towards top 80% of the total volume, B as cumulative total volume between 80% to 90% and rest as class C.

2. XYZ is classification based on the CoV (Coefficient of Variation) which defines whether the stability of the time series. 

```{r abc_xyz analysis, echo=FALSE}
xyz_class <- quick.summary %>% 
  select(Grain, stability) 

abc_xyz <- dat %>%
  group_by(Grain) %>%
  summarise(Total.Volume = sum(Volume)) %>%
  arrange(desc(Total.Volume)) %>%
  mutate(volume.share = round(Total.Volume*100/sum(Total.Volume),4), cum.sum = cumsum(Total.Volume)) %>%
  mutate(abc_class = ifelse(cum.sum <= 0.8 * sum(Total.Volume),
                            "A",
                            ifelse((cum.sum > 0.8 * sum(Total.Volume)) & (cum.sum <= 0.95 * sum(Total.Volume)),
                                    "B",
                                    "C"))) %>%
  inner_join(xyz_class, by = "Grain") %>%
  mutate(xyz_class = ifelse(stability == "Stable",
                            "X",
                            ifelse(stability == "Mildly Stable",
                                   "Y",
                                   "Z"))) %>%
  select(Grain, abc_class, xyz_class, volume.share)

ggplot(abc_xyz, aes(x = abc_class, y = ..count../sum(..count..), fill = xyz_class)) +
  geom_bar(stat = "count", position = "dodge") +
  scale_color_brewer(palette="Blues") +
  labs(x = "ABC Class", y = "% of Grains", title = "Grain Segmentation") +
  scale_y_continuous(labels=scales::percent) +
  geom_text(stat='count',
            aes(label=paste0(round(..count..*100/sum(..count..),2),"%")),
            vjust=-0.25, size=3.5, position = position_dodge(width = 1))

# TODO: Group by abc and xyz classes and sum the volume share, create graph for 9 categories.
volume.share.dat <- abc_xyz %>%
  group_by(abc_class, xyz_class) %>%
  summarise(total.share = sum(volume.share), 
            no.of.grains = n(), .groups = 'drop')

ggplot(volume.share.dat, aes(x = abc_class, y = total.share, fill = xyz_class)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_color_brewer(palette="Blues") +
  labs(x = "ABC Class", y = "% Volume Share", title = "Volume Share") +
  scale_y_continuous() +
  geom_text(stat='identity',
            aes(label=paste0(round(total.share, 2),"%", ", n = ", no.of.grains)),
            vjust=-0.25, size=4, position = position_dodge(width = 1))
  
# Need to write the classes if someone needs it for further analysis, need to change the colors
grain.abc_xyz <- abc_xyz
```

In order to perform further analysis on the data, we need to fill in the missing gaps in the data, making sure that the time series related features are properly calculated.

## Seasonality

Through seasonality we would be able to identify the pattern within time series. For different frequency of time series we will have single seasonality or multiple seasonality.

We will look at some general seasonality - Weekly and Monthly. For understanding other complex seasonality we will be using more of a model based approach - BATS and TBATS are best candidate to identify them.

Tips:

Try to find dips or increases in the line plots, if you see a consistent dip or increase consider that as a seasonal behaviour.

```{r seasonality, echo=FALSE}
monthly_seasonality <- ggplot() + labs(title = "Monthly Seasonality")
weekly_seasonality <- ggplot() + labs(title = "Weekly Seasonality")
daily_seasonality <- ggplot() + labs(title = "Daily Seasonality")

if(aggregation.frequency %in% c("monthly","weekly", "daily")){
  monthly_seasonality <- dat %>%
    mutate(month = month(Date)) %>%
    group_by(Grain, month) %>%
    summarise(Total.Volume = sum(Volume), .groups = 'drop') %>%
    group_by(month) %>%
    summarise(Mean.Volume = mean(Total.Volume)) %>%
    ggplot(aes(x = month, y = log10(Mean.Volume), group = 1)) +
    geom_line(size = 1.5, color = "steelblue") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    labs(x = "Month", y = "Log Total Volume", title = "Monthly Seasonality")
}

if(aggregation.frequency %in% c("daily", "weekly")){
  weekly_seasonality <- dat %>%
    mutate(week = week(Date)) %>%
    group_by(Grain, week) %>%
    summarise(Total.Volume = sum(Volume), .groups = 'drop') %>%
    group_by(week) %>%
    summarise(Mean.Volume = mean(Total.Volume)) %>%
    ggplot(aes(x = week, y = log10(Mean.Volume), group = 1)) +
    geom_line(size = 1.5, color = "steelblue") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    labs(x = "Week", y = "Log Total Volume", title = "Weekly Seasonality")
}

if(aggregation.frequency %in% c("daily")){ # To be tested
  daily_seasonality <- dat %>%
    mutate(day = day(Date)) %>%
    group_by(Grain, day) %>%
    summarise(Total.Volume = sum(Volume), .groups = 'drop') %>%
    group_by(day) %>%
    summarise(Mean.Volume = mean(Total.Volume)) %>%
    ggplot(aes(x = day, y = log10(Mean.Volume), group = 1)) +
    geom_line(size = 1.5, color = "steelblue") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    labs(x = "Day", y = "Log Total Volume", title = "Daily Seasonality")
}

grid.arrange(daily_seasonality, weekly_seasonality, monthly_seasonality, nrow = 1)

```

## Stationarity

Stationarity of the time series will be calculated using the Augmented Dickey Fuller test.

```{r stationarity, echo=FALSE, message=FALSE, warning=FALSE}
stationarity <- dat %>%
  select(Grain, Date) %>%
  group_by(Grain) %>%
  summarise(n = n()) %>%
  filter(n > 2 * freq)  %>%
  inner_join(dat, by = c("Grain")) %>%
  arrange(Grain, Date) %>%
  group_by(Grain) %>%
  mutate(Volume = ts(Volume, frequency = freq)) %>%
  summarise(is.stationary = adf.test(Volume)$p.value <= 0.05) 

stationarity %>%
  ggplot(aes(x = is.stationary, y = ..count../sum(..count..), fill = is.stationary)) +
  geom_bar(stat = "count", fill = "steelblue") +
  geom_text(stat='count', aes(label=paste0(round(..count..*100/sum(..count..),2),"%")), vjust=-0.25, size=3.5) +
  scale_y_continuous(labels=scales::percent) +
  labs(x = "Is Stationary", y = "% of Grains", title = "Stationarity")

grain.stationarity <- stationarity
```

Differencing helps in removing the trends in the data, this is a useful technique to trend-stationarize the data. User can apply any level of differencing but we will resort to just first and second differencing because if the time series can't be stationarize within two differencing it is highly unlikely that it might get stationarize with a higher order.

**First Differencing**

In order to difference the time series we subtract the value at Lag 1 from the original time series. Then we perform Augmented Dickey Fuller test to identify the stationarity of the time series.

```{r first_difference_stationarity, echo=FALSE, warning=FALSE}
first.diff_stationarity <- dat %>%
  select(Grain, Date) %>%
  group_by(Grain) %>%
  summarise(n = n()) %>%
  filter(n > 2 * freq)  %>%
  inner_join(dat, by = c("Grain")) %>%
  arrange(Grain, Date) %>%
  group_by(Grain) %>%
  mutate(Volume = ts(Volume - lag(Volume), frequency = freq)) %>%
  na.omit() %>%
  summarise(is.stationary = adf.test(Volume)$p.value <= 0.05) 

first.diff_stationarity %>%
  ggplot(aes(x = is.stationary, fill = is.stationary)) +
  geom_bar(stat = "count", fill = "steelblue") +
  labs(x = "Is Stationary", y = "Number of Grains", title = "Stationarity First Difference") +
  geom_text(stat='count', aes(label=..count..), vjust=-0.25, size=3.5)

grain.stationarity.first.diff <- first.diff_stationarity %>% select(Grain, stationarity.first.diff = is.stationary)
```

**Second Differencing**

In order to difference the time series we subtract the value at difference of LAG1 and LAG2 and subtract it from the first differenced time series. Then we perform Augmented Dickey Fuller test to identify the stationarity of the time series.

```{r second differencing, echo=FALSE, warning=FALSE}
second.diff_stationarity <- dat %>%
  select(Grain, Date) %>%
  group_by(Grain) %>%
  summarise(n = n()) %>%
  filter(n > 2 * freq)  %>%
  inner_join(dat, by = c("Grain")) %>%
  arrange(Grain, Date) %>%
  group_by(Grain) %>%
  mutate(Volume = ts((Volume - lag(Volume)) - (lag(Volume, 1) - lag(Volume, 2)), frequency = freq)) %>%
  na.omit() %>%
  summarise(is.stationary = adf.test(Volume)$p.value <= 0.05) 

second.diff_stationarity %>%
  ggplot(aes(x = is.stationary, fill = is.stationary)) +
  geom_bar(stat = "count", fill = "steelblue") +
  labs(x = "Is Stationary", y = "Number of Grains", title = "Stationarity Second Difference") +
  geom_text(stat='count', aes(label=..count..), vjust=-0.25, size=3.5)

grain.stationarity.second.diff <- second.diff_stationarity %>% select(Grain, stationarity.second.diff = is.stationary)
```

```{r fill_gaps, echo=FALSE}
dat.for.tsibble <- dat %>% select(Date, Grain, Volume)
dat.tsibble <- tsibble::as_tsibble(dat.for.tsibble, index = "Date", key = c("Grain"))

```

```{r stl_decompose, echo=FALSE}
# Calculate time series features
if(aggregation.frequency == "monthly"){
  dat.tsibble_stl <- dat.tsibble %>%
    mutate(Date = yearmonth((Date))) %>%
    select(Date, Grain, Volume)
}else if(aggregation.frequency == "weekly"){
  dat.tsibble_stl <- dat.tsibble %>%
    mutate(Date = yearweek((Date))) %>%
    select(Date, Grain, Volume)
}

# STL Features
stl.features <- dat.tsibble_stl %>% features(Volume, feat_stl)
```

## Trend and Seasonal Strength

* The graph is divided into four sections The mid point for these sections are defined if the Strength is 50%. This is denoted by the dashed lines. Please note this is a general graph. You might have a different requirement based on the case study.

* The quadrants are defined counter clock-wise i.e. numbering is same as the coordinate system.
  - Quadrant 1 - High Trend and High Seasonality.
  - Quadrant 2 - Low Trend and High Seasonality. 
  - Quadrant 3 - Low Trend and Low Seasonality.
  - Quadrant 4 - High Trend and Low Seasonality.

```{r strength_trend_season, echo=FALSE}
segments_stl_features <- stl.features %>% inner_join(abc_xyz, by = "Grain")
ggplot(segments_stl_features, aes(x = trend_strength,
                         y = seasonal_strength_year, color = abc_class)) +
  geom_point(size = 1) +
  geom_jitter() +
  geom_hline(yintercept = 0.5,
             linetype = 'dashed') +
  geom_vline(xintercept = 0.5,
             linetype = 'dashed') +
  scale_x_continuous(label = scales::percent,
                     expand = c(0, 0),
                     limits = c(0, 1.05)) +
  scale_y_continuous(label = scales::percent,
                     expand = c(0, 0),
                     limits = c(0, 1.05)) +
  labs(x = "Trend Strength",
       y = "Seasonal Strength",
       title = "Strength of Time Series")  

grain.stl.features <- segments_stl_features
```

## Trend in last 6 periods

In this section we perform a 6 period moving average on each of the Grain and try to assess the trends in the last 6 months, it is ussually observed that the business tend to be affected by the recent past, and trend in the period explains the movement of the time series. We cannot assess seasonality/cyclicity in recent past as it is  a part of longer term, specifically more than 2 years.

```{r trend_last_6p, echo=FALSE}
detrend.dat <- dat.tsibble %>%
  as.data.frame() %>%
  group_by(Grain) %>%
  mutate(trend = zoo::rollmean(Volume, 6, fill=NA, align = "right"))  %>%
  group_modify(~ tail(.x, 6)) %>%
  group_by(Grain) %>%
  filter(length(Grain)>= 6) %>%
  mutate(X = 1:length(Grain))

grain.trend.6.periods <- detrend.dat %>%
  select(Grain, trend, X) %>%
  group_by(Grain) %>%
  summarise(slope = coefficients(lm(trend~X))[2]) %>%
  mutate(slope = ifelse(slope > 0, "Positive Trend", ifelse(slope == 0, "No Trend", "Negative Trend")))

ggplot(grain.trend.6.periods, aes(x = slope, y = ..count../sum(..count..))) +
  geom_bar(stat = "count", fill = "steelblue") +
  labs(x = "", y = "% of Grains", title = "Trend in last 6 periods") +
  scale_y_continuous(labels=scales::percent) +
  geom_text(stat='count',
            aes(label=paste0(round(..count..*100/sum(..count..),2),"%")),
            vjust=-0.25, size=3.5, position = position_stack())
grain.trend.6.periods
```

## Intermittency

For intermittency, we have analyzed the number of zeros within each of the grain. As intermittency can cause issues while performing forecasts.

We calculate four summaries based on intermittency

1. Percentage of zeros in each time series (Graph 1).
2. Average Interval Between Non-zero Observations (Graph 2)
3. Percentage of data for each time series starts with zero (Graph 3) [Can help in identification of new products]
4. Percentage of data for each time series ends with zero (Graph 4) [Can help in identification of discontinued products]

**Tips**

* Lower the values accross all the four graphs, the less intermittent the time series will be.

* First Graph - If the percentage of zero exceeds 30% the time series will be considered as intermittent time series.

```{r intermittency, echo=FALSE}
dat.tsibble.intermittency <- tsibble::as_tsibble(dat.for.tsibble, index = "Date", key = c("Grain"))

rm(dat.for.tsibble)
intermittent.features <- dat.tsibble %>% features(Volume, feat_intermittent)

 intermittency.dat <- dat.tsibble.intermittency %>%
  as.data.frame() %>% 
  group_by(Grain) %>% 
  summarise(zero.percentage = round(sum(Volume == 0) * 100/length(unique(dat.tsibble$Date)), 2)) 

rm(dat.tsibble.intermittency)

p_intermittency <- intermittency.dat %>%
  ggplot(aes(x = zero.percentage)) +
  geom_histogram(binwidth = 10, fill = "steelblue") +
  labs(title = "Intermittency",
       x = "Zeros in Time Series",
       y = "Number of Grains")

p_zero_run_mean <- ggplot(intermittent.features, aes(x = zero_run_mean)) +
  geom_histogram(binwidth = 10, fill = "steelblue") +
  labs(x = "Average Zero Run",
       y = "Number of Grains",
       title = "Mean Interval Between Non-Zero Obs")

p_zero_start_prop <- ggplot(intermittent.features, aes(x = round(zero_start_prop*100,2))) +
  geom_histogram(binwidth = 10, fill = "steelblue") +
  labs(x = "Proportion of Zero Start",
       y = "Number of Grains",
       title = "Percentage of Data Starts with Zero")

p_zero_end_prop <- ggplot(intermittent.features, aes(x = round(zero_end_prop*100,2))) +
  geom_histogram(binwidth = 10, fill = "steelblue") +
  labs(x = "Percentage of Zero End",
       y = "Number of Grains",
       title = "Percentage of Data Ends with Zero")

grid.arrange(p_intermittency,
             p_zero_run_mean,
             p_zero_start_prop,
             p_zero_end_prop,
             nrow = 2)

grain.intermittency <- intermittency.dat %>%
  inner_join(intermittent.features, by = "Grain") %>%
  select(Grain, zero.percentage, zero_run_mean, zero_start_prop, zero_end_prop,)

```



## Multiplicative/Additive Time Series

* If the seasonality and residual components are independent of the trend, then you have an additive series. If the seasonality and residual components are in fact dependent, meaning they fluctuate on trend, then you have a multiplicative series.

* If you see high fluctuations in the time series with the trend, consider it to be multiplicative, if not the time series is additive. 

* Decomposition of time series, perform both additive and multiplicative decomposition of the time series, and analyze the results.

* Quantitative Approach - Assessment of Sum of Square of ACF. 

```{r compare_independency_of_residuals, echo=FALSE}
ssacf <- function(tss){
  sum.sq.acf <- sum((acf(tss, na.action = na.pass, plot = FALSE)$acf^2), na.rm = T)
  return(sum.sq.acf)
}

detrend.dat <- dat.tsibble %>%
  as.data.frame() %>%
  group_by(Grain) %>%
  mutate(trend = zoo::rollmean(Volume, 8, fill=NA, align = "right"),
         detrend_volume_additive = Volume - trend,
         detrend_volume_multiplicative = Volume/trend) 

deseasonal.dat <- detrend.dat %>%
  ungroup() %>%
  group_by(month = month(Date)) %>%
  mutate(seasonal_additive = mean(detrend_volume_additive, na.rm = T),
         seasonal_multiplicative = mean(detrend_volume_multiplicative, na.rm = T))


residuals.dat <- deseasonal.dat %>%
  ungroup() %>%
  mutate(res_additive = detrend_volume_additive - seasonal_additive,
         res_multiplicative = detrend_volume_multiplicative/seasonal_multiplicative) 

mult_add_ts <- residuals.dat %>%
  select(Grain, res_additive, res_multiplicative) %>%
  ungroup() %>%
  group_by(Grain) %>%
  mutate(type = ifelse(ssacf(res_additive) < ssacf(res_multiplicative), "Additive", "Multiplicative")) %>%
  select(Grain, type) %>%
  distinct()

mult_add_ts

grain.mult_add <- mult_add_ts

grain.date.trends <- detrend.dat %>%
  select(Grain, Date, trend)

grain.date.season <- deseasonal.dat %>%
  select(Grain, Date, seasonal_additive, seasonal_multiplicative)

grain.date.residual <- residuals.dat %>%
  select(Grain, Date, res_additive, res_multiplicative)

grain.date.decompose <- grain.date.trends %>%
  inner_join(grain.date.season %>% ungroup() %>% select(-month), by = c("Grain", "Date")) %>%
  inner_join(grain.date.residual, by = c("Grain", "Date")) %>%
  mutate(Date = as_date(Date))

rm(residuals.dat, deseasonal.dat, detrend.dat, grain.date.trends, grain.date.season, grain.date.residual)
```

## Outlier Detection

Uses supsmu for non-seasonal series and a robust STL decomposition for seasonal series. To estimate missing values and outlier replacements, linear interpolation is used on the (possibly seasonally adjusted) series. 

```{r outlier_detection, echo=FALSE, warning=FALSE}
outliers.treatment <- dat %>%
  group_by(Grain) %>%
  mutate(clean.volume = tsclean(ts(Volume, frequency = freq))) %>%
  mutate(is.outlier = (Volume != clean.volume)) %>%
  mutate(outlier_dev_percent = abs(Volume - clean.volume)*100/(Volume+1))
  

outliers <- outliers.treatment %>%
  group_by(Grain, is.outlier) %>%
  mutate(biggest.outlier = max(outlier_dev_percent)) %>%
  select(Date, Grain, is.outlier, biggest.outlier) %>%
  ungroup()


grain.biggest.outlier <- outliers %>%
  filter(is.outlier == TRUE) %>%
  group_by(Grain) %>%
  summarise(biggest.outlier = mean(biggest.outlier, na.rm = T))

grain.count.outliers <- outliers %>%
  group_by(Grain) %>%
  summarise(n.outliers = sum(is.outlier))

rm(outliers)

p1.grains <- ggplot(grain.count.outliers, aes(x = n.outliers > 0, y = ..count../sum(..count..))) +
  geom_bar(stat = "count", fill="steelblue") +
  labs(x = "Has Outliers",
       y = "% of Grains",
       title = "Grains with Outliers") +
  scale_y_continuous(labels=scales::percent) +
  geom_text(stat='count', aes(label=paste0(round(..count..*100/sum(..count..),2),"%")), vjust=-0.25, size=3.5)


p2.outliers.hist <- ggplot(grain.count.outliers, aes(x = n.outliers)) +
  geom_bar(fill = "steelblue", stat = 'count') +
  labs(x = "Number of Outliers", y = "Count", title = "Distribution of Outliers")

grid.arrange(p1.grains, p2.outliers.hist, nrow = 1)

grain.outliers <- grain.count.outliers %>%
  left_join(grain.biggest.outlier, by = "Grain") %>%
  mutate(biggest.outlier = ifelse(is.na(biggest.outlier),0,biggest.outlier))
```


## Clustering Time Series

This is a rough clustering method, we use the Dynamic Time Wrapping to compare the time series, also we define a total of nine clusters. A extension of finding the best clusters would be comparing multiple methods on multiple scores, thus estimating better aand natural clusters.

```{r find_similar_ts, echo=FALSE, warning=FALSE}

dat.list <- as.list(split(dat$Volume, dat$Grain))
grain.list <- names(dat.list)
clust <- dtwclust::tsclust(dat.list, k = 3, preproc = zscore, distance = "dtw_basic")

```

```{r Similar Grains, echo=FALSE, warning=FALSE}
cluster.number <- clust@cluster
clust.dat <- data.frame(Grain = grain.list, cluster = cluster.number)

ggplot(clust.dat, aes(x = factor(cluster.number))) +
  geom_bar(fill = "steelblue", stat = 'count') +
  labs(x = "Clusters", y = "Count", title = "Similar Time Series") +
  geom_text(stat='count', aes(label=..count..), vjust=-0.25, size=3.5)
```

**Grain Clusters**
```{r cluster_numbers, echo=FALSE, warning=FALSE}
grain.cluster <- clust.dat %>%
  arrange(cluster.number)

grain.cluster
```


## Naive Method - Moving Averages

It is quite important to come up with a baseline methodology for comparing more complex time series models. We will resort to Moving Averages and try to perform an appropriate method based on the frequency of the time series. 

For Monthly - 3MA and 6MA

For Weekly - 13MA and 26MA

For Daily - 91MA and 182MA

A better way to understand the above periods is thinking in terms of frequencies

Time Period for performing Moving average - frequency divided by 4 and frequency divided by 2. For example - Monthly data observed over an year would have frequency = 12, so we perform, m = 12/4 = 3, and m = 12/2 = 6, i.e. 3 month MA and 6 month MA models.

We use Accuracy as our parameter to assess the model performance. A simple way to understand the accuracy measure is to calculate first normalized Mean Absolute Error for each grain and then performing a weighted mean across the grain. 

You can manually calculate the accuracy for your other models by following these steps - 

1. Calculate Absolute Error for each of the data points in the data frame.

2. Calculate the Sum of Absolute Error.

3. Calculate the Sum of Actual Values.

4. Divide Sum of Absolute Error by Sum of Actuals.

5. Subtract the value from 1 to get the Accuracy.

6. Multiply the value by 100.

```{r naive_Accuracy, echo = FALSE, warning=FALSE}
data = data.table(dat.tsibble)
data = data[order(Grain,Date)]
# Change date format to standard ymd format
#data <- data[,Date := ymd(format(Date,'%Y-%m-%d'))]

error_per_snapshot = list()

for (cutoff_date in snapshots){
  
  #cutoff_date = snapshots[1]
  train = data[Date <= cutoff_date]
  test = data[Date > cutoff_date]
  
  ## Add lag mapping in test
  lag_maping = data.table(Date = sort(unique(test$Date)),
                          Lag = seq(1:length(unique(test$Date))))
  test = merge(test,lag_maping,by='Date')
  
  ## Get the naive forecast from training period - 3MA
  latest_3periods = train[,tail(.SD,3),by=.(Grain)]
  training_forecast_3ma = latest_3periods[,.(Forecast_3MA = mean(Volume)), by = .(Grain)]
  
  ## Get the naive forecast from training period - 6MA
  latest_6periods = train[,tail(.SD,6),by=.(Grain)]
  training_forecast_6ma = latest_6periods[,.(Forecast_6MA = mean(Volume)), by = .(Grain)]
  
  ## Get error metrics on test period - 3MA & 6MA
  test = merge(test,training_forecast_3ma,by='Grain')
  test[,Error_3MA := Forecast_3MA - Volume]
  test[,Abs_Error_3MA := abs(Error_3MA)]
  
  test = merge(test,training_forecast_6ma,by='Grain')
  test[,Error_6MA := Forecast_6MA - Volume]
  test[,Abs_Error_6MA := abs(Error_6MA)]
  
  single_period_validation = test[,.(Grain,Lag,Error_3MA,Abs_Error_3MA,Error_6MA,Abs_Error_6MA,Volume)]
  
  ## Keep only requested validation lags
  single_period_validation = single_period_validation[Lag %in% validation_single_period]
  
  ## Aggregate Period Validation
  agg_df = list()
  for (agg_period in validation_agg_period){
    #agg_period = 3
    test_forward_period = test[Lag <= agg_period]
    test_forward_period = test_forward_period[,.(Error_3MA = sum(Error_3MA),
                                                 Abs_Error_3MA = sum(Abs_Error_3MA),
                                                 Error_6MA = sum(Error_6MA),
                                                 Abs_Error_6MA = sum(Abs_Error_6MA),
                                                 Volume = sum(Volume)),
                                              by=.(Grain)]
    test_forward_period[,Lag := paste('Forward', as.character(agg_period))]
    
    # Save the dataframe in a list
    agg_df[[as.character(agg_period)]] = test_forward_period
  }
  
  agg_period_validation = bind_rows(agg_df)
  
  ## Join Single period and Aggregate period Validation
  validation_metrics = rbind(single_period_validation,agg_period_validation)
  validation_metrics[,Snapshot_date := cutoff_date]
  
  ## Add in a list
  error_per_snapshot[[as.character(cutoff_date)]] = validation_metrics
  
}

naive.models.dashboard = as_tibble(bind_rows(error_per_snapshot))

```


```{r acf_plots_data}
tss <- tapply(dat$Volume, dat$Grain, ts, frequency = freq)

acf_data <- NULL
for(name in names(tss)){
  if (length(tss[[name]]) == 1){
    acf_values = 1
    pacf_values = 1
  }else{
    acf_values <- acf(ts(tss[[name]], frequency = freq), lag.max = length(tss[[name]])-1, plot = FALSE)$acf %>% as.vector()
    pacf_values <- c(1, acf(ts(tss[[name]], frequency = freq), lag.max = length(tss[[name]]), plot = FALSE, type = "partial")$acf %>% as.vector())
  }
  if(is.null(acf_data)){
    acf_data <- data.frame(Grain = name, acf.lag = 0:(length(acf_values)-1), acf_value = acf_values, pacf_value = pacf_values)
    }else{
      acf_data <- rbind(acf_data, data.frame(Grain = name, acf.lag = 0:(length(acf_values)-1), acf_value = acf_values, pacf_value = pacf_values))
      }
}
```


## Algorithm Selection

Here we combine all the important properties of each time series, this will help us identifying the algorithms to use on particular case study.

Rules

1. Seasonal Strength = Low, Trend Strength = Low
    a. If the time series is stationary = AUTO ARIMA
    b. Else SES
    
2. Seasonal Strength = Low, Trend Strength = High
    Use Linear Trend Model (Models that can learn trend well)
    If the time series gets stationarize by first or second differencing and number of observations is atleast 3 times the frequency of time series then use ETS. 
  
3. Seasonal Strength = High, Trend Strength = Low
    Use Seasonal Models (Models that learn seasonality well)
    
    **Note: ETS also perform well on seasonal data but it would be beneficial to check it based on the performance of the model on the test data.**
    

4. Seasonal Strength = High, Trend Strength = High
    If the time series can be stationarized and it has more than data more than 3 x frequency of the time series and the nature of demand is smooth, ARIMA models would perform the best.
    
5. If the data is Erratic and Lumpy - Simple Moving Average should suffice.

6. If the data is intermittent (more than 30% of zeros) or has less number of observations - CROSTONS would be the best choice.

```{r combining_time_series_summary, echo=FALSE}

grain.stl.features.subset <- grain.stl.features  %>%
  select(Grain, trend_strength, seasonal_strength_year, seasonal_peak_year, seasonal_trough_year, linearity, curvature)

quick.summary.obs <- quick.summary %>%
  select(-stability)

# Summary Table
summary_table <- grain.abc_xyz %>%
  left_join(grain.cluster, by = "Grain") %>%
  left_join(quick.summary.obs, by = "Grain") %>%
  left_join(grain.stability, by = "Grain") %>%
  left_join(grain.trend.6.periods, by = "Grain") %>%
  left_join(grain.stationarity, by = "Grain") %>%
  left_join(grain.stationarity.first.diff, by = "Grain") %>%
  left_join(grain.stationarity.second.diff, by = "Grain") %>%
  left_join(grain.entropy, by = "Grain") %>%
  left_join(grain.demand.pattern, by = "Grain") %>%
  left_join(grain.outliers, by = "Grain") %>%
  left_join(grain.intermittency, by = "Grain") %>%
  left_join(grain.mult_add, by = "Grain") %>%
  left_join(grain.stl.features.subset, by = "Grain") %>%
  replace_na(list(is.stationary = FALSE, stationarity.first.diff = FALSE, stationarity.second.diff = FALSE, std.dev = -99999, CoV = -99999)) %>%
  mutate(rank_outliers = rank(-n.outliers, ties.method = "first"),
         rank_portfolio_share = rank(-volume.share, ties.method = "first"),
         rank_biggest_outlier = rank(-biggest.outlier, ties.method = "first")) %>%
  mutate(table_type = "Summary")

# Features for Sales Table from Summary Table
summary_table_subset <- summary_table %>%
  select(Grain, abc_class, xyz_class, rank_biggest_outlier, rank_biggest_outlier, rank_outliers)

summary_table_portfolio_ranking <- summary_table %>%
  select(Grain, rank_portfolio_share) 

# Acf Table
acf.table <- acf_data

# Sales Table
sales.table <- grain.date.decompose %>%
  left_join(outliers.treatment, by = c("Grain", "Date")) %>%
  inner_join(summary_table_subset, by = c("Grain")) %>%
  replace_na(list(trend = 0.0, res_additive = 0.0, res_multiplicative = 0.0,
                  Volume = 0.0, clean_volume = 0.0, is.outlier = FALSE)) %>%
  mutate(table_type = "Sales")


sales.table <- sales.table %>%
  arrange(Grain, Date) %>%
  group_by(Grain) %>%
  mutate(lag.period = 0:(n() - 1)) %>%
  inner_join(acf.table, by = c("Grain", "lag.period" = "acf.lag")) %>%
  inner_join(summary_table_portfolio_ranking, by = "Grain")

# Naive Forecasts
snapshots.naive.forecasts <- naive.models.dashboard %>%
  mutate(table_type = "Naive-Forecast") %>%
  inner_join(summary_table_portfolio_ranking, by = "Grain") %>%
  inner_join(summary_table_subset, by = c("Grain"))

# Combining data for dashboards
#dashboard_dataset <- bind_rows(summary_table, sales.table, snapshots.naive.forecasts)
```

```{r Algorithm Selection, eval=FALSE, include=FALSE}

algorithm.selection <- summary_table %>%
  mutate(trend_strength = ifelse(trend_strength < 0.5, "Low", "High"),
         seasonal_strength= ifelse(seasonal_strength_year < 0.5, "Low", "High"),
         linearity = ifelse(abs(linearity) > abs(curvature), "Linear", "Non-Linear")) %>%
  select(Grain, abc_class, xyz_class, cluster, observations,
         stability, is.stationary, stationarity.first.diff, stationarity.second.diff, forecastability, 
         demand.pattern, n.outliers, zero.percentage, type, trend_strength, seasonal_strength,
         seasonal_peak_year, seasonal_trough_year, linearity)

rules <- function(x){
  algorithm <- NULL
  trend_strength_index <- which(names(x) == "trend_strength")
  seasonal_strength_index <- which(names(x) == "seasonal_strength")
  
  stationary_index <- which(names(x) == "is.stationary")
  first_stationary_index <- which(names(x) == "stationarity.first.diff")
  second_stationary_index <- which(names(x) == "stationarity.second.diff")
  
  observations_index <- which(names(x) == "observations")
  demand_pattern_index <- which(names(x) == "demand.pattern")
  
  zero.percent_index <- which(names(x) == "zero.percentage")
  
  if((x[trend_strength_index] == "Low") & (x[seasonal_strength_index] == "Low")){
    algorithm <- "SES"
  }else if((x[trend_strength_index] == "Low") & (x[seasonal_strength_index] == "High")){
    algorithm <- "Seasonal Model"
  }else if((x[trend_strength_index] == "High") & (x[seasonal_strength_index] == "Low")){
    algorithm <- "Linear Trend Model"
    if((!(x[stationary_index] == "TRUE") | ((x[first_stationary_index] == "TRUE") | (x[second_stationary_index] == "TRUE"))) & (x[observations_index] >= 3 * freq)){
     algorithm <- "ETS"
    }
  }else if((x[trend_strength_index] == "High") & (x[seasonal_strength_index] == "High")){
    if((((x[stationary_index] == "TRUE") | (x[first_stationary_index] == "TRUE") | (x[second_stationary_index] == "TRUE"))) & (x[demand_pattern_index] == "Smooth") & (x[observations_index] >= 3 * freq)){
     algorithm <- "Auto Arima"
     }
  }
  
  if((x[demand_pattern_index] == "Erratic" | x[demand_pattern_index]  == "Lumpy") & (x[observations_index]  <= 2 * freq)){
    algorithm <- "Simple Moving Average"
   }

  if((x[observations_index] < freq) | (x[zero.percent_index] >= 30)){
    algorithm <- "Crostons"
  }

  if(is.null(algorithm)){
    algorithm <- "Simple Moving Average"
  }
  
  return(algorithm)
}

# No numbers, only information on the nature.
row_list <- split(algorithm.selection, seq(nrow(algorithm.selection)))
algorithm.selection$algorithm <- unlist(lapply(row_list, rules))

algorithm.selection
```

```{r}
{
#------------------Format Sales Table------------------#
summary_table <- data.table(summary_table)
sales.table <- data.table(sales.table)
snapshots.naive.forecasts <- data.table(snapshots.naive.forecasts)

sales.table = sales.table[,.(Grain,abc_class,xyz_class,rank_portfolio_share,rank_outliers,rank_biggest_outlier,
                             Date,Volume,clean.volume,is.outlier,trend,seasonal_additive,seasonal_multiplicative,
                             res_additive, res_multiplicative, lag.period, acf_value, pacf_value)]

# Remove null values from clean.volume
sales.table[,clean.volume := ifelse(is.na(clean.volume),Volume,clean.volume)]

# Check NA Values
anyNA(sales.table)

# Rename in required format
names(sales.table) = c('DD_GRAIN','DD_ABC_CLASS','DD_XYZ_CLASS',"DD_RANK_BY_PORTFOLIO_SHARE",
                       'DD_RANK_BY_NUM_OF_OUTLIERS','DD_RANK_BY_LARGEST_OUTLIER','DD_DATE','CT_VOLUME',
                       'CT_OUTLIER_TREATED_VOLUME','DD_IS_OUTLIER','CT_TREND_VAL','CT_SEASONAL_ADDITIVE_VALUE',
                       'CT_SEASONAL_MULTIPLICATIVE_VALUE','CT_RESIDUAL_ADDITIVE',
                       'CT_RESIDUAL_MULTIPLICATIVE', "DD_LAG", "CT_ACF", "CT_PACF")


#------------------Format Naive Accuracy Table------------------#

snapshots.naive.forecasts = snapshots.naive.forecasts[,.(Snapshot_date,Grain,abc_class,xyz_class,rank_portfolio_share,
                                                         Lag,Error_3MA,Abs_Error_3MA,Error_6MA,Abs_Error_6MA,Volume)] 

names(snapshots.naive.forecasts) = c('DD_SNAPSHOT_DATE','DD_GRAIN','DD_ABC_CLASS','DD_XYZ_CLASS',	'DD_RANK_BY_PORTFOLIO_SHARE','DD_LAG','CT_ERROR_3MA','CT_ABS_ERROR_3MA','CT_ERROR_6MA','CT_ABS_ERROR_6MA','CT_VOLUME')

#fwrite(snapshots.naive.forecasts,'For Report/3_naive_forecasts.csv')

#------------------Format Summary Table------------------#

format_bins = function(binned_values){
  #binned_values = summary_table$observations_bin
  
  # Extract the left & right part of the cut
  split_op = str_split_fixed(binned_values,',',2)
  first_bin = split_op[,1]
  second_bin = split_op[,2]
  first_partition = substr(first_bin,2,nchar(first_bin))
  second_partition = substr(second_bin,1,nchar(second_bin)-1)
  
  # Get the max value - to be used in padding
  max_value = max(as.numeric(second_partition))
  
  # Pad
  first_partition = ifelse(first_partition != '-1',str_pad(first_partition,nchar(max_value),pad="0"),'-1')
  second_partition = str_pad(second_partition,nchar(max_value),pad="0")
  
  formated_bins = paste0("(",first_partition,",",second_partition,"]")
  
  return(formated_bins)
}

summary_table[,trend_strength := round(trend_strength,2)]
summary_table[,seasonal_strength_year := round(seasonal_strength_year,2)]

summary_table[,observations_bin := cut(observations,breaks = c(-1,seq(0,max(observations)+4,by=5)))]
summary_table[,observations_bin := format_bins(observations_bin)]

summary_table[,zero_run_mean_bin := cut(zero_run_mean,breaks = c(-1,seq(0,max(zero_run_mean)+4,by=5)))]
summary_table[,zero_run_mean_bin := format_bins(zero_run_mean_bin)]

summary_table[,percentage_of_zeros := round(n.zeros*100/observations,1)]
summary_table[,percentage_of_zeros_bin := cut(percentage_of_zeros,breaks = c(-1,seq(0,max(percentage_of_zeros)+4,by=5)))]
summary_table[,percentage_of_zeros_bin := format_bins(percentage_of_zeros_bin)]


summary_table[,zero_start_prop := round(zero_start_prop*100)]
summary_table[,zero_start_prop_bin := cut(zero_start_prop,breaks = c(-1,seq(0,max(zero_start_prop)+4,by=5)))]
summary_table[,zero_start_prop_bin := format_bins(zero_start_prop_bin)]

summary_table[,zero_end_prop := round(zero_end_prop*100)]
summary_table[,zero_end_prop_bin := cut(zero_end_prop,breaks = c(-1,seq(0,max(zero_end_prop)+4,by=5)))]
summary_table[,zero_end_prop_bin := format_bins(zero_end_prop_bin)]

# Keep required columns
summary_table = summary_table[,.(Grain,abc_class,xyz_class,rank_portfolio_share,volume.share,min.date,max.date,observations_bin,stability,
                                 demand.pattern,is.stationary,trend_strength,seasonal_strength_year,slope,percentage_of_zeros_bin,
                                 zero_run_mean_bin,zero_start_prop_bin,zero_end_prop_bin,stationarity.first.diff,
                                 stationarity.second.diff,n.outliers,cluster)]

names(summary_table) = c('DD_GRAIN','DD_ABC_CLASS','DD_XYZ_CLASS','DD_RANK_BY_PORTFOLIO_SHARE','CT_VOLUME_SHARE','DD_MIN_DATE','DD_MAX_DATE',
                         'DD_LEN_OF_TS_BIN','DD_STABILITY','DD_DEMAND_PATTERN','DD_STATIONARY_LAG0','CT_TREND_STRENGTH','DD_SEASONAL_STRENGTH_YEAR',
                         'DD_TREND_IN_PAST_6_MONTHS','DD_PERCENT_ZEROS_IN_TS','DD_ZERO_RUN_MEAN','DD_ZERO_START_PROP','DD_ZERO_END_BIN',
                         'DD_STATIONARY_LAG1','DD_STATIONARY_LAG2','DD_NUM_OF_OUTLIERS','DD_CLUSTER')
}

# Write data for dashboards  
dir.create(paste(data_path,'output',sep = '/'))

fwrite(summary_table, paste(data_path,'output',"summary_table.csv",sep='/'))
fwrite(sales.table, paste(data_path,'output',"sales_table.csv",sep='/'))
fwrite(snapshots.naive.forecasts, paste(data_path,'output',"naive_forecasts.csv",sep='/'))

print(Sys.time() - time.start)

```
