# Compute statistics for each stop in Berlin/Brandenburg
# (e.g. average hourly departures). 

library(tidyverse)
library(lubridate)
library(sf)
library(geojsonsf)
library(dtplyr)
library(data.table)

# prepare list for each day
day_list <- c("0912", "0913", "0914", "0915", "0916", "0917", "0918")

# prepare function to switch to next day if departure is later than midnight
wd_po <- function(weekday){
  if(weekday == "Montag"){
    return("Dienstag")
  } else if(weekday == "Dienstag"){
    return("Mittwoch")
  } else if(weekday == "Mittwoch"){
    return("Donnerstag")
  } else if(weekday == "Donnerstag"){
    return("Freitag")
  } else if(weekday == "Freitag"){
    return("Samstag")
  } else if(weekday == "Samstag"){
    return("Sonntag")
  } else if(weekday == "Sonntag"){
    return("Montag")
  }
}

# load manual blacklist of broken stations
blacklist <- read_lines("data/blacklist_ids.txt")

# load list of stations to rename 
rename <- read_csv("data/rename.csv")

rename_list = rename$new_name
names(rename_list) <- rename$stop_id

rename_municipality <- rename %>% 
  select("stop_name" = "new_name", "municipality" = "new_municipality") %>% 
  distinct()

# evaluate each day and put everything in one long tibble

stats_long <- lapply(day_list, function(input_day){
  # load stop_times for current day
  input_data = fread(str_c("data/stop_times_", input_day, ".csv"))

  # filter or rename broken stops
  input_cor <- as_tibble(input_data) %>% 
    filter(!(stop_id %in% blacklist)) %>% 
    rowwise %>% 
    mutate(stop_name = ifelse(stop_id %in% names(rename_list), rename_list[[stop_id]], stop_name)) %>% 
    as_tibble()

  
  # Calculate departures per day
  daily <- lazy_dt(input_cor) %>% 
    group_by(stop_name) %>% 
    summarize(dep_per_day = n()) %>% 
    as_tibble()
  
  # Calculate departures per hour
  hourly <- lazy_dt(input_cor) %>% 
    group_by(stop_name, dep_hour) %>% 
    summarize(dep_per_hour = n()) %>% 
    as_tibble()
  

  # calculate average hourly departures for time from 6am to 8pm
  hourly_avg <- lazy_dt(hourly) %>% 
    filter((dep_hour >= 6) & (dep_hour < 20)) %>% 
    group_by(stop_name) %>% 
    summarize(dep_per_hour_avg = sum(dep_per_hour) / 14) %>% 
    as_tibble()
  
  # put it all together
  stats_long <- daily %>% 
    left_join(hourly_avg, by = "stop_name") %>% 
    full_join(hourly, by = "stop_name") %>% 
    mutate(weekday = input_day) %>% 
    relocate(stop_name, weekday)
  
  }) %>% bind_rows %>% 
  arrange(stop_name, weekday)

# Join statistics with station data/coords
stations_raw <- fread("data/stations_coords.csv")

stations_to_join <- lazy_dt(stations_raw) %>% 
  mutate(lat = str_extract(geometry, "\\|.*") %>% str_remove("\\|") %>% as.double(),
         lon = str_extract(geometry, ".*\\|") %>% str_remove("\\|") %>% as.double()) %>% 
  select(stop_name, lat, lon, "municipality" = "GEN", AGS) %>% 
  as_tibble() %>% 
  # attach renamed stops (at this time only name and municipality)
  bind_rows(rename_municipality)

stats_full_long <- stats_long %>% 
  left_join(stations_to_join) %>% 
  relocate(stop_name, municipality, AGS, lat, lon, weekday, 
           dep_per_day, dep_per_hour_avg, dep_hour, dep_per_hour) %>% 
  mutate(weekday = str_replace_all(weekday, c(
    "0912" = "Montag", "0913" = "Dienstag", "0914" = "Mittwoch",
    "0915" = "Donnerstag", "0916" = "Freitag",
    "0917" = "Samstag", "0918" = "Sonntag"
    )))
  
# fwrite(stats_full_long, "data/temp_stats_long.csv")

# correct departure times later than midnight
hours_corrected <- stats_full_long %>% 
  rowwise %>% 
  mutate(
    dep_hour_cor = ifelse(dep_hour > 23, dep_hour - 24, dep_hour),
    weekday_cor = ifelse(dep_hour > 23, wd_po(weekday), weekday)
  )

# calculate absolute departures for each hour of the day
hour_counts <- hours_corrected %>% 
  group_by(stop_name, weekday_cor, dep_hour_cor) %>% 
  summarize(dep_per_hour_cor = sum(dep_per_hour))

# fwrite(hour_counts, "data/temp_hour_counts.csv")

# Keep only data valid per station and day - get rid of wrong hour counts
stats_long_short <- stats_full_long %>% 
  select(-dep_hour, -dep_per_hour) %>% 
  distinct()

# prepare data for joining, so that every day has the basics even when there
# are no departures on a certain day
week_scheme <- stats_long_short %>% 
  select(stop_name, municipality, AGS, lat, lon) %>% 
  group_by(stop_name, municipality, AGS, lat, lon) %>% 
  summarize(weekday = c("Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"))

stats_long_short_week <- week_scheme %>% 
  left_join(stats_long_short, by=c("stop_name", "municipality", "AGS", "lat", "lon", "weekday"))

# Add corrected hour counts
stats_long_corrected <- hour_counts %>% 
  rename("weekday" = "weekday_cor") %>% 
  left_join(stats_long_short_week, by=c("stop_name", "weekday"))

# write long format data
fwrite(stats_long_corrected, "data/stats_long_corrected.csv")
