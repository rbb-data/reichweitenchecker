# Calculate travel times from all stops to all city centres

# Current state:
# - The city centre stop "Kostrzyn (PL), Bahnhof" does not exist (not sure why)
# - Runs:
#     - "wednesday" / "day": memory exhausted for city centre stops "S+U Berlin Hauptbahnhof",
#       "S Potsdam Hauptbahnhof", "Falkensee, Bahnhof", "S Hennigsdorf Bhf", "S Hoppegarten",
#     - "saturday" / "day": memory exhausted for city centre stop "S+U Berlin Hauptbahnhof"
#     - "sunday" / "day": memory exhausted for city centre stop "S+U Berlin Hauptbahnhof"
# - Routing failed only when an extended time setting was specified (8am-8pm), data is available
#   though for a more constrained time setting (8am-noon)

# install.packages('tidyverse')
# install.packages('tidytransit')
# install.packages('sf')
# install.packages('geojsonsf')
# install.packages('jsonlite')
# install.packages('dtplyr')

library(tidyverse)
library(tidytransit)
library(parallel)
library(readr)
library(sf)
library(geojsonsf)


# Konstanten
args <- commandArgs(trailingOnly = TRUE)
DAY_NAME <- args[1]
DAY_TIME <- args[2]

if (DAY_NAME == "wednesday") {
  DAY <- "2023-05-24"
} else if (DAY_NAME == "saturday") {
  DAY <- "2023-05-27"
} else if (DAY_NAME == "sunday") {
  DAY <- "2023-05-28"
} else {
  print("Invalid day")
  quit()
}

# START_TIME: earliest departure time
# END_TIME: latest arrival time

if (DAY_TIME == "day") {
  START_TIME <- 8 * 3600
  END_TIME <- 20 * 3600
} else if (DAY_TIME == "night") {
  START_TIME <- 20 * 3600
  END_TIME <- 24 * 3600 - 1
} else {
  print("Invalid time")
  quit()
}

# 1 hour
MAX_TRAVEL_TIME <- 60 * 60

TARGET_DIR <- sprintf(
  "data/travel_times_%s_%s_arrival",
   DAY_NAME,
   DAY_TIME
)

# create target dir if it doesn't exist
dir.create(TARGET_DIR)


print("Reading GTFS...")

gtfs_de <- read_gtfs("data/20230109_preprocessed.zip", quiet = FALSE)


print("Generating stop names...")
cities = read.csv(file = "data/Public-Transport-2023-cities.csv")
unique_stop_names <- unique(cities$stop_name)
print(unique_stop_names)

# rm(geo_nrw, all_stops, nrw_stops)

# Filter existing files
# existing_files <- list.files(sprintf("data/travel_times_%s", DAY_NAME), "*.csv", full.names = FALSE)
# existing_files <- gsub(".csv", "", existing_files)
# existing_files <- lapply(existing_files, URLdecode)
# unique_stop_names <- unique_stop_names[!(unique_stop_names %in% existing_files)]


print("Computing stop times...")
# Prepare optimized datastructure for RAPTOR
stop_times <-
  filter_stop_times(gtfs_de, DAY, START_TIME, END_TIME)

rm(gtfs_de)

# Cluster setup
gc()

print("Setting up cluster...")
no_cores <- 2 # detectCores() / 2
cl <- makeCluster(no_cores, type = "FORK")

print("Running...")
# Run all stops in cluster
failures <-
  parLapply(cl, unique_stop_names, function(stop_name) {
    tryCatch(
      {
        tt <-
          travel_times(
            stop_times,
            stop_name,
            arrival = TRUE,
            stop_dist_check = FALSE,
            time_range = END_TIME - START_TIME,
            return_coords = TRUE,
            max_transfers = 3
          )
        write.csv(
          tt[tt$travel_time <= MAX_TRAVEL_TIME, ],
          sprintf(
            "%s/%s.csv",
            TARGET_DIR,
            URLencode(stop_name, reserved = TRUE, repeated = TRUE)
          )
        )
      },
      error = function(e) {
        sprintf("%s errored: %s", stop_name, e)
      },
      warning = function(w) {
        sprintf("%s warned: %s", stop_name, w)
      }
    )
  })

stopCluster(cl)

failures_unlist <- unlist(failures)
write.csv(failures_unlist, sprintf("data/fails_%s_%s.csv", DAY_NAME, DAY_TIME))
