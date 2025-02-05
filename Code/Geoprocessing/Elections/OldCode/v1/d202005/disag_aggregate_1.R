# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/disag_aggregate_1.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/disag_aggregate_1.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%"ubu"){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%"zhukov"){setwd("~/Dropbox/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
library(SUNGEO)



#############################################
# Transfer files
#############################################

# List of source directories
dirz <- list.dirs("../../SanGEO\ Files", full.names=T, recursive = F) %>% data.frame(dir_in = ., stringsAsFactors = F) %>% mutate(iso3 = .$dir_in%>%(function(x){gsub("../../SanGEO\ Files/","",x) %>% countrycode("country.name","iso3c") %>% replace_na("XKX")})) %>% as.data.table()

# Create target directories (if none exist)
if(!dir.exists("Elections/Micro")){dir.create("Elections/Micro")}
paste0("Elections/Micro/",dirz$iso3) %>% subset(!dir.exists(.)) %>% for(i in .){dir.create(i)}
dir("Elections/Micro")

# Temp directory
tempd <- tempdir()

#################
# Australia
#################

iso_ <- "AUS"

# # 2001 (zip), 2004, 2007, 2010
# con_i0 <- dir(dirz[iso3%in%iso_,dir_in]) %>% subset(grepl(".zip$",.)) %>% paste0(dirz[iso3%in%iso_,dir_in],"/",.) %>% unzip(zipfile = ., exdir = tempd)
# con_i0 # ok. come back to this later...

##
# 2004
##

i0 <- 1
aus_2004 <- dir(dirz[iso3%in%iso_,dir_in]) %>% subset(grepl(".csv$",.)) %>% .[i0] %>% paste0(dirz[iso3%in%iso_,dir_in],"/",.) %>% fread(.)
aus_2004 %>% data.table::setnames(names(.), names(.) %>% gsub("[^[:alnum:] ]| ","_",.)  %>% gsub("_Percentage","_pvs1",.)  %>% gsub("_Votes","_pv1",.)) %>% data.table::setnames("TotalVotes","vv1")
aus_2004

# Geocode
addrz <- aus_2004[,paste0(PollingPlace,", ",DivisionNm,", Australia")]
aus_2004_geo_gn <- geocode_gn(query= aus_2004$PollingPlace, country_iso3 = "AUS", str_meth="lv", verbose=TRUE, details = TRUE , match_all = TRUE, ncores = detectCores()-1) 
aus_2004_geo_osm <- geocode_osm_batch(addrz, verbose=TRUE, details = TRUE, return_all = TRUE)
save(aus_2004_geo_gn,aus_2004_geo_osm,file="Elections/Micro/AUS/AUS2004_geo.RData")
