# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_preprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_preprocess.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhukov","zhuk")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}

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
# install.packages("devtools")
# library(devtools)
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
library(SUNGEO)



###################################################
# Explore Marty's data
###################################################

elec_list <- dir("Elections/eForensics/martydav/SunGEO_Data_Repository/Elections/")
i <- 6
elec_list[i]

dir(paste0("Elections/eForensics/martydav/SunGEO_Data_Repository/Elections/",elec_list[i],"/Eforensics_Disaggregate_Data/"))

# Disaggregated
efor_disag <- fread(paste0("Elections/eForensics/martydav/SunGEO_Data_Repository/Elections/",elec_list[i],"/Eforensics_Disaggregate_Data/",gsub("_\\d{4}","",elec_list[i]),"_Eforensics_Estimates.csv"))
efor_disag

# Shapefile
efor_shp <- sf::st_read(paste0("Elections/eForensics/martydav/SunGEO_Data_Repository/Elections/",elec_list[i],"/Shapefile/",elec_list[i],"_w_EForensicsEstimates.shp")) %>% st_transform(st_crs("EPSG:4326")) %>% data.table::setnames(tolower(names(.)))
efor_shp
plot(efor_shp['pfraud'])

# WKT
efor_gjs <- fread(paste0("Elections/eForensics/martydav/SunGEO_Data_Repository/Elections/",elec_list[i],"/WKT_File/",elec_list[i],"_w_EForensicsEstimates.txt")) %>% st_as_sf(wkt = "WKT_Output")
plot(efor_gjs['pfraud'])

?st_as_sf
