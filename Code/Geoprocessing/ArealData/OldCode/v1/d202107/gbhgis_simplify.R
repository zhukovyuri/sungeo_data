# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/mpidr_preprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/mpidr_preprocess.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhuk","zhukov")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf",
                      "maptools",
                      "parallel",
                      "countrycode",
                      "data.table",
                      "deldir",
                      "tidyverse",
                      "rmapshaper")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)
library(SUNGEO)
check_sys_mapshaper()

###############################################
###############################################
###############################################
## GBHGIS
###############################################
###############################################
###############################################


# temp <- tempdir()

filez <- dir("Admin/GBHGIS/Raw") %>% .[grep("counties$",.)] %>% paste0("Admin/GBHGIS/Raw/",.) %>%list.files(pattern="shp$",full.names=T) %>% data.frame(path=.,yr = str_extract(.,"\\d{4}"),adm1=str_extract(.,"EW\\d{4}|S\\d{4}") %>% gsub("\\d{4}","",.)) %>% dplyr::mutate(ADM1_ISO2=adm1 %>% match(c("EW","S")) %>% c("GB-EAW","GB-SCT")[.],ADM1_NAME=adm1 %>% match(c("EW","S")) %>% c("England and Wales","Scotland")[.],ADM1_CODE=adm1 %>% match(c("EW","S")) %>% (1:2)[.]) %>% dplyr::arrange(yr) %>% .[!duplicated(paste0(.$yr,.$adm1)),]
filez

file_list <- mclapply(1:nrow(filez),function(f0){
  paste0(filez$ADM1_ISO2[f0]," ",filez$yr[f0]) %>% print()
  map <- st_read(filez$path[f0],quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ISO = "GBR", NAME_0 = countrycode("GBR","iso3c","country.name"), ID_0 = countrycode("GBR","iso3c","iso3n"), ADM1_ISO2 = filez$ADM1_ISO2[f0], NAME_1=filez$ADM1_NAME[f0], ID_1=filez$ADM1_CODE[f0], NAME_2 = G_NAME %>% as.character() %>% tolower() %>% tools::toTitleCase(), ID_2 = G_UNIT, YEAR = filez$yr[f0]) %>% data.table::setnames(c("ISO","NAME_0","ID_0","NAME_1","ID_1","NAME_2","ID_2"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE")) %>% dplyr::select(ADM0_ISO3,ADM1_ISO2,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,YEAR,geometry)
  map

  # Simplify
  {
    t1_ <- Sys.time()
    map <- rmapshaper::ms_simplify(map,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
    object.size(map) %>% format(units="Mb") %>% paste0(filez$yr[f0]," adm0 ",.) %>% print()
    t2_ <- Sys.time()
    print(t2_-t1_)
  }
  # Save
  dir("Admin/GBHGIS/Simplified")
  write_sf(map,dsn=paste0("Admin/GBHGIS/Simplified/GBHGIS_",filez$ADM1_ISO2[f0],"_",filez$yr[f0],"_ADM2_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)


},mc.cores=8)
