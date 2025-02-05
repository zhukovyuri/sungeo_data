# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/nhgis_aggregate_1.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/nhgis_aggregate_1.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","V8","rmapshaper")
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
check_sys_mapshaper()



###############################################
###############################################
###############################################
## SUNGEO HGIS
###############################################
###############################################
###############################################

# Example file
dir("Admin/GAUL/Simplified/")
map_gaul <- sf::st_read("Admin/GAUL/Simplified/GAUL_TZA_1993_ADM2_wgs.geojson")
map_gadm <- sf::st_read("Admin/GADM/Simplified/GADM_TZA_2018_ADM2_wgs.geojson")
intersect(names(map_gadm),names(map_gaul))

# File list
filez_in <- "Admin/SUNGEO_HGIS/Admin/" %>% (function(x){dir(x) %>% grep("^[[:alpha:]]{3}\\b",.,value=T) %>% paste0(x,.)}) %>% list.files(pattern="wgs\\.geojson$|\\d{4}\\.geojson|v2\\.geojson",full.names=T)

# Numeric codes
i0 <- 93
num_codez <- mclapply(1:length(filez_in),function(i0){
	map_ <- sf::st_read(filez_in[i0],quiet=T)
	data.table(
		f0=filez_in[i0],
		ADM0_ISO3=filez_in[i0] %>% str_split("/",simplify=T) %>% .[,4],
		ADM0_NAME=as.character(map_$NAME_0),
		ADM0_CODE=NA_real_,
		ADM1_NAME=ifelse(rep((filez_in[i0] %>% str_split("_",simplify=T) %>% .[,3]) %in% c("ADM1","ADM2","ADM3"),nrow(map_)),as.character(map_$NAME_1),NA_character_),
		ADM1_CODE=NA_real_,
		ADM2_NAME=ifelse(rep((filez_in[i0] %>% str_split("_",simplify=T) %>% .[,3]) %in% c("ADM2","ADM3"),nrow(map_)),as.character(map_$NAME_2),NA_character_),
		ADM2_CODE=NA_real_,
		ADM3_NAME=ifelse(rep((filez_in[i0] %>% str_split("_",simplify=T) %>% .[,3]) %in% c("ADM3"),nrow(map_)),as.character(map_$NAME_3),NA_character_),
		ADM3_CODE=NA_real_
	)
},mc.cores=detectCores()) %>% dplyr::bind_rows() %>% .[,ADM0_CODE := ADM0_NAME %>% as.factor() %>% as.numeric()] %>% .[!is.na(ADM1_NAME),ADM1_CODE := paste(ADM0_NAME,ADM1_NAME) %>% as.factor() %>% as.numeric()] %>% .[!is.na(ADM2_NAME),ADM2_CODE := paste(ADM0_NAME,ADM1_NAME,ADM2_NAME) %>% as.factor() %>% as.numeric()] %>% .[!is.na(ADM3_NAME),ADM3_CODE := paste(ADM0_NAME,ADM1_NAME,ADM2_NAME,ADM3_NAME) %>% as.factor() %>% as.numeric()]
num_codez

# Temp directory
temp <- tempdir()

# Loop over years
print("Prepping temp files")
y0 <- 79
yrz_list <- mclapply(1:length(filez_in),function(y0){
	print(filez_in[y0])
	# Load 
	map_ <- sf::st_read(filez_in[y0],quiet=T)
	map_$id <- NULL
	st_is_valid(map_,reason=T)
	num_ <- num_codez[f0%in%filez_in[y0]]
	num_ %>% as.data.frame()
	map_ <- map_ %>% data.table::setnames(c("NAME_0","NAME_1","NAME_2","NAME_3"),c("ADM0_NAME","ADM1_NAME","ADM2_NAME","ADM3_NAME"),skip_absent=TRUE)
	map_$VERSION <- paste0("SUNGEO HGIS 1.0")
	map_$ADM0_ISO3 <- num_$ADM0_ISO3[1]
	map_$ADM0_CODE <- match(map_$ADM0_NAME,num_$ADM0_NAME) %>% num_[.,ADM0_CODE]
	if((filez_in[y0] %>% str_split("_",simplify=T) %>% .[,3]) %in% c("ADM1","ADM2","ADM3")){
		map_$ADM1_CODE <- match(map_$ADM1_NAME,num_$ADM1_NAME) %>% num_[.,ADM1_CODE]		
	}
	if((filez_in[y0] %>% str_split("_",simplify=T) %>% .[,3]) %in% c("ADM2","ADM3")){
		map_$ADM2_CODE <- match(map_$ADM2_NAME,num_$ADM2_NAME) %>% num_[.,ADM2_CODE]		
	}
	if((filez_in[y0] %>% str_split("_",simplify=T) %>% .[,3]) %in% c("ADM3")){
		map_$ADM3_CODE <- match(map_$ADM3_NAME,num_$ADM3_NAME) %>% num_[.,ADM3_CODE]		
	}
	if(!"COMMENT"%in%names(map_)){map_$COMMENT <- ""}
	# map_ %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,ADM3_NAME,ADM3_CODE,VERSION,SOURCE,COMMENT,geometry,everything())
	# Select columns
	map_ <- map_ %>% dplyr::select(c(dplyr::one_of("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE","ADM3_NAME","ADM3_CODE","VERSION","SOURCE","COMMENT"),everything()))

	# Save
	dir("Admin/SHGIS/Simplified")
	sf::write_sf(map_,dsn=paste0("Admin/SHGIS/Simplified/SHGIS_",num_$ADM0_ISO3[1],"_",(filez_in[y0] %>% str_split("_",simplify=T) %>% .[,4]),"_",(filez_in[y0] %>% str_split("_",simplify=T) %>% .[,3]),"_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	

},mc.cores=8)

