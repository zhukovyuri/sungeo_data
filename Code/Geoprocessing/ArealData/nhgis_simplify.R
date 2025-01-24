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


###############################################
###############################################
###############################################
## NHGIS
###############################################
###############################################
###############################################


# Lists of NHGIS polygons
adm1_filez <- dir(paste0("Admin/IPUMS_NHGIS/USA/state")) %>% subset(grepl("_tl\\d{4}",.)) %>% .[!(dir(paste0("Admin/IPUMS_NHGIS/USA/state")) %>% subset(grepl("_tl\\d{4}",.)) %>% str_split("_|\\.zip", simplify = TRUE) %>% .[,6] %>% duplicated(fromLast=TRUE))]
adm2_filez <- dir(paste0("Admin/IPUMS_NHGIS/USA/county")) %>% subset(grepl("_tl\\d{4}",.)) %>% .[!(dir(paste0("Admin/IPUMS_NHGIS/USA/county")) %>% subset(grepl("_tl\\d{4}",.)) %>% str_split("_|\\.zip", simplify = TRUE) %>% .[,6] %>% duplicated(fromLast=TRUE))]

# Loop over years
yrz <- adm2_filez %>% str_split("_|\\.zip", simplify = TRUE) %>% .[,6] %>% as.numeric() %>% sort()
k0 <- 24
for(k0 in 24:length(yrz)){
	print(paste0(k0,"/",length(yrz)," ",yrz[k0]))

	# Load polygons
	temp <- tempdir()
	map1_ <- paste0("Admin/IPUMS_NHGIS/USA/state/",adm1_filez) %>% subset(grepl(paste0(yrz[k0],".zip$"),.)) %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% data.table::setnames(c("STATENAM","GISJOIN2"),c("ADM1_NAME","ADM1_CODE"),skip_absent=TRUE) %>% data.table::setnames(c("NAME00","STATEFP00","NAMELSAD00","CNTYIDFP00"),c("ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=TRUE) %>% data.table::setnames(c("NAME10","STATEFP10","NAMELSAD10","GEOID10"),c("ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=TRUE) %>% data.table::setnames(c("NAME","STATEFP","NAMELSAD","GEOID"),c("ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=TRUE)
	map2_ <- paste0("Admin/IPUMS_NHGIS/USA/county/",adm2_filez) %>% subset(grepl(paste0(yrz[k0],".zip$"),.)) %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% data.table::setnames(c("STATENAM","NHGISNAM","NHGISST","GISJOIN2"),c("ADM1_NAME","ADM2_NAME","ADM1_CODE","ADM2_CODE"),skip_absent=TRUE) %>% data.table::setnames(c("NAME00","STATEFP00","NAMELSAD00","CNTYIDFP00"),c("ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=TRUE) %>% data.table::setnames(c("NAME10","STATEFP10","NAMELSAD10","GEOID10"),c("ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=TRUE) %>% data.table::setnames(c("NAME","STATEFP","NAMELSAD","GEOID"),c("ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=TRUE)
	if(yrz[k0]>=2000){
		map2_$ADM1_NAME <- map1_$ADM1_NAME[match(map2_$ADM1_CODE,map1_$ADM1_CODE)] %>% as.character()
	}


	# Simplify polygons
	# map1_sp <- map1_ %>% as("Spatial")
	# map2_sp <- map2_ %>% as("Spatial")
	{
	t1_ <- Sys.time()
	map1_ <- rmapshaper::ms_simplify(map1_,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	map2_ <- rmapshaper::ms_simplify(map2_,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	t2_ <- Sys.time()
	print(t2_-t1_)
	}
	# object.size(map2_) %>% format(units="Mb")

	# Dissolve
	map0_ <- map1_ %>% rmapshaper::ms_dissolve(sys=T)

	# Re-order varibles
	map0_ <- map0_ %>% dplyr::mutate(ADM0_ISO3 = "USA", ADM0_NAME = "United States") %>% dplyr::select(ADM0_ISO3,ADM0_NAME,geometry)
	map1_ <- map1_ %>% dplyr::mutate(ADM0_ISO3 = "USA", ADM0_NAME = "United States") %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM1_CODE,ADM1_NAME,geometry)
	map2_ <- map2_ %>% dplyr::mutate(ADM0_ISO3 = "USA", ADM0_NAME = "United States") %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM1_CODE,ADM1_NAME,ADM2_CODE,ADM2_NAME,geometry)

	# Save
	dir("Admin/IPUMS_NHGIS/Simplified")
	write_sf(map0_,dsn=paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_USA_",yrz[k0],"_ADM0_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	write_sf(map1_,dsn=paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_USA_",yrz[k0],"_ADM1_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	write_sf(map2_,dsn=paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_USA_",yrz[k0],"_ADM2_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)

}