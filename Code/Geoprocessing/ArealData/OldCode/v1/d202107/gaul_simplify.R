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
## GAUL
###############################################
###############################################
###############################################


temp <- tempdir()

# Loop over years
print("Prepping temp files")
y0 <- 24
yrz <- 1990:2014
yrz_list <- mclapply(1:length(yrz),function(y0){
	print(yrz[y0])

	# Load GAUL
	map0 <- paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_0.zip") %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ADM0_ISO3 = countrycode(ADM0_NAME,"country.name","iso3c")) %>% tidyr::replace_na(list(ADM0_ISO3 = "ZZZ")) %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,STATUS,DISP_AREA,geometry)
	map0$ADM0_ISO3[map0$ADM0_NAME%in%"Serbia and Montenegro"] <- "SRB"
	map1 <- paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_1.zip") %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ADM0_ISO3 = countrycode(ADM0_NAME,"country.name","iso3c")) %>% tidyr::replace_na(list(ADM0_ISO3 = "ZZZ")) %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,STATUS,DISP_AREA,geometry)
	map1$ADM0_ISO3[map1$ADM0_NAME%in%"Serbia and Montenegro"] <- "SRB"
	map2 <- paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_2.zip") %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ADM0_ISO3 = countrycode(ADM0_NAME,"country.name","iso3c")) %>% tidyr::replace_na(list(ADM0_ISO3 = "ZZZ")) %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,STATUS,DISP_AREA,geometry)
	map2$ADM0_ISO3[map2$ADM0_NAME%in%"Serbia and Montenegro"] <- "SRB"

	# Simplify polygons
	{
	t1_ <- Sys.time()
	map0_ <- rmapshaper::ms_simplify(map0,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	map1_ <- rmapshaper::ms_simplify(map1,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	map2_ <- rmapshaper::ms_simplify(map2,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	t2_ <- Sys.time()
	print(t2_-t1_)
	}
	# object.size(map0_) %>% format(units="Mb")
	# object.size(map1_) %>% format(units="Mb")
	# object.size(map2_) %>% format(units="Mb")
	# plot(map2_["ADM0_ISO3"],lwd=.1); dev.off()

	cntz <- map0_$ADM0_ISO3 %>% unique() %>% sort()
	for(k0 in 1:length(cntz)){
		# Subset
		map0_k <- map0_[map0_$ADM0_ISO3%in%cntz[k0],]
		map1_k <- map1_[map1_$ADM0_ISO3%in%cntz[k0],]
		map2_k <- map2_[map2_$ADM0_ISO3%in%cntz[k0],]
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map2_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GAUL/Simplified")
		write_sf(map0_k,dsn=paste0("Admin/GAUL/Simplified/GAUL_",cntz[k0],"_",yrz[y0],"_ADM0_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
		write_sf(map1_k,dsn=paste0("Admin/GAUL/Simplified/GAUL_",cntz[k0],"_",yrz[y0],"_ADM1_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
		write_sf(map2_k,dsn=paste0("Admin/GAUL/Simplified/GAUL_",cntz[k0],"_",yrz[y0],"_ADM2_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}

},mc.cores=8)



# Fix broken geometries
cntz_fix <- c("SYR")

y0 <- 1
yrz <- 1990:2014
yrz_list <- mclapply(1:length(yrz),function(y0){
	print(yrz[y0])

	# Load GAUL
	map0 <- paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_0.zip") %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ADM0_ISO3 = countrycode(ADM0_NAME,"country.name","iso3c")) %>% tidyr::replace_na(list(ADM0_ISO3 = "ZZZ")) %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,STATUS,DISP_AREA,geometry)
	map0$ADM0_ISO3[map0$ADM0_NAME%in%"Serbia and Montenegro"] <- "SRB"
	map1 <- paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_1.zip") %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ADM0_ISO3 = countrycode(ADM0_NAME,"country.name","iso3c")) %>% tidyr::replace_na(list(ADM0_ISO3 = "ZZZ")) %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,STATUS,DISP_AREA,geometry)
	# map1$ADM0_ISO3[map1$ADM0_NAME%in%"Serbia and Montenegro"] <- "SRB"
	map2 <- paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_2.zip") %>% unzip(exdir = temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% dplyr::mutate(ADM0_ISO3 = countrycode(ADM0_NAME,"country.name","iso3c")) %>% tidyr::replace_na(list(ADM0_ISO3 = "ZZZ")) %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,STATUS,DISP_AREA,geometry)
	map2$ADM0_ISO3[map2$ADM0_NAME%in%"Serbia and Montenegro"] <- "SRB"

	# Subset
	map0 <- map0[map0$ADM0_ISO3%in% cntz_fix,]
	map1 <- map1[map1$ADM0_ISO3%in% cntz_fix,]
	map2 <- map2[map2$ADM0_ISO3%in% cntz_fix,]

	# Simplify polygons
	{
	t1_ <- Sys.time()
	map0_ <- rmapshaper::ms_simplify(map0,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	map1_ <- rmapshaper::ms_simplify(map1,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	map2_ <- rmapshaper::ms_simplify(map2,keep=.05,keep_shapes=T,sys=T) %>% fix_geom()
	# map2_ <- map2 %>% fix_geom()
	t2_ <- Sys.time()
	print(t2_-t1_)
	}

	# sf::st_is_empty(map0_)
	# sf::st_is_empty(map1_)
	# sf::st_is_empty(map2_)

	# plot(map2_["ADM2_NAME"])

	# object.size(map0_) %>% format(units="Mb")
	# object.size(map1_) %>% format(units="Mb")
	# object.size(map2_) %>% format(units="Mb")
	# plot(map2_["ADM0_ISO3"],lwd=.1); dev.off()

	# cntz <- map0_$ADM0_ISO3 %>% unique() %>% sort()
	cntz <- map2_$ADM0_ISO3 %>% unique() %>% sort()
	for(k0 in 1:length(cntz)){
		# Subset
		map0_k <- map0_[map0_$ADM0_ISO3%in%cntz[k0],]
		map1_k <- map1_[map1_$ADM0_ISO3%in%cntz[k0],]
		map2_k <- map2_[map2_$ADM0_ISO3%in%cntz[k0],]
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map2_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GAUL/Simplified")
		write_sf(map0_k,dsn=paste0("Admin/GAUL/Simplified/GAUL_",cntz[k0],"_",yrz[y0],"_ADM0_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
		write_sf(map1_k,dsn=paste0("Admin/GAUL/Simplified/GAUL_",cntz[k0],"_",yrz[y0],"_ADM1_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
		write_sf(map2_k,dsn=paste0("Admin/GAUL/Simplified/GAUL_",cntz[k0],"_",yrz[y0],"_ADM2_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}

},mc.cores=8)