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
list.of.packages <- c("sf","raster","dplyr","data.table","countrycode","parallel","maptools","stringi","V8","rmapshaper","SUNGEO")
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
## geoBoundaries
###############################################
###############################################
###############################################


# Loop over filez
y0 <- 362
temp <- tempdir()
filez <- dir("Admin/geoBoundaries/Zips") %>% .[grep("ADM0|ADM1|ADM2",.)]
gb_list <- mclapply(1:length(filez),function(y0){

	filez[y0] %>% str_split("-",simplify=T) %>% .[,3:4] %>% paste0(collapse="_") %>% paste0(y0,"/",length(filez)," ",.,collapse=" ") %>% print()

	# Load geoB
	if(filez[y0] %>% str_split("-",simplify=T) %>% .[,4] == "ADM0"){
	map <- paste0("Admin/geoBoundaries/Zips/",filez[y0]) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n"), NAME_0 = countrycode(shapeGroup,"iso3c","country.name"),VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% 
		data.table::setnames(c("shapeGroup","NAME_0","ID_0"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE")) %>%  dplyr::mutate_if(is.factor,as.character) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,VERSION,geometry)
	}
	if(filez[y0] %>% str_split("-",simplify=T) %>% .[,4] == "ADM1"){
	map <- paste0("Admin/geoBoundaries/Zips/",filez[y0]) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n") , NAME_0 = countrycode(shapeGroup,"iso3c","country.name"), ID_1 = shapeID %>% str_split("-",simplify=T) %>% .[,4] %>% str_extract("\\d+"), VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% 
		data.table::setnames(c("shapeGroup","NAME_0","ID_0","shapeName","ID_1"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),skip_absent=T) %>%  dplyr::mutate_if(is.factor,as.character) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,VERSION,geometry)
	}
	if(filez[y0] %>% str_split("-",simplify=T) %>% .[,4] == "ADM2"){
	map <- paste0("Admin/geoBoundaries/Zips/",filez[y0]) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n"), NAME_0 = countrycode(shapeGroup,"iso3c","country.name"), ID_2 = shapeID %>% str_split("-",simplify=T) %>% .[,4] %>% str_extract("\\d+"), NAME_1 = NA_character_, ID_1 =NA_real_, VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% 
		data.table::setnames(c("shapeGroup","NAME_0","ID_0","NAME_1","ID_1","shapeName","ID_2"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=T) %>% dplyr::mutate_if(is.factor,as.character) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,VERSION,geometry)
	map1 <- paste0("Admin/geoBoundaries/Zips/",filez[y0] %>% gsub("ADM2","ADM1",.)) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326"))  %>% dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n"), NAME_0 = countrycode(shapeGroup,"iso3c","country.name"), ID_1 = shapeID %>% str_split("-",simplify=T) %>% .[,4] %>% str_extract("\\d+"), VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% data.table::setnames(c("shapeGroup","NAME_0","ID_0","shapeName","ID_1"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),skip_absent=T) %>%  dplyr::mutate_if(is.factor,as.character) %>%  dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,VERSION,geometry)
	o <- map %>% st_centroid() %>% st_within(map1) %>% as.data.table()
	map$ADM1_NAME[o$row.id] <- map1$ADM1_NAME[o$col.id]
	map$ADM1_CODE[o$row.id] <- map1$ADM1_CODE[o$col.id]
	rm(map1)
	}
	map

	# Simplify polygons
	object.size(map) %>% format(units="Mb")
	if(grepl("NPL-ADM2",filez[y0])){
		t1_ <- Sys.time()
		map_ <- rmapshaper::ms_simplify(map,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
		map_[sf::st_is_empty(map_),] <- sf::st_is_empty(map_) %>% map[.,] 
		map <- map_; rm(map_)
		object.size(map) %>% format(units="Mb") %>% paste0(filez[y0] %>% str_split("-",simplify=T) %>% .[,3:4] %>% paste0(collapse="_") ," ",.) %>% print()
		# plot(map["geometry"])
		t2_ <- Sys.time()
		print(t2_-t1_)

	} else 	{
		t1_ <- Sys.time()
		map <- rmapshaper::ms_simplify(map,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
		object.size(map) %>% format(units="Mb") %>% paste0(filez[y0] %>% str_split("-",simplify=T) %>% .[,3:4] %>% paste0(collapse="_") ," ",.) %>% print()
		# sf::st_is_valid(map)
		# plot(map["geometry"])
		t2_ <- Sys.time()
		print(t2_-t1_)
	}
	# plot(map2_["ADM0_ISO3"],lwd=.1); dev.off()

	# Save
	dir("Admin/geoBoundaries/Simplified")
	write_sf(map,dsn=paste0("Admin/geoBoundaries/Simplified/geoBoundaries_",filez[y0] %>% str_split("-",simplify=T) %>% .[,3],"_",2020,"_",filez[y0] %>% str_split("-",simplify=T) %>% .[,4],"_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)

	

},mc.cores=8)
# 

# dir("Admin/geoBoundaries/Simplified/")
# map <- st_read("Admin/geoBoundaries/Simplified/geoBoundaries_MDA_2018_ADM2_wgs.geojson")
# map
# filez[y0] %>% str_split("-",simplify=T) %>% .[,4] 



# Fix broken geometries
cntz_fix <- c("AUT-ADM2")

# Loop over filez
y0 <- 1
temp <- tempdir()
filez <- dir("Admin/geoBoundaries/Zips") %>% .[grep("ADM0|ADM1|ADM2",.)] %>% .[grep(paste0("-",cntz_fix,"-",collapse="|"),.)]
gb_list <- mclapply(1:length(filez),function(y0){

	filez[y0] %>% str_split("-",simplify=T) %>% .[,3:4] %>% paste0(collapse="_") %>% paste0(y0,"/",length(filez)," ",.,collapse=" ") %>% print()

	# Load geoB
	if(filez[y0] %>% str_split("-",simplify=T) %>% .[,4] == "ADM0"){
	map <- paste0("Admin/geoBoundaries/Zips/",filez[y0]) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n"), NAME_0 = countrycode(shapeGroup,"iso3c","country.name"),VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% 
		data.table::setnames(c("shapeGroup","NAME_0","ID_0"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE")) %>%  dplyr::mutate_if(is.factor,as.character) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,VERSION,geometry)
	}
	if(filez[y0] %>% str_split("-",simplify=T) %>% .[,4] == "ADM1"){
	map <- paste0("Admin/geoBoundaries/Zips/",filez[y0]) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n") , NAME_0 = countrycode(shapeGroup,"iso3c","country.name"), ID_1 = shapeID %>% str_split("-",simplify=T) %>% .[,4] %>% str_extract("\\d+"), VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% 
		data.table::setnames(c("shapeGroup","NAME_0","ID_0","shapeName","ID_1"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),skip_absent=T) %>%  dplyr::mutate_if(is.factor,as.character) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,VERSION,geometry)
	}
	if(filez[y0] %>% str_split("-",simplify=T) %>% .[,4] == "ADM2"){
	map <- paste0("Admin/geoBoundaries/Zips/",filez[y0]) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n"), NAME_0 = countrycode(shapeGroup,"iso3c","country.name"), ID_2 = shapeID %>% str_split("-",simplify=T) %>% .[,4] %>% str_extract("\\d+"), NAME_1 = NA_character_, ID_1 =NA_real_, VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% 
		data.table::setnames(c("shapeGroup","NAME_0","ID_0","NAME_1","ID_1","shapeName","ID_2"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE"),skip_absent=T) %>% dplyr::mutate_if(is.factor,as.character) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,VERSION,geometry)
	map1 <- paste0("Admin/geoBoundaries/Zips/",filez[y0] %>% gsub("ADM2","ADM1",.)) %>% unzip(exdir=temp) %>% subset(grepl(".shp$",.)) %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326"))  %>% dplyr::mutate(ID_0 = shapeGroup %>% countrycode("iso3c","iso3n"), NAME_0 = countrycode(shapeGroup,"iso3c","country.name"), ID_1 = shapeID %>% str_split("-",simplify=T) %>% .[,4] %>% str_extract("\\d+"), VERSION= shapeID %>% str_split("-",simplify=T) %>% .[,3]) %>% data.table::setnames(c("shapeGroup","NAME_0","ID_0","shapeName","ID_1"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),skip_absent=T) %>%  dplyr::mutate_if(is.factor,as.character) %>%  dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,VERSION,geometry)
	o <- map %>% st_centroid() %>% st_within(map1) %>% as.data.table()
	map$ADM1_NAME[o$row.id] <- map1$ADM1_NAME[o$col.id]
	map$ADM1_CODE[o$row.id] <- map1$ADM1_CODE[o$col.id]
	rm(map1)
	}
	map %>% fix_geom()

	# Simplify polygons
	object.size(map) %>% format(units="Mb")
	{
	t1_ <- Sys.time()
	map <- rmapshaper::ms_simplify(map %>% fix_geom(),keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	object.size(map) %>% format(units="Mb") %>% paste0(filez[y0] %>% str_split("-",simplify=T) %>% .[,3:4] %>% paste0(collapse="_") ," ",.) %>% print()
	t2_ <- Sys.time()
	print(t2_-t1_)
	}
	# plot(map["ADM0_ISO3"],lwd=.1); dev.off()

	# Drop empties
	map <- map[!sf::st_is_empty(map),] 
	map

	# Save
	dir("Admin/geoBoundaries/Simplified")
	write_sf(map,dsn=paste0("Admin/geoBoundaries/Simplified/geoBoundaries_",filez[y0] %>% str_split("-",simplify=T) %>% .[,3],"_",2020,"_",filez[y0] %>% str_split("-",simplify=T) %>% .[,4],"_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)

},mc.cores=8)
# 