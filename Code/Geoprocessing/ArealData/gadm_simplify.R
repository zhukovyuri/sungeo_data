# ssh zhukov@sungeo.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/gadm_simplify.R")' &
# R

rm(list=ls())

setwd("~/")
if(grepl("sungeo",Sys.info()[["nodename"]])){setwd("/data/Dropbox/SUNGEO/Data")}
if(grepl("^ubu|^zhu",Sys.info()[["nodename"]])){setwd("~/Dropbox/SUNGEO/Data")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","dplyr","data.table","countrycode","parallel","stringi","V8","rmapshaper","SUNGEO")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# install.packages("devtools")
# library(devtools)
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
# library(SUNGEO)
rmapshaper::check_sys_mapshaper()
sf::sf_use_s2(FALSE)

###############################################
###############################################
###############################################
## GADM
###############################################
###############################################
###############################################


temp <- tempdir()

dir("Admin/GADM/GADM_410_LEVELS") # 2022
dir("Admin/GADM/GADM_36_LEVELS") # 2018
dir("Admin/GADM/GADM_28_LEVELS") # 2015
# dir("Admin/GADM/GADM_27")
# Loop over years
y0 <- 3
vz <- c(28,36,410)
yrz <- c(2015,2018,2022)
vz_list <- parallel::mclapply(1:length(vz),function(y0){
	print(yrz[y0])
	if(vz[y0]%in%410){
		suppressWarnings({
			suppressMessages({
				map0 <- sf::read_sf(paste0("Admin/GADM/GADM_",vz[y0],"_LEVELS/gadm_",vz[y0],"-levels.gpkg"), layer="ADM_0") %>% sf::st_transform(st_crs("EPSG:4326")) %>% 
					dplyr::mutate(ID_0 = GID_0 %>% countrycode::countrycode("iso3c","iso3n"),ISO=GID_0 ,NAME_0=GID_0 %>% countrycode::countrycode("iso3c","country.name"), VERSION=vz[y0]) %>% 
					data.table::setnames(c("ISO","NAME_0","ID_0"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE")) %>% 
					dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,VERSION,geom)
				sf::st_geometry(map0) <- "geometry"
				map1 <- sf::read_sf(paste0("Admin/GADM/GADM_",vz[y0],"_LEVELS/gadm_",vz[y0],"-levels.gpkg"), layer="ADM_1") %>% sf::st_transform(st_crs("EPSG:4326")) %>% 
					dplyr::mutate(ID_0 = GID_0 %>% countrycode::countrycode("iso3c","iso3n"),ISO=GID_0 ,NAME_0=GID_0 %>% countrycode::countrycode("iso3c","country.name"),ID_1 = paste0(GID_0,"_",NAME_1) %>% as.factor() %>% as.numeric(), VERSION=vz[y0]) %>% 
					data.table::setnames(c("ISO","NAME_0","ID_0","NAME_1","ID_1"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE")) %>% 
					dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,VERSION,geom)
				sf::st_geometry(map1) <- "geometry"
				map2 <- sf::read_sf(paste0("Admin/GADM/GADM_",vz[y0],"_LEVELS/gadm_",vz[y0],"-levels.gpkg"), layer="ADM_2") %>% sf::st_transform(st_crs("EPSG:4326")) %>% 
					dplyr::mutate(ID_0 = GID_0 %>% countrycode::countrycode("iso3c","iso3n"),ISO=GID_0 ,NAME_0=GID_0 %>% countrycode::countrycode("iso3c","country.name"),ID_1 = paste0(GID_0,"_",NAME_1) %>% as.factor() %>% as.numeric(),ID_2 = paste0(GID_0,"_",NAME_1,"_",NAME_2) %>% as.factor() %>% as.numeric(), VERSION=vz[y0]) %>% 
					data.table::setnames(c("ISO","NAME_0","ID_0","NAME_1","ID_1","NAME_2","ID_2"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE")) %>% 
					dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,VERSION,geom)
				sf::st_geometry(map2) <- "geometry"
			})
		})
	}
	if(!vz[y0]%in%410){
		# Load GADM
		map0 <- paste0("Admin/GADM/GADM_",vz[y0],"_LEVELS/gadm",vz[y0],"_",ifelse(vz[y0]==28,"adm",""),"0.shp") %>% sf::st_read(quiet=T) %>% 
			sf::st_transform(st_crs("EPSG:4326")) %>% 
			dplyr::mutate(ID_0 = ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_0 %>% 		as.factor() %>% as.numeric(),ID_0),ISO=ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),GID_0 %>% as.character(),ISO %>% as.character()),NAME_0=ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_0 %>% as.character(),NAME_ENGLI %>% as.character()), VERSION=vz[y0]) %>% 
			data.table::setnames(c("ISO","NAME_0","ID_0"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE")) %>% 
			dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,VERSION,geometry)
		map1 <- paste0("Admin/GADM/GADM_",vz[y0],"_LEVELS/gadm",vz[y0],"_",ifelse(vz[y0]==28,"adm",""),"1.shp") %>% sf::st_read(quiet=T) %>% 
			sf::st_transform(st_crs("EPSG:4326")) %>% 
			dplyr::mutate(ID_0 = ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_0 %>% 		as.factor() %>% as.numeric(),ID_0), ISO=ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),GID_0 %>% as.character(),ISO %>% as.character()), NAME_0=ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_0 %>% as.character(),NAME_0 %>% as.character()),ID_1 = ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_1 %>% as.factor() %>% as.numeric(),ID_1), VERSION=vz[y0]) %>% 
			data.table::setnames(c("ISO","NAME_0","ID_0","NAME_1","ID_1"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE")) %>% 
			dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,VERSION,geometry)
		map2 <- paste0("Admin/GADM/GADM_",vz[y0],"_LEVELS/gadm",vz[y0],"_",ifelse(vz[y0]==28,"adm",""),"2.shp") %>% sf::st_read(quiet=T) %>% 
			sf::st_transform(st_crs("EPSG:4326")) %>% 
			dplyr::mutate(ID_0 = ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_0 %>% 		as.factor() %>% as.numeric(),ID_0), ISO=ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),GID_0 %>% as.character(),ISO %>% as.character()), NAME_0=ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_0 %>% as.character(),NAME_0 %>% as.character()),ID_1 = ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_1 %>% as.factor() %>% as.numeric(),ID_1),ID_2 = ifelse((vz[y0]==36) %>% rep(times=dplyr::n()),NAME_2 %>% as.factor() %>% as.numeric(),ID_2), VERSION=vz[y0]) %>% 
			data.table::setnames(c("ISO","NAME_0","ID_0","NAME_1","ID_1","NAME_2","ID_2"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE","ADM2_NAME","ADM2_CODE")) %>% 
			dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,VERSION,geometry)
	}


	# Simplify polygons
	object.size(map0) %>% format(units="Mb")
	object.size(map1) %>% format(units="Mb")
	object.size(map2) %>% format(units="Mb")

	{
		t1_ <- Sys.time()
		map0_ <- rmapshaper::ms_simplify(map0,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
		object.size(map0_) %>% format(units="Mb") %>% paste0(vz[y0]," adm0 ",.) %>% print()
		map1_ <- rmapshaper::ms_simplify(map1,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
		object.size(map1_) %>% format(units="Mb") %>% paste0(vz[y0]," adm1 ",.) %>% print()
		if(!vz[y0]%in%410){
			map2_ <- rmapshaper::ms_simplify(map2,keep=.01,keep_shapes=T,sys=TRUE) %>% fix_geom()
		} else {
			# map2_ <- rmapshaper::ms_simplify(geojsonsf::geojson_sf(paste0(tempd,"/GADM_",yrz[y0],"_ADM2_wgs.geojson")),keep=.01,keep_shapes=T,sys=TRUE) %>% fix_geom()
			map2_ <- map2 %>% data.table::copy()
		}
		object.size(map2_) %>% format(units="Mb") %>% paste0(vz[y0]," adm2 ",.) %>% print()
		t2_ <- Sys.time()
		print(t2_-t1_)
	}
	# plot(map2_["ADM0_ISO3"],lwd=.1); dev.off()

	# By level
	cntz <- map0_$ADM0_ISO3 %>% unique() %>% sort()
	for(k0 in 1:length(cntz)){
		# Subset
		map0_k <- map0_[map0_$ADM0_ISO3%in%cntz[k0],]
		# map1_k <- map1_[map1_$ADM0_ISO3%in%cntz[k0],]
		# map2_k <- map2_[map2_$ADM0_ISO3%in%cntz[k0],]
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map0_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GADM/Simplified")
		sf::write_sf(map0_k,dsn=paste0("Admin/GADM/Simplified/GADM_",cntz[k0],"_",yrz[y0],"_ADM0_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
		# sf::write_sf(map1_k,dsn=paste0("Admin/GADM/Simplified/GADM_",cntz[k0],"_",yrz[y0],"_ADM1_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
		# sf::write_sf(map2_k,dsn=paste0("Admin/GADM/Simplified/GADM_",cntz[k0],"_",yrz[y0],"_ADM2_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}
	cntz <- map1_$ADM0_ISO3 %>% unique() %>% sort()
	for(k0 in 1:length(cntz)){
		# Subset
		map1_k <- map1_[map1_$ADM0_ISO3%in%cntz[k0],]
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map1_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GADM/Simplified")
		sf::write_sf(map1_k,dsn=paste0("Admin/GADM/Simplified/GADM_",cntz[k0],"_",yrz[y0],"_ADM1_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}
	cntz <- map2_$ADM0_ISO3 %>% unique() %>% sort()
	k0 <- 4
	for(k0 in 1:length(cntz)){
		# Subset
		if(!vz[y0]%in%410){
			map2_k <- map2_[map2_$ADM0_ISO3%in%cntz[k0],]
		}
		if(vz[y0]%in%410){
			map2_k <- map2_[map2_$ADM0_ISO3%in%cntz[k0],] %>% data.table::copy() %>% fix_geom() %>% rmapshaper::ms_simplify(keep=.01,keep_shapes=T,sys=TRUE) %>% fix_geom()
			# # Save as temp file
			# 	tempd <- tempdir()
			# 	sf::write_sf(map2_[map2_$ADM0_ISO3%in%cntz[k0],],dsn=paste0(tempd,"/GADM_",cntz[k0],"_",yrz[y0],"_ADM2_wgs.geojson"), delete_dsn=TRUE)
			# 	map2_k <- geojsonsf::geojson_sf(paste0(tempd,"/GADM_",cntz[k0],"_",yrz[y0],"_ADM2_wgs.geojson"))%>% fix_geom() %>% rmapshaper::ms_simplify(keep=.01,keep_shapes=T,sys=TRUE) %>% fix_geom()
		}
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map2_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GADM/Simplified")
		sf::write_sf(map2_k,dsn=paste0("Admin/GADM/Simplified/GADM_",cntz[k0],"_",yrz[y0],"_ADM2_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}

},mc.cores=2)

# Errors:
# CAN
# SWE

# # Preview
dir("Admin/GADM/Simplified/")
# map <- sf::st_read("Admin/GADM/Simplified/GADM_AFG_2022_ADM2_wgs.geojson")
# map
# png("../test.png")
# plot(map["ADM1_NAME"])
# dev.off()