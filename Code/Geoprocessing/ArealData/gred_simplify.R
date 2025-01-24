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
## GRED
###############################################
###############################################
###############################################

simp_poly <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,LVA=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01,BOL=.01,HRV=.01,IND=.01)

temp <- tempdir()

# List of GRED polygons

sub_dirz <- dir("Admin/GRED/")[grep(".zip$",dir("Admin/GRED/"))][1]  %>% paste0("Admin/GRED/",.) %>% unzip(exdir = temp) %>% data.frame(f0 = . , f1 = temp, f2 = sapply(strsplit(.,"\\.|/"),"[",4), f3 = sapply(strsplit(.,"\\.|/"),"[",5), f4 = sapply(strsplit(.,"\\.|/"),"[",6), stringsAsFactors = FALSE) %>% distinct(f4,.keep_all = TRUE) %>% drop_na(f4) %>% dplyr::mutate(ctr_n = f4 %>% strsplit("_") %>% sapply("[",3), 	iso3 = f4 %>% strsplit("_") %>% sapply("[",3) %>% countrycode("country.name","iso3c"), yr = f4 %>% strsplit("_") %>% sapply("[",4), f0 = gsub("dbf$","shp",f0)) %>% as.data.table()
sub_dirz

# Loop over files
y0 <- 1
gred_list <- mclapply(1:nrow(sub_dirz),function(y0){
	print(paste0(y0," ",sub_dirz$iso3[y0],"_",sub_dirz$yr[y0]))

	# Load GRED
	map0 <- st_read(sub_dirz$f0[y0],quiet=TRUE) %>% data.table::setnames(names(.),tolower(names(.))) %>% st_transform(st_crs("EPSG:4326")) %>% 
		dplyr::mutate(ISO = sub_dirz$iso3[y0], VERSION= dir("Admin/GRED/")[grep(".zip$",dir("Admin/GRED/"))][1] %>% str_split("_|\\.zip",simplify=T) %>% .[,2]) %>% 
		data.table::setnames(c("ISO","ctr_n","ctr","cst_n","cst"),c("ADM0_ISO3","ADM0_NAME","ADM0_CODE","CST_NAME","CST_CODE")) %>% 
		dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,CST_NAME,CST_CODE,VERSION,geometry)  %>% fix_geom()
    
    # Fix character encoding issue
    map0 <- map0 %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

	# Simplify polygons
	object.size(map0) %>% format(units="Mb") 
	if(sub_dirz$iso3[y0]%in%names(simp_poly)|object.size(map0) %>% format(units="Mb") > 2)
	{
	t1_ <- Sys.time()
	map0 <- rmapshaper::ms_simplify(map0,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
	object.size(map0) %>% format(units="Mb") %>% paste0(sub_dirz$iso3[y0],"_",sub_dirz$yr[y0]," adm0 ",.) %>% print()
	t2_ <- Sys.time()
	print(t2_-t1_)
	}
	# plot(map0["ADM0_ISO3"],lwd=.1); dev.off()

	# Save
	dir("Admin/GRED/Simplified")
	write_sf(map0,dsn=paste0("Admin/GRED/Simplified/GRED_",sub_dirz$iso3[y0],"_",sub_dirz$yr[y0],"_CST_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)

},mc.cores=8)


dir("Admin/GRED/Simplified/")
map <- st_read("Admin/GRED/Simplified/GRED_MDA_2018_ADM2_wgs.geojson")
map
