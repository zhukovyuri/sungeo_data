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
list.of.packages <- c("sf","raster","dplyr","data.table","countrycode","parallel","sp")
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


##############################
# PRIO GRID
##############################


# Load grid
load("Admin/PRIOGRID/PRIOGRID.RData")
prio_full <- map0 %>% sf::st_as_sf() %>% sf::st_set_crs(4326)
rm(map0)

# Load cshapes
list.files("Admin/cshapes")
cshapes <- sf::st_read("Admin/cshapes/cshapes.shp")
cshapes$iso3c <- countrycode::countrycode(cshapes$CNTRY_NAME,"country.name","iso3c")
cshapes[is.na(cshapes$iso3c),]$iso3c <- as.character(cshapes[is.na(cshapes$iso3c),]$ISO1AL3)
cshapes[cshapes$CNTRY_NAME%in%"Zanzibar",]$iso3c <- "EAZ"
cshapes[cshapes$CNTRY_NAME%in%"Kosovo",]$iso3c <- "XKX"

yrz <- c(1946:2020)
yrz <- c(1990:2020)
length(yrz)
y0 <- 2

mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)/2
ncores

prio_list <- parallel::mclapply(1:length(yrz),function(y0){
	print(yrz[y0])

	# Subset by year
	map0_year <- cshapes[cshapes$GWSYEAR<=yrz[y0]&(cshapes$GWEYEAR>=min(yrz[y0],2016)),] %>% .[!duplicated(.$iso3c),]
	o <- sf::st_intersects(prio_full,map0_year) %>% as.data.table(.) %>% data.table::setnames(c("prio_rn","cs_rn"))
	prio_year <- prio_full[o[,prio_rn],] 

	# Assign country codes
	prio_year$ADM0_ISO3 <- map0_year  %>% as.data.table() %>% .[o$cs_rn,iso3c] %>% as.character()
	prio_year$ADM0_NAME <- map0_year  %>% as.data.table() %>% .[o$cs_rn,CNTRY_NAME] %>% as.character()
	prio_year$ADM0_CODE <- map0_year  %>% as.data.table() %>% .[o$cs_rn,GWCODE] 
	prio_year$VERSION <- "1.01"
	prio_year <- prio_year %>% data.table::setnames(c("gid","col","row","xcoord","ycoord"),c("PRIO_CODE","PRIO_COL","PRIO_ROW","PRIO_X","PRIO_Y"))  %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,PRIO_CODE,PRIO_COL,PRIO_ROW,PRIO_X,PRIO_Y,VERSION,geometry)
	rm(o,map0_year)

	# Save country files
	cntz <- prio_year$ADM0_ISO3 %>% unique() %>% sort()
	k0 <- 1
	for(k0 in 1:length(cntz)){
		# Subset
		map0_k <- prio_year[prio_year$ADM0_ISO3%in%cntz[k0],]
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map0_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GADM/Simplified")
		write_sf(map0_k,dsn=paste0("Admin/PRIOGRID/Simplified/PRIOGRID_",cntz[k0],"_",yrz[y0],"_PRIO_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}

},mc.cores=ncores)



##############################
# HEX GRID
##############################

rm(list=ls())

# Load grid
load("Admin/HEXGRID/hex_05_full.RData")
ls()
hex_full <- hex_05_full %>% data.table::setnames(c("HEX_ID","HEX_X","HEX_Y"),c("HEX05_CODE","HEX05_X","HEX05_Y"))
rm(hex_05_full)

# Load cshapes
list.files("Admin/cshapes")
cshapes <- sf::st_read("Admin/cshapes/cshapes.shp")
cshapes$iso3c <- countrycode::countrycode(cshapes$CNTRY_NAME,"country.name","iso3c")
cshapes[is.na(cshapes$iso3c),]$iso3c <- as.character(cshapes[is.na(cshapes$iso3c),]$ISO1AL3)
cshapes[cshapes$CNTRY_NAME%in%"Zanzibar",]$iso3c <- "EAZ"
cshapes[cshapes$CNTRY_NAME%in%"Kosovo",]$iso3c <- "XKX"

cshapes$GWCODE

yrz <- c(1946:2020)
yrz <- c(1990:2020)
length(yrz)
y0 <- 2

mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)/2
ncores

hex_list <- parallel::mclapply(1:length(yrz),function(y0){
	print(yrz[y0])

	# Subset by year
	map0_year <- cshapes[cshapes$GWSYEAR<=yrz[y0]&(cshapes$GWEYEAR>=min(yrz[y0],2016)),] %>% .[!duplicated(.$iso3c),]
	o <- sf::st_intersects(hex_full,map0_year) %>% as.data.table(.) %>% data.table::setnames(c("hex_rn","cs_rn"))
	hex_year <- hex_full[o[,hex_rn],] 

	# Assign country codes
	hex_year$ADM0_ISO3 <- map0_year  %>% as.data.table() %>% .[o$cs_rn,iso3c] %>% as.character()
	hex_year$ADM0_NAME <- map0_year  %>% as.data.table() %>% .[o$cs_rn,CNTRY_NAME] %>% as.character()
	hex_year$ADM0_CODE <- map0_year  %>% as.data.table() %>% .[o$cs_rn,GWCODE] 
	hex_year$VERSION <- "0.01"
	hex_year <- hex_year  %>% dplyr::select(ADM0_ISO3,ADM0_NAME,ADM0_CODE,HEX05_CODE,HEX05_X,HEX05_Y,VERSION,geometry)
	rm(o,map0_year)

	# plot(hex_year[hex_year$ADM0_ISO3%in%"ARG","geometry"])
	# plot(map0_year["geometry"],col=NA,border="red",add=T)

	# Save country files
	cntz <- hex_year$ADM0_ISO3 %>% unique() %>% sort()
	k0 <- 1
	for(k0 in 1:length(cntz)){
		# Subset
		map0_k <- hex_year[hex_year$ADM0_ISO3%in%cntz[k0],]
		print(paste0(cntz[k0],"_",yrz[y0]));print(object.size(map0_k) %>% format(units="Mb"))
		# Save
		dir("Admin/GADM/Simplified")
		write_sf(map0_k,dsn=paste0("Admin/HEXGRID/Simplified/HEXGRID_",cntz[k0],"_",yrz[y0],"_HEX05_wgs.geojson"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
	}

},mc.cores=ncores)