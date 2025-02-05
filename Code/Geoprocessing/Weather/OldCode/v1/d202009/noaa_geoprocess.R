# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/WeatherData/Geoprocessing/noaa_geoprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/WeatherData/Geoprocessing/noaa_geoprocess.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","ncdf4","fields")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
library(SUNGEO)



#############################
## Open nc
#############################

## 
# Temp
##

dir("Weather/NOAA/")

# Load weather data
mycdf <- ncdf4::nc_open("Weather/NOAA/Raw/air.mon.mean.v501.nc", verbose = F, write = FALSE)
mycdf # Inspect
timedata <- ncdf4::ncvar_get(mycdf,'time')
lat <- ncdf4::ncvar_get(mycdf,'lat')
long <- ncdf4::ncvar_get(mycdf,'lon')
xdata <- ncdf4::ncvar_get(mycdf,'air')
dim(xdata)

# Alternate longitude
long.alt <- long
long.alt[long>180] <- long.alt[long>180]-360

# Re-order columns
xdata <- xdata[order(long.alt),,]
long.alt <- long.alt[order(long.alt)]

# Plot
# fields::image.plot(long.alt,rev(lat),xdata[,rev(1:dim(xdata)[2]),1000])


## 
# Precipitation
##

# Load weather data
mycdf2 <- ncdf4::nc_open("Weather/NOAA/Raw/precip.mon.total.v501.nc", verbose = F, write = FALSE)
mycdf2 # Inspect
timedata2 <- ncdf4::ncvar_get(mycdf2,'time')
lat2 <- ncdf4::ncvar_get(mycdf2,'lat')
long2 <- ncdf4::ncvar_get(mycdf2,'lon')
xdata2 <- ncdf4::ncvar_get(mycdf2,'precip')
dim(xdata2)

# Alternate longitude
long.alt2 <- long2
long.alt2[long2>180] <- long.alt2[long2>180]-360

# Re-order columns
xdata2 <- xdata2[order(long.alt2),,]
long.alt2 <- long.alt2[order(long.alt2)]

# Plot
# fields::image.plot(long.alt2,rev(lat2),xdata2[,rev(1:dim(xdata2)[2]),100])


#############################
# Time matrix
#############################

##
# Temp
##

# Hours
seq.hourz <- seq(as.Date("1900-1-1 0:0:0"),as.Date("2017-1-1 0:0:0"),by=1/24)
seq.hourz <- data.frame(DATE=gsub("-","",seq.hourz),HOUR=1:length(seq.hourz),stringsAsFactors=FALSE)
seq.hourz[1:48,]

# Dates
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors = FALSE) %>% as.data.table()
# Dates (extend to 1500)
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors=F)
datez2 <- seq(as.Date("1500-01-01"), as.Date("1899-12-31"), by="days")
ticker2 <- data.frame(DATE=gsub("-","",datez2),TID=1-(length(datez2):1),WID=(1-rev(rep(1:length(datez2),each=7)[1:length(datez2)])),YRMO=substr(gsub("-","",datez2),1,6),YEAR=substr(gsub("-","",datez2),1,4),stringsAsFactors=F)
ticker <- rbind(ticker2,ticker)
ticker[ticker$YRMO%in%c(189912,190001),]

# Merge dates with hours
ticker <- merge(ticker,seq.hourz,by="DATE",all.x=F,all.y=T)
ticker <- ticker[ticker$HOUR<range(timedata)[2],]
tail(ticker)
weather.times <- ticker[ticker$HOUR%in%timedata,]
tail(weather.times)
rm(ticker,ticker2,datez,seq.hourz)

##
# Rain
##

# Hours
seq.hourz <- seq(as.Date("1900-1-1 0:0:0"),as.Date("2017-1-1 0:0:0"),by=1/24)
seq.hourz <- data.frame(DATE=gsub("-","",seq.hourz),HOUR=1:length(seq.hourz),stringsAsFactors=F)
seq.hourz[1:48,]

# Dates
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors = FALSE) %>% as.data.table()
# Dates (extend to 1500)
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors=F)
datez2 <- seq(as.Date("1500-01-01"), as.Date("1899-12-31"), by="days")
ticker2 <- data.frame(DATE=gsub("-","",datez2),TID=1-(length(datez2):1),WID=(1-rev(rep(1:length(datez2),each=7)[1:length(datez2)])),YRMO=substr(gsub("-","",datez2),1,6),YEAR=substr(gsub("-","",datez2),1,4),stringsAsFactors=F)
ticker <- rbind(ticker2,ticker)
ticker[ticker$YRMO%in%c(189912,190001),]

# Merge dates with hours
ticker <- merge(ticker,seq.hourz,by="DATE",all.x=F,all.y=T)
ticker <- ticker[ticker$HOUR<range(timedata2)[2],]
rain.times <- ticker[ticker$HOUR%in%timedata2,]
tail(rain.times)
rm(ticker,ticker2,datez,seq.hourz)


######################################
# Loop over ADMZ
######################################

# Skip existing files?
skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")

a0 <- 1; admz[a0]

# OPEN loop 001
for(a0 in 1:length(admz)){

	# List of boundary files (ordered by size)
	admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table() %>% .[order(file.size(f0)),]

	# a0_ <- 1000; admz_dir[a0_,]

	# Object sizes
	sort( sapply(ls(),function(x){object.size(get(x)) %>% format(units="Mb")}))

	# Number of cores
	mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
	mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
	ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

	# Proceed if at least one adm file exists
	# OPEN if 002
	if(nrow(admz_dir)>0){
		# Loop over files
		# OPEN mclapply 003
		mclapply(1:nrow(admz_dir),function(a0_){
			# Error catching 
			# OPEN try 004
			tryCatch({

				# File name
				fnn <- paste0("NOAAv501_",admz_dir[a0_,iso3],"_",admz[a0],admz_dir[a0_,.(yr,adm) %>% paste0(collapse="_")],".RDS")

				# Skip if file exists
				# OPEN if 005
			    if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Weather/NOAA/Processed/",admz[a0])))){

					# Load polygons
					map <- sf::st_read(admz_dir[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)

					# Bounding box (to crop raster)
					bbx <- sf::st_bbox(map)
					xrange <- which(round(long.alt)>=round(bbx["xmin"])&round(long.alt)<=round(bbx["xmax"]))
					yrange <- which(round(lat)>=round(bbx["ymin"])&round(lat)<=round(bbx["ymax"]))

					t <- 1

					# Loop over weather times
					# OPEN lapply 004
					weather.mat <- lapply(1:nrow(weather.times),function(t){

						# Create empy matrix
						map00.0 <- map %>% as.data.table() %>% .[,YRMO := as.character(rain.times[t,"YRMO"])]

						# Print at each year
						if(grepl("01$",as.character(rain.times[t,"YRMO"]))){print(paste(admz[a0],admz_dir[a0_,.(iso3,yr,adm)] %>% paste0(collapse=" "),as.character(rain.times[t,"YRMO"])))}

						# Convert to data.frame
						sub.xdata <- xdata[xrange,yrange,t]
						sub.xdata2 <- xdata2[xrange,yrange,t]
						lonlat <- expand.grid(long.alt[xrange], lat[yrange])
						tmp.vec <- as.vector(sub.xdata)
						tmp.vec2 <- as.vector(sub.xdata2)
						xdata.dt <- data.table(cbind(lonlat, tmp.vec)) %>% data.table::setnames(c("LONG", "LAT", paste0("TEMP_",as.character(weather.times[t,"YRMO"]))))
						xdata.dt2 <- data.table(cbind(lonlat, tmp.vec2)) %>% data.table::setnames(c("LONG", "LAT", paste0("RAIN_",as.character(weather.times[t,"YRMO"]))))

						# Convert to raster
						sp::coordinates(xdata.dt) <- ~ LONG + LAT
						sp::gridded(xdata.dt) <- TRUE
						xdata.r <- raster::raster(xdata.dt)
						sp::coordinates(xdata.dt2) <- ~ LONG + LAT
						sp::gridded(xdata.dt2) <- TRUE
						xdata.r2 <- raster::raster(xdata.dt2)

						# Extract means
						map00.0[,NOAA_TEMP := raster::extract(xdata.r,map,fun=mean,factors=T,buffer=1000, small=T,na.rm=T)]
						map00.0[,NOAA_RAIN := raster::extract(xdata.r2,map,fun=mean,factors=T,buffer=1000, small=T,na.rm=T)]

						# Remove geometry
						map00.0[,geometry := NULL]

						# Store object
						map00.0

					# CLOSE lapply 004 [t]
					}) %>% dplyr::bind_rows()

					# Save
					saveRDS(weather.mat,file=paste0("Weather/NOAA/Processed/",admz[a0],"/",fnn))

					print(paste0(fnn %>% gsub("NOAAv501_|.RDS","",.)," -- finished"))

				# CLOSE if 005 [skip if exists]
				}
			# CLOSE try 004
			},error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("NOAAv501_|.RDS","",.)));message(e)})
		# CLOSE loop 003 [a0_]
		},mc.cores=ncores)
	# CLOSE if 002
	}
# CLOSE loop 001 [a0]
}