# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/WeatherData/Geoprocessing/noaa_geoprocess.R")' &
# nohup R -e 'source("SUNGEO/Code/WeatherData/Geoprocessing/noaa_geoprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/WeatherData/Geoprocessing/noaa_geoprocess.R")' &
# tail -f nohup.out
# R

## GLC
# ssh zhukov@zhukov.isr.umich.edu
# ssh greatlakes.arc-ts.umich.edu
# singularity exec /home/zhukov/rgeo.sif R 

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("zhubu")){setwd("/media/zhukov/sg1/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhukov","zhuk")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}
if(grepl("arc-ts|^node|^likert",Sys.info()[["nodename"]])){setwd("SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","ncdf4","fields","Rmpi","snow")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}; req.packages <- lapply(list.of.packages, require, character.only = TRUE); rm(list.of.packages,new.packages,req.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# library(SUNGEO)


# Log file
log_file <- "../r_log_errors_noaa.txt"
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)
dir("../")

#############################
## Open nc
#############################

## 
# Temp
##

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

# Print progress for each file?
extra_verbose <- FALSE

# Skip existing files?
skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
# suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Weather/NOAA/Processed/",geoset,"/","NOAAv501_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()]
# }); 
sub_dirz[,mean(exists)]

# Subset
sub_dirz <- sub_dirz[which(!exists),]

# k0 <- 2200; sub_dirz[k0]

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
# mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
mem_all <- as.numeric(system("awk '/MemAvailable/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores())


# PSOCK
ncores <- min(nrow(sub_dirz),ncores)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
# cl <- parallel::makeCluster(ncores,type = "MPI", outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("extra_verbose","skip_existing","sub_dirz","admz","ncores","mem_all","mem0","log_file","long.alt","long","lat","weather.times","rain.times","xdata","xdata2"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(dplyr))
parallel::clusterEvalQ(NULL, expr=library(sf))
parallel::clusterEvalQ(NULL, expr=library(sp))
parallel::clusterEvalQ(NULL, expr=library(stringr))
parallel::clusterEvalQ(NULL, expr=library(stringi))
parallel::clusterEvalQ(NULL, expr=library(raster))
# parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# # Forking
# cntz_list <- mclapply(nrow(sub_dirz):1,function(k0){

# # Single core
# cntz_list <- lapply(1:nrow(sub_dirz),function(k0){

	# Error catching 
	tryCatch({

		# Start timer
		t1 <- Sys.time()

		# Skip if file exists
	    if(!skip_existing|(skip_existing&!sub_dirz[k0,exists])){

			# Load polygons
			suppressWarnings({
				suppressMessages({
				map <- sf::st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
				})
			})

			# Proceed if non-empty geometry
			if(sum(sf::st_is_empty(map))==0&nrow(map)>0){

				# Bounding box (to crop raster)
				bbx <- sf::st_bbox(map)
				xrange <- which(round(long.alt)>=round(bbx["xmin"])&round(long.alt)<=round(bbx["xmax"]))
				yrange <- which(round(lat)>=round(bbx["ymin"])&round(lat)<=round(bbx["ymax"]))

				# Loop over weather times
				weather.mat <- lapply(1:nrow(weather.times),function(t){

					# Create empy matrix
					map00.0 <- map %>% as.data.table() %>% .[,YRMO := as.character(rain.times[t,"YRMO"])]

					# # Print at each year
					# sub_dirz[k0,adm]
					if(extra_verbose){if(grepl("01$",as.character(rain.times[t,"YRMO"]))){print(sub_dirz[k0,paste(geoset,adm,iso3,adm_yr,as.character(rain.times[t,"YRMO"]))])}}

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

				# CLOSE lapply
				}) %>% dplyr::bind_rows()

				# Save
				saveRDS(weather.mat,file=sub_dirz[k0,fnn])

				# Progress
				print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub("NOAAv501_|.RDS","",.)," -- finished"))
		        t2 <- Sys.time()
		        print(t2-t1)

	        # Close if map>0
			}

		# CLOSE skip if exists
		}
	
	# CLOSE try 
	 },error=function(e){
    # write(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, fnn]), file = log_file,append=TRUE);
    print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, fnn]));
    message(e)
    })

# # Close lapply
# })
# gc()

# # Close mclapply
# },mc.cores = min(nrow(sub_dirz),ncores))
# gc()

# Close parLapply
})
parallel::stopCluster(cl)
gc()
# q()
# n
# pkill -9 R
# R