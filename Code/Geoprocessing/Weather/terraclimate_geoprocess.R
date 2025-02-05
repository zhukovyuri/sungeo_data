# ssh zhukov@sungeo.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/WeatherData/Geoprocessing/terraclimate_geoprocess.R")' > sungeo_terra.log 2>&1 &
# nohup R -e 'source("/media/zhukov/dropbox2022/Dropbox/SUNGEO/Code/WeatherData/Geoprocessing/terraclimate_geoprocess.R")' > sungeo_terra.log 2>&1 &
# tail -f nohup.out
# R

rm(list=ls())


## Detect system and set directory
setwd("~/")
if(grepl("boba",Sys.info()[["nodename"]])){setwd("/media/zhukov/dropbox2022/Dropbox/SUNGEO/")}
if(grepl("sungeo",Sys.info()[["nodename"]])){setwd("/data/Dropbox/SUNGEO/")}
if(grepl("^ubu|^zhu",Sys.info()[["nodename"]])){setwd("~/Dropbox/SUNGEO/")}


## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("data.table","stringr","parallel","dplyr","sf","terra","SUNGEO","rvest","countrycode","ncdf4","fields")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)

sf::sf_use_s2(FALSE)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# library(SUNGEO)


# # Log file
# log_file <- "r_log_errors_terraclimate.txt"
# write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)
# dir()

#############################
## File list
#############################

# File list
tc_varz <- data.table::fread("Data/Weather/TerraClimate/terraclimate_variables.csv",header=TRUE)
filez <- data.table::data.table(fn=list.files("Data/Weather/TerraClimate/Raw",pattern="nc$"))%>%.[,abbr:=fn%>%stringr::str_split("_")%>%sapply("[",2)]%>%.[,year:=fn%>%stringr::str_split("_|\\.")%>%sapply("[",3)]
f0 <- 1
tc_varz[,abbr]

# Matrix of dates
ticker <- SUNGEO::make_ticker(date_min=filez[1,paste0(year,"0101")],date_max=filez[.N,paste0(year,"1231")]) %>% .[!duplicated(YRMO)]


######################################
# Loop over ADMZ
######################################

# Print progress for each file?
extra_verbose <- TRUE

# Skip existing files?
skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","HEXGRID","PRIOGRID")

# Full list of files to be processed
# suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Data/Admin/",admz[a0],"/Reindexed")) %>% stringr::str_split("_",simplify=TRUE) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character() %>% gsub("GB-EAW","GBR",.), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Data/Admin/",admz[a0],"/Reindexed"),full.names=T) %>% as.character()) %>% dplyr::arrange(iso3,adm_yr) %>% data.table::as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,dplyr::everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Data/Weather/TerraClimate/Processed/",geoset,"/","TerraClimate_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F) %>% .[file.size(adm_f0)>1000]
# }); 

# Subset
a0 <- 1
# print(sub_dirz %>% .[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[,mean(exists)])
# sub_dirz <- sub_dirz[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[which(!exists),]
print(sub_dirz %>% .[!duplicated(umap)] %>% .[,mean(exists)])
sub_dirz <- sub_dirz %>% .[!duplicated(umap)] %>% .[which(!exists),]
# sub_dirz <- sub_dirz[!iso3%in%c("RUS","ATA")]
if(nrow(sub_dirz)==0){stop("All files have been processed.")}

# tc_type <- "tmax"
k0 <- 34; sub_dirz[k0]

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
# mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
mem_all <- as.numeric(system("awk '/MemAvailable/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores())/4


# PSOCK
ncores <- min(nrow(sub_dirz),ncores)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
# cl <- parallel::makeCluster(ncores,type = "MPI", outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("extra_verbose","skip_existing","sub_dirz","admz","ncores","mem_all","mem0","ticker","tc_varz","filez"),envir = environment())
cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# # Forking
# cntz_list <- mclapply(nrow(sub_dirz):1,function(k0){

# # Single core
# cntz_list <- lapply(1:nrow(sub_dirz),function(k0){

	# Error catching 
	tryCatch({

		# Load packages
		list.of.packages <- c("data.table","stringr","parallel","dplyr","sf","terra","SUNGEO","rvest","countrycode","ncdf4","fields")
		new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
		loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
		sf::sf_use_s2(FALSE)

		# Start timer
		t1 <- Sys.time()

		# Skip if file exists
	    if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn]))){

			# Load polygons
			suppressWarnings({
				suppressMessages({
					# if(!sub_dirz[k0,iso3%in%c("ATA","RUS")]){
						map <- sf::st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
					# } else {
					# 	# Exceptions
					# 	map <- sf::st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% SUNGEO::utm_select() %>% 					sf::st_buffer(0)
					# }
				})
			})

			# Proceed if non-empty geometry
			if(sum(sf::st_is_empty(map))==0&nrow(map)>0){

				t1 <- Sys.time()

				# Bounding box (to crop raster)
				bbx <- sf::st_bbox(map)
				# xrange <- which(round(long.alt)>=round(bbx["xmin"])&round(long.alt)<=round(bbx["xmax"]))
				# yrange <- which(round(lat)>=round(bbx["ymin"])&round(lat)<=round(bbx["ymax"]))

				# Loop over weather times
				t <- 1
				m0 <- 1
				weather.mat <- mclapply(1:nrow(ticker),function(t){
					# print(weather.times[t,"YRMO"])

					# # Print at each year
					# sub_dirz[k0,adm]
					if(extra_verbose){if(grepl("01$",as.character(ticker[t,YRMO]))){print(sub_dirz[k0,paste(geoset,adm,iso3,adm_yr,as.character(ticker[t,YRMO]))])}}

					# Create empy matrix
					map00.0 <- map %>% data.table::as.data.table() %>% .[,YRMO := ticker[t,YRMO]] 
					# if(!paste0(sub_dirz[k0,adm],"_CODE")%in%names(map00.0)){map00.0[,paste0(sub_dirz[k0,adm],"_CODE"):=.I]}
					# poly_idz <- map00.0 %>% .[,paste0(sub_dirz[k0,paste0(iso3,"_",geoset,adm_yr,"_",adm,"_")],get(paste0(sub_dirz[k0,adm],"_CODE")))]
					poly_idz <- map00.0 %>% .[,SG_POLYID]

					# Loop over data types
					# t0 <- 12
					tc_mat <- lapply(tc_varz[,.I],function(t0){

						# Extract type
						tc_type <- tc_varz[t0,abbr]
						
						# Load using terra package
						filez_t <- filez[year%in%ticker[t,YEAR]] %>% .[abbr%in%tc_type]
						terra_nc <- terra::rast(paste0("Data/Weather/TerraClimate/Raw/",filez_t[,fn]))
						terra_dates <- data.table::data.table(name=names(terra_nc)) %>% .[,YRMO := filez_t[,as.integer(year)*100]+.I] 
						# if(sub_dirz[k0,iso3%in%c("ATA","RUS")]){
						# 	print(paste0("reprojecting raster... (",ticker[t,YRMO],", ",sub_dirz[k0,umap],")"))
						# 	terra_nc <- terra::project(terra_nc,terra::crs(map),threads=TRUE)
						# }

						# Crop by country
						terra_m0 <- terra_nc[[terra_dates[YRMO%in%ticker[t,YRMO],name]]] %>% terra::crop(terra::ext(bbx))

						# Zonal stats
						map_z <- terra::zonal(x=terra_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames(tc_type)

						# Save
						map_z
					}) %>% dplyr::bind_cols() %>% data.table::as.data.table() %>% .[,SG_POLYID:=poly_idz] %>% .[,YRMO:=ticker[t,YRMO]] %>% dplyr::select("SG_POLYID","YRMO",dplyr::everything())
					
					# plot(terra_m0)
					# dev.off()

					# Store object
					tc_mat					

				# CLOSE lapply
				},mc.cores=12) %>% dplyr::bind_rows()

				# Save
				saveRDS(weather.mat,file=sub_dirz[k0,fnn])

				# Progress
				print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[6] %>% gsub(".RDS","",.)," -- finished"))
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
    print(e)
    })

# # Close lapply
# })
# gc()

# # Close mclapply
# },mc.cores = min(nrow(sub_dirz),ncores))
# gc()

# Close parLapply
}); parallel::stopCluster(cl)
gc()
# q()
# n
# pkill -9 R
# R
