# Connect to boba
# ssh 10.0.0.18
# ssh 2601:189:8000:b3a0:d774:600f:ebca:d6b2

# ssh zhukov@sungeo.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/NightLights/Geoprocessing/viirs_geoprocess_annual.R")'  > sungeo_viirs.log 2>&1 &  
# nohup R -e 'source("/media/zhukov/dropbox2022/Dropbox/SUNGEO/Code/NightLights/Geoprocessing/viirs_geoprocess_annual.R")'  > sungeo_viirs.log 2>&1 &  
# nohup R -e 'source("/home/zhukov/Dropbox/SUNGEO/Code/NightLights/Geoprocessing/viirs_geoprocess_annual.R")'  > sungeo_viirs.log 2>&1 &  
# tail -f sungeo_ghspop.log
# R
# 3026160


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
list.of.packages <- c("data.table","stringr","parallel","dplyr","sf","terra","SUNGEO","countrycode")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)


#############################
## File list
#############################

# Data source
datsrz <- "NightLights/VIIRSannual"
dir("Data/NightLights/VIIRSannual/Raw")

# File list
filez <- data.table::data.table(fn=list.files(paste0("Data/",datsrz,"/Raw"),pattern="^VNL",full=TRUE))%>%.[,year:=fn%>%stringr::str_extract("\\_\\d{4}\\_|\\_\\d{6}-\\d{6}\\_")%>%stringr::str_extract("\\d{4}")%>%as.integer()] %>% .[order(year)] %>% .[,destf:=fn%>%stringr::str_split("\\/")%>%sapply(dplyr::last)%>%gsub("\\.gz$","",.)]
# f0 <- 1

# Matrix of dates
ticker <- SUNGEO::make_ticker(date_min=filez[1,paste0(year,"0101")],date_max=filez[.N,paste0(year,"1231")]) %>% .[!duplicated(YEAR)] %>% .[YEAR%in%filez[,year]]


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
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Data/Admin/",admz[a0],"/Reindexed")) %>% stringr::str_split("_",simplify=TRUE) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character() %>% gsub("GB-EAW","GBR",.), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Data/Admin/",admz[a0],"/Reindexed"),full.names=T) %>% as.character()) %>% dplyr::arrange(iso3,adm_yr) %>% data.table::as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,dplyr::everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Data/",datsrz,"/Processed/",geoset,"/",datsrz%>%stringr::str_split("\\/")%>%sapply("[",2),"_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F) %>% .[file.size(adm_f0)>1000]
# }); 

# Subset
a0 <- 7
print(sub_dirz %>% .[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[,mean(exists)])
sub_dirz <- sub_dirz[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[which(!exists),]
# print(sub_dirz %>% .[!duplicated(umap)] %>% .[,mean(exists)])
# sub_dirz <- sub_dirz %>% .[!duplicated(umap)] %>% .[which(!exists),]
print(nrow(sub_dirz))
if(nrow(sub_dirz)==0){stop("All files have been processed.")}

# tc_type <- "tmax"
k0 <- 4; sub_dirz[k0]

# Ungzip all files, save to temp folder
tempd <- tempdir()
i <- 12
for(i in 1:nrow(filez)){
	print(paste0("Unzipping ",filez[i,year]))
	destf <- paste0(tempd,"/",filez[i,destf])
	gunz <- R.utils::gunzip(filez[i,fn],destname=destf,temporary=TRUE,remove=FALSE,overwrite=TRUE)
}
# r <- terra::rast(destf)
# dir(tempd)
# file.size(paste0(tempd,"/",filez[i,destf]))

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
parallel::clusterExport(NULL,c("extra_verbose","skip_existing","sub_dirz","admz","ncores","mem_all","mem0","ticker","filez","tempd"),envir = environment())
cntz_list <- parallel::parLapply(NULL,1:10,function(k0){

# # Forking
# cntz_list <- mclapply(nrow(sub_dirz):1,function(k0){

# # Single core
# cntz_list <- lapply(1:nrow(sub_dirz),function(k0){

	# Error catching 
	tryCatch({

		# Load packages
		list.of.packages <- c("data.table","stringr","parallel","dplyr","sf","terra","SUNGEO","countrycode")
		new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
		loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
		sf::sf_use_s2(FALSE)

		# Temp directory
		tempdx <- tempdir()

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

				# Loop over times
				t <- 12
				# m0 <- 1
				viirs.mat <- parallel::mclapply(1:nrow(ticker),function(t){

					# Print time period
					if(extra_verbose){print(sub_dirz[k0,paste(geoset,adm,iso3,adm_yr,as.character(ticker[t,YEAR]))])}

					# Create empy matrix
					map00.0 <- map %>% data.table::as.data.table() %>% .[,YEAR := ticker[t,YEAR]] 
					poly_idz <- map00.0 %>% .[,SG_POLYID]

					# Unzip
					filez_t <- filez[year%in%ticker[t,YEAR]] 
					# if(!file.exists(filez_t[,stringr::str_split(fn,"\\/")%>%sapply(dplyr::last)%>%gsub("zip$","tif",.)%>%paste0(tempdx,"/",.)])){
					# 	unzip(filez_t[,fn],exdir=tempdx)
					# }

					# Load using terra package
					f_tif <- terra::rast(filez_t[,paste0(tempd,"/",destf)])
					# Exceptions
					# if(sub_dirz[k0,iso3%in%c("ATA","RUS")]){
					# 	print(paste0("reprojecting raster... (",ticker[t,YRMO],", ",sub_dirz[k0,umap],")"))
					# 	f_tif <- terra::project(f_tif,terra::crs(map))
					# }

					# Crop by country
					f_tif_m0 <- f_tif %>% terra::crop(terra::ext(bbx))
					rm(f_tif)

					# Zonal stats
					map_z <- terra::zonal(x=f_tif_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("vnl_avgmsk_mean")
					map_z_sum <- terra::zonal(x=f_tif_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="sum",na.rm=TRUE) %>% data.table::setnames("vnl_avgmsk_sum")
					map_z <- list(map_z,map_z_sum)%>%dplyr::bind_cols()%>%data.table::as.data.table() %>% .[,SG_POLYID:=poly_idz] %>% .[,YEAR:=ticker[t,YEAR]] %>% dplyr::select("SG_POLYID","YEAR",dplyr::everything())

					# # # Preview
					# plot(f_tif_m0)
					# dev.off()
					# rnkz <- map_z[,rank(vnl_avgmsk_sum)/max(rank(vnl_avgmsk_sum))]
					# plot(map00.0[,geometry],col=rgb(rnkz,rnkz,rnkz))
					# dev.off()

					# Store object
					map_z					

				# CLOSE lapply
				},mc.cores=12) %>% dplyr::bind_rows()

				# Save
				saveRDS(viirs.mat,file=sub_dirz[k0,fnn])

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

# Clear temp files
file.remove(paste0(tempd,"/",grep("^VNL",dir(tempd),value=TRUE)))
