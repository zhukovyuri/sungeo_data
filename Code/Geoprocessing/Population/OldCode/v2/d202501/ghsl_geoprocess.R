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
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
# library(SUNGEO)

# Log file
log_file <- paste0("../r_log_errors_ghs_",Sys.time() %>% gsub("-| |\\:|EDT","",.),".txt") 
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)

#############################
## Open raster
#############################

# List of rasters
filez <- list.files("Population/GHS",pattern="30ss", full.names=T)
tempd <- tempdir()
pop_ras_list <- lapply(1:length(filez),function(f0){
	unzip(filez[f0],exdir=tempd) %>% .[grep(".tif$",.)]  %>% raster::raster()
	})

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Population/GHS/Processed/",geoset,"/","GHS_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F)
}); 
sub_dirz[,mean(exists)]

# # Move to temporary file location
# if(Sys.info()[["nodename"]]%in%c("zhukov")){
# 	sub_dirz[,fnn := paste0("/mnt/BACKUP_4TB_1/SUNGEO/Data/",fnn)] %>% .[,exists := fnn %>% file.exists()]
# }
# sub_dirz[,mean(exists)]

# Subset
sub_dirz <- sub_dirz[exists==FALSE,] %>% .[!duplicated(umap)]


######################################
# Loop over ADMZ
######################################

# Skip existing files?
skip_existing <- TRUE


k0 <- 300; sub_dirz[k0,fnn]

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

# PSOCK
ncores <- min(nrow(sub_dirz),ncores)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("skip_existing","sub_dirz","admz","tempd","ncores","mem_all","mem0","log_file","pop_ras_list","filez"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(dplyr))
parallel::clusterEvalQ(NULL, expr=library(sf))
# parallel::clusterEvalQ(NULL, expr=library(V8))
parallel::clusterEvalQ(NULL, expr=library(stringr))
parallel::clusterEvalQ(NULL, expr=library(raster))
# parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
cntz_list <- parLapply(NULL,nrow(sub_dirz):1,function(k0){

# # Forking
# cntz_list <- mclapply(nrow(sub_dirz):1,function(k0){

# # Single core
# cntz_list <- lapply(1:nrow(sub_dirz),function(k0){

  # Error catching
  tryCatch({

  	# Skip if exists
  	if(!skip_existing|(skip_existing&sub_dirz[k0,!exists])){

	# Load polygons
	map <- st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

	# # Check empty geometries
	# sf::st_is_empty(map) %>% map[.,]
	# sf::st_is_valid(map)

	# SHGIS exceptions
	if(sub_dirz[k0,.(iso3=="UKR",geoset=="SHGIS",adm_yr==1945) %>% unlist() %>% all()]){
	map <- sf::st_set_precision(map, 1e5)
	}
	if(sub_dirz[k0,.(iso3=="RUS",geoset=="SHGIS",adm_yr==1937) %>% unlist() %>% all()]){
	map <- sf::st_set_precision(map, 1e5)
	}

	# Proceed only if >0 rows
	if(nrow(map)>0){

		# Start timer
		t1 <- Sys.time()

		# Unique geo ID
		idvar <- paste0(sub_dirz[k0,adm],"_CODE")

		# Create ID if missing
		if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(sub_dirz[k0,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

	  	# Loop over rasters
	  	p0 <- 1
	  	pop_mat <- lapply(1:length(pop_ras_list),function(p0){
			
			# Load raster
		  	pop_ras <- pop_ras_list[[p0]]

			# Crop by bounding box
			bbx <- sf::st_bbox(map)
			pop_ras_ <- pop_ras %>% raster::crop(bbx[c("xmin","xmax","ymin","ymax")] %>% raster::extent())
			pop_file <- pop_ras_ %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim()

			# Extract means & sums
			map00.0 <- map %>% as.data.table() %>% .[,.(get(idvar))] %>% data.table::setnames(idvar) %>% .[,YEAR := filez[p0] %>% str_split("_|.zip",simplify=T) %>% .[,3] %>% gsub("E","",.) %>% as.numeric()] %>% .[,GHS_POP_COUNT := raster::extract(pop_ras_,map,fun=sum,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,GHS_POP_KM2 :=GHS_POP_COUNT/(as.numeric(sf::st_area(map))/1e6 )]	

			# # Plot
			# plot(map00.0 %>% sf::st_as_sf() %>% .["GHS_POP_KM2"]); dev.off()

			# # Delete temporary files
			rm(pop_ras,pop_ras_)
			if(pop_file!="memory"){file.remove(c(pop_file,gsub("grd$","gri",c(pop_file))))}
			# dir(paste0(tempd,"/raster"))

			# Store
			map00.0

  		}) %>% dplyr::bind_rows()
	  	pop_mat

	  	# Linear interpolation, etc.
	  	if((map %>% as.data.table() %>% .[,get(idvar)] %>% length())>1){
	  	f1 <- as.formula(paste0("GHS_POP_COUNT~YEAR*as.factor(",idvar,")"))
	  	f2 <- as.formula(paste0("GHS_POP_KM2~YEAR*as.factor(",idvar,")"))
		  } else {
	  	f1 <- as.formula(paste0("GHS_POP_COUNT~YEAR"))
	  	f2 <- as.formula(paste0("GHS_POP_KM2~YEAR"))
		  }
	  	mod <- lm(f1,data=pop_mat)
	  	mod_km <- lm(f2,data=pop_mat)
	  	X <- data.table(idvar=map %>% as.data.table() %>% .[,get(idvar)] %>% rep(each=length(min(pop_mat$YEAR):2020)),YEAR = min(pop_mat$YEAR):2020 %>% rep(nrow(map))) %>% data.table::setnames("idvar",idvar) %>% .[,GHS_POP_COUNT_LI := predict(mod,.)] %>% .[,GHS_POP_KM2_LI := predict(mod_km,.)] %>% merge(pop_mat,by=c(idvar,"YEAR"),all=T) %>% .[,GHS_POP_COUNT_MR := data.table::nafill(GHS_POP_COUNT,"locf"),by= idvar] %>% .[,GHS_POP_KM2_MR := data.table::nafill(GHS_POP_KM2,"locf"),by= idvar] %>% dplyr::select(all_of(idvar),YEAR,GHS_POP_COUNT,GHS_POP_KM2,everything())
	  	
	  	# Merge back
	  	pop_mat <- merge(map,X,by=idvar,all=T) %>% as.data.table() %>% .[,geometry:=NULL]

  		# Save
		saveRDS(pop_mat,file=sub_dirz[k0,fnn])

		# Progress
		print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub(".RDS","",.)," -- finished"))
        t2 <- Sys.time()
        print(t2-t1)


	# Close if map>0
	}

	# Close if exists
	}

  # Close error catch
  },error=function(e){
    write(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, fnn]), file = log_file,append=TRUE);
    print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, fnn]));
    # message(e)
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
