# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/NightLights/Geoprocessing/dmsp_geoprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/NightLights/Geoprocessing/dmsp_geoprocess.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("zhubu")){setwd("/media/zhukov/sg1/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhukov","zhuk")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("^node|^likert",Sys.info()[["nodename"]])){setwd("SUNGEO/Data/")}
if(grepl("arc-ts",Sys.info()[["nodename"]])){setwd("/scratch/zhukov_root/zhukov1/zhukov/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","dplyr","stringr","data.table","countrycode","parallel","maptools","stringi","R.utils")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
# library(SUNGEO)

# sessionInfo()

# Log file
log_file <- "../r_log_errors_dmsp.txt"
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)

#############################
## Prepare environment
#############################

# List of rasters
filez <- list.files("NightLights/DMSP",pattern="tar$", full.names=T)
tempd <- tempdir()
if(grepl("^node|^likert",Sys.info()[["nodename"]])){tempd <- "../../TEMP"}
unlink(tempd,recursive=TRUE)
dir(tempd)
f0 <- 1
nl_ras_list <- lapply(1:length(filez),function(f0){print(paste0(f0,"/",length(filez)))
	untar(filez[f0],exdir=tempd) 
	sl_ras <- list.files(tempd,full.names=TRUE,pattern="stable_lights.avg_vis.tif.gz") %>% R.utils::gunzip() %>% raster::raster()
	cf_ras <- list.files(tempd,full.names=TRUE,pattern="cf_cvg.tif.gz") %>% R.utils::gunzip() %>% raster::raster()
	av_ras <- list.files(tempd,full.names=TRUE,pattern="avg_vis.tif.gz") %>% R.utils::gunzip() %>% raster::raster()
	list(sl_ras=sl_ras,cf_ras=cf_ras,av_ras=av_ras)
	})
# saveRDS(nl_ras_list,file=paste0(tempd,"/nl_ras_list.RDS"))
# readRDS(paste0(tempd,"/nl_ras_list.RDS"))
nl_info <- sapply(nl_ras_list,function(x){sapply(x,names) %>% stringr::str_split("\\.",simplify=TRUE) %>% .[,1]}) %>% .[1,] %>% data.table(dmsp_file=.,dmsp_version=substr(.,1,3),dmsp_year=substr(.,4,7) %>% as.numeric())

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("NightLights/DMSP/Processed/",geoset,"/","DMSP_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F)
}); sub_dirz[,mean(exists)]

# Subset
sub_dirz <- sub_dirz[exists==FALSE,] %>% .[!duplicated(umap)]
# dir(tempd)

######################################
# Set up parallelization
######################################

# Skip existing files?
skip_existing <- TRUE
# sub_dirz %>% as.data.frame()

# k0 <- 2143; sub_dirz[k0,fnn]
# sub_dirz[,grep("MDV",fnn)]

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1) %>% min(nrow(sub_dirz),.)

# # PSOCK
# cl <- parallel::makePSOCKcluster(ncores, outfile="")
# parallel::setDefaultCluster(cl)
# parallel::clusterExport(NULL,c("skip_existing","sub_dirz","admz","tempd","ncores","mem_all","mem0","log_file","nl_ras_list","nl_info","filez"),envir = environment())
# parallel::clusterEvalQ(NULL, expr=library(data.table))
# parallel::clusterEvalQ(NULL, expr=library(dplyr))
# parallel::clusterEvalQ(NULL, expr=library(sf))
# parallel::clusterEvalQ(NULL, expr=library(R.utils))
# parallel::clusterEvalQ(NULL, expr=library(stringr))
# parallel::clusterEvalQ(NULL, expr=library(raster))
# # parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
# cntz_list <- parallel::parLapply(NULL,1:nrow(sub_dirz),function(k0){

# Forking
cntz_list <- parallel::mclapply(nrow(sub_dirz):1,function(k0){

# # Single core
# cntz_list <- lapply(1:nrow(sub_dirz),function(k0){

  #######################
  # Begin scipt
  #######################

  # Error catching
  tryCatch({

  	# Skip if exists
  	if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn]))){

	# Load polygons
	map <- st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

	# # Check empty geometries
	# sf::st_is_empty(map) %>% map[.,]
	# sf::st_is_valid(map)

	# SHGIS exceptions#######################
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
	  	suppressWarnings({
		  	nl_mat <- lapply(1:length(nl_ras_list),function(p0){

		  		# print(paste0(p0,"/",length(nl_ras_list)," ",nl_info[p0,"dmsp_year"]))
				
				# Load raster
			  	sl_ras <- nl_ras_list[[p0]][["sl_ras"]]
			  	cf_ras <- nl_ras_list[[p0]][["cf_ras"]]
			  	av_ras <- nl_ras_list[[p0]][["av_ras"]]

				# Crop by bounding box
				bbx <- sf::st_bbox(map)
				sl_ras_ <- sl_ras %>% raster::crop(bbx[c("xmin","xmax","ymin","ymax")] %>% raster::extent())
				cf_ras_ <- cf_ras %>% raster::crop(bbx[c("xmin","xmax","ymin","ymax")] %>% raster::extent())
				av_ras_ <- av_ras %>% raster::crop(bbx[c("xmin","xmax","ymin","ymax")] %>% raster::extent())
				# raster::plot(av_ras_)
				sl_ras_file <- sl_ras_ %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim()
				cf_ras_file <- cf_ras_ %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim()
				av_ras_file <- av_ras_ %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim() 
				
				# Extract means & sums
				map00.0 <- map %>% as.data.table() %>% .[,c("DMSP_VERSION","YEAR") := nl_info[p0,.(dmsp_version,dmsp_year)]] %>% .[,DMSP_SL_SUM := raster::extract(sl_ras_,map,fun=sum,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_SL_MEAN := raster::extract(sl_ras_,map,fun=mean,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_SL_SD := raster::extract(sl_ras_,map,fun=sd,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_CF_SUM := raster::extract(cf_ras_,map,fun=sum,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_CF_MEAN := raster::extract(cf_ras_,map,fun=mean,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_CF_SD := raster::extract(cf_ras_,map,fun=sd,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_AV_SUM := raster::extract(av_ras_,map,fun=sum,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_AV_MEAN := raster::extract(av_ras_,map,fun=mean,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,DMSP_AV_SD := raster::extract(av_ras_,map,fun=sd,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,geometry := NULL]

				# # Plot
				# plot(map00.0 %>% sf::st_as_sf() %>% .["DMSP_CF_KM2"]); dev.off()

				# Delete temporary files
				file.remove(c(sl_ras_file,cf_ras_file,av_ras_file,gsub("grd$","gri",c(sl_ras_file,cf_ras_file,av_ras_file))))

				# Store
				map00.0

	  		}) %>% dplyr::bind_rows() %>% .[,POLYGON_ID := paste0(sub_dirz[k0,paste0(iso3,"_",geoset,adm_yr,"_",adm)],"_",get(idvar))] %>% .[,.SD,.SDcols=c("POLYGON_ID",idvar,"YEAR",grep("^DMSP",names(.),value=TRUE))]
	  	})
	  	nl_mat

  		# Save
		saveRDS(nl_mat,file=sub_dirz[k0,fnn])

		# Progress
		print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub(".RDS","",.)," -- finished  :)"))
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

# Close mclapply
},mc.cores = min(nrow(sub_dirz),ncores))
gc()

# # Close parLapply
# })
# parallel::stopCluster(cl)
# gc()
# q()
# n
# pkill -9 R
# R
unlink(tempd,recursive=TRUE)