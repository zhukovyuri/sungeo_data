# Connect to boba
# ssh 10.0.0.18
# ssh 2601:189:8000:b3a0:d774:600f:ebca:d6b2

# ssh zhukov@sungeo.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/Elevation/Geoprocessing/etopo1_geoprocess.R")' > sungeo_etopo1.log 2>&1 &
# nohup R -e 'source("/media/zhukov/dropbox2022/Dropbox/SUNGEO/Code/Elevation/Geoprocessing/etopo1_geoprocess.R")' > sungeo_etopo1.log 2>&1 &
# tail -f nohup.out
# R
# 1351631
# 

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

#############################
## File list
#############################

# File list
dir("Data/Elevation/ETOPO1/Raw")

# tc_varz <- data.table::fread("Data/Weather/TerraClimate/terraclimate_variables.csv",header=TRUE)
filez <- data.table::data.table(fn=list.files("Data/Elevation/ETOPO1/Raw",pattern="tif$",full=TRUE))%>%.[,fn0:=fn%>%stringr::str_split("\\/")%>%sapply("[",5)]
f0 <- 1

# # Matrix of dates
# ticker <- SUNGEO::make_ticker(date_min=19000101,date_max=20171231) %>% .[!duplicated(YRMO)]
dir("Data/Elevation/ETOPO1/Processed")

######################################
# Loop over ADMZ
######################################

# Print progress for each file?
extra_verbose <- TRUE

# Skip existing files?
skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","HEXGRID","PRIOGRID")
# a0 <- 6

# Full list of files to be processed
# suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Data/Admin/",admz[a0],"/Reindexed")) %>% stringr::str_split("_",simplify=TRUE) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character() %>% gsub("GB-EAW","GBR",.), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Data/Admin/",admz[a0],"/Reindexed"),full.names=T) %>% as.character()) %>% dplyr::arrange(iso3,adm_yr) %>% data.table::as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,dplyr::everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Data/Elevation/ETOPO1/Processed/",geoset,"/","ETOPO1_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F) %>% .[file.size(adm_f0)>1000]

# }); 

sub_dirz[geoset%in%"geoBoundaries"]

# Subset
# Subset
a0 <- 5
print(sub_dirz %>% .[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[,mean(exists)])
sub_dirz <- sub_dirz[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[which(!exists),]
# print(sub_dirz %>% .[!duplicated(umap)] %>% .[,mean(exists)])
# sub_dirz <- sub_dirz %>% .[!duplicated(umap)] %>% .[which(!exists),]
print(nrow(sub_dirz))
if(nrow(sub_dirz)==0){stop("All files have been processed.")}
sub_dirz

# tc_type <- "tmax"
k0 <- 1; sub_dirz[k0]

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
parallel::clusterExport(NULL,c("extra_verbose","skip_existing","sub_dirz","admz","ncores","mem_all","mem0","filez"),envir = environment())
cntz_list <- parallel::parLapply(NULL,1:nrow(sub_dirz),function(k0){

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

		# print("checkpoint A")
		# Load weather nc
		suppressMessages({
			etopo1 <- terra::rast(filez[1,fn])
		})
		
		# Skip if file exists
    if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn]))){

		# Load polygons
		suppressWarnings({
			suppressMessages({
					map <- sf::st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
			})
		})

		# Proceed if non-empty geometry
		if(sum(sf::st_is_empty(map))==0&nrow(map)>0){

			# Start timer
			t1 <- Sys.time()

			# Bounding box (to crop raster)
			bbx <- sf::st_bbox(map)

			# Create empy matrix
			map00.0 <- map %>% data.table::as.data.table() 
			poly_idz <- map00.0 %>% .[,SG_POLYID]

			# Crop by country
			etopo1_m0 <- etopo1 %>% terra::crop(terra::ext(bbx))
			etopo1_file <- etopo1_m0 %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim()

			# Calculate slope
			slope_m0 <- etopo1_m0 %>% terra::terrain(v=c("slope","TRI"))
			slope_file <- slope_m0 %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim()

			# Zonal stats
			map_z_mean <- terra::zonal(x=etopo1_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("elev_mean")
			map_z_sd <- terra::zonal(x=etopo1_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="sd",na.rm=TRUE) %>% data.table::setnames("elev_sd")
			map_z_min <- terra::zonal(x=etopo1_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="min",na.rm=TRUE) %>% data.table::setnames("elev_min")
			map_z_max <- terra::zonal(x=etopo1_m0,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="max",na.rm=TRUE) %>% data.table::setnames("elev_max")
			map_slope_mean <- terra::zonal(x=slope_m0[["slope"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("slope_mean")
			map_slope_sd <- terra::zonal(x=slope_m0[["slope"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="sd",na.rm=TRUE) %>% data.table::setnames("slope_sd")
			map_slope_min <- terra::zonal(x=slope_m0[["slope"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="min",na.rm=TRUE) %>% data.table::setnames("slope_min")
			map_slope_max <- terra::zonal(x=slope_m0[["slope"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="max",na.rm=TRUE) %>% data.table::setnames("slope_max")
			map_tri_mean <- terra::zonal(x=slope_m0[["TRI"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("tri_mean")
			map_tri_sd <- terra::zonal(x=slope_m0[["TRI"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="sd",na.rm=TRUE) %>% data.table::setnames("tri_sd")
			map_tri_min <- terra::zonal(x=slope_m0[["TRI"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="min",na.rm=TRUE) %>% data.table::setnames("tri_min")
			map_tri_max <- terra::zonal(x=slope_m0[["TRI"]],z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="max",na.rm=TRUE) %>% data.table::setnames("tri_max")

			# Delete temporary files
			rm(etopo1_m0,slope_m0)
			if(etopo1_file!="memory"){file.remove(c(etopo1_file,gsub("grd$","gri",c(etopo1_file))))}
			if(slope_file!="memory"){file.remove(c(slope_file,gsub("grd$","gri",c(slope_file))))}

			# Save
			elev.mat <- list(map_z_mean,map_z_sd,map_z_min,map_z_max,map_slope_mean,map_slope_sd,map_slope_min,map_slope_max,map_tri_mean,map_tri_sd,map_tri_min,map_tri_max)%>%dplyr::bind_cols() %>% data.table::as.data.table() %>% .[,SG_POLYID:=poly_idz] %>% dplyr::select("SG_POLYID",dplyr::everything())

			# # # Preview
			# plot(slope_m0[["TRI"]])
			# dev.off()
			# rnkz <- elev.mat[,rank(slope_mean)/max(rank(slope_mean))]
			# plot(map00.0[,geometry],col=rgb(rnkz,rnkz,rnkz))
			# dev.off()
				
			# Save
			saveRDS(elev.mat,file=sub_dirz[k0,fnn])
			
			# Progress
			print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[6] %>% gsub(".RDS","",.)," -- finished"))
      t2 <- Sys.time()
      print(t2-t1)

      # Close if map>0
		}

	# CLOSE skip if exists
	}
	
	# Clear memory
	rm(etopo1); gc()

	# CLOSE try 
	},error=function(e){
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

