# Connect to boba
# ssh 10.0.0.18
# ssh 2601:189:8000:b3a0:d774:600f:ebca:d6b2

# ssh zhukov@sungeo.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/LandCover/Geoprocessing/glcc_geoprocess.R")' > sungeo_glcc.log 2>&1 &
# nohup R -e 'source("/home/zhukov/Dropbox/SUNGEO/Code/LandCover/Geoprocessing/glcc_geoprocess.R")' > sungeo_glcc.log 2>&1 &
# nohup R -e 'source("/media/zhukov/dropbox2022/Dropbox/SUNGEO/Code/LandCover/Geoprocessing/glcc_geoprocess.R")' > sungeo_glcc.log 2>&1 &
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
dir("Data/LandCover/GLCC/Raw")

# tc_varz <- data.table::fread("Data/Weather/TerraClimate/terraclimate_variables.csv",header=TRUE)
filez <- data.table::data.table(fn=list.files("Data/LandCover/GLCC/Raw",pattern="gblulcgeo20\\.tif$",full=TRUE))%>%.[,fn0:=fn%>%stringr::str_split("\\/")%>%sapply("[",5)]%>%.[,fn0:=fn%>%stringr::str_split("\\/")%>%sapply("[",5)]
f0 <- 1

# # Matrix of dates
# ticker <- SUNGEO::make_ticker(date_min=19000101,date_max=20171231) %>% .[!duplicated(YRMO)]
dir("Data/LandCover/GLCC/Processed")

######################################
# Loop over ADMZ
######################################

# 

# Print progress for each file?
extra_verbose <- TRUE

# Skip existing files?
skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","HEXGRID","PRIOGRID")
# a0 <- 6

# Full list of files to be processed
# suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Data/Admin/",admz[a0],"/Reindexed")) %>% stringr::str_split("_",simplify=TRUE) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character() %>% gsub("GB-EAW","GBR",.), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Data/Admin/",admz[a0],"/Reindexed"),full.names=T) %>% as.character()) %>% dplyr::arrange(iso3,adm_yr) %>% data.table::as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,dplyr::everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Data/LandCover/GLCC/Processed/",geoset,"/","GLCC_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F) %>% .[file.size(adm_f0)>1000]

# }); 

sub_dirz[geoset%in%"geoBoundaries"]

# Subset
# Subset
a0 <- 2
print(sub_dirz %>% .[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[,mean(exists)])
sub_dirz <- sub_dirz[geoset%in%admz[a0]] %>% .[!duplicated(umap)] %>% .[which(!exists),]
# print(sub_dirz %>% .[!duplicated(umap)] %>% .[,mean(exists)])
# sub_dirz <- sub_dirz %>% .[!duplicated(umap)] %>% .[which(!exists),]
print(nrow(sub_dirz))
if(nrow(sub_dirz)==0){stop("All files have been processed.")}
sub_dirz

# tc_type <- "tmax"
k0 <- 3; sub_dirz[k0]

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
			glcc <- terra::rast(filez[1,fn])
		})
		
		# Skip if file exists
    if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn]))){

		# Load polygons
		suppressWarnings({
			suppressMessages({
					map <- sf::st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
			})
		})

		# Clasifications
		forest <- c(1:5)
    wetlands <- c(11,17)
    desert <- c(6:7,8,9:10,16)
    farm <- c(12,14)
    urban <- c(13)

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
			tmp_glcc <- paste0(tempfile(),"_glcc.tif")
			glcc_m0 <- glcc %>% terra::crop(terra::ext(bbx),filename=tmp_glcc)
			
			# Zonal stats
			map_z_farmland <- terra::zonal(x=glcc_m0%in%farm,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("GLCC_FARMLAND")
			map_z_forest <- terra::zonal(x=glcc_m0%in%forest,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("GLCC_FOREST")
			map_z_open <- terra::zonal(x=glcc_m0%in%desert,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("GLCC_OPEN")
			map_z_urban <- terra::zonal(x=glcc_m0%in%urban,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("GLCC_URBAN")
			map_z_wetland <- terra::zonal(x=glcc_m0%in%wetlands,z=map00.0 %>% sf::st_as_sf() %>% terra::vect(),fun="mean",na.rm=TRUE) %>% data.table::setnames("GLCC_WETLAND")

			# Delete temporary files
			rm(glcc_m0)
			file.remove(grep(stringr::str_split(tmp_glcc,"/",simplify=TRUE)[4],dir(tempdir()),value=TRUE)%>%paste0(tempdir(),"/",.));

			# Save
			glcc.mat <- list(map_z_farmland,map_z_forest,map_z_open,map_z_urban,map_z_wetland)%>%dplyr::bind_cols() %>% data.table::as.data.table() %>% .[,SG_POLYID:=poly_idz] %>% dplyr::select("SG_POLYID",dplyr::everything())

			# # # Preview
			# plot(slope_m0[["TRI"]])
			# dev.off()
			# rnkz <- elev.mat[,rank(slope_mean)/max(rank(slope_mean))]
			# plot(map00.0[,geometry],col=rgb(rnkz,rnkz,rnkz))
			# dev.off()
				
			# Save
			saveRDS(glcc.mat,file=sub_dirz[k0,fnn])
			
			# Progress
			print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[6] %>% gsub(".RDS","",.)," -- finished"))
      t2 <- Sys.time()
      print(t2-t1)

      # Close if map>0
		}

	# CLOSE skip if exists
	}
	
	# Clear memory
	rm(glcc); gc()

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

