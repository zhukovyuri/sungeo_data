# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/Ethnic/Geoprocessing/greg_geoprocess.R")' &
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
list.of.packages <- c("sf","raster","dplyr","stringr","data.table","countrycode","parallel","maptools","stringi")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
# library(SUNGEO)
sungeo_lib <- grep("R\\/x86",.libPaths(),value=T)
library(SUNGEO,lib.loc=sungeo_lib)

# sessionInfo()

# Log file
log_file <- paste0("../r_log_errors_greg_",Sys.time() %>% gsub("-| |\\:|EDT","",.),".txt") 
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)

#############################
## Prepare environment
#############################

# # Temp dir
tempd <- tempdir()
# unlink(tempd,recursive=TRUE)

# Load polygons and fix character encoding issue (convert all to ASCII)
load("Ethnic/GREG/Raw/GREG_ETHNIC.RData")
greg <- greg %>% sf::st_as_sf() %>% sf::st_set_crs("+init=epsg:4326") %>% dplyr::mutate_if(sapply(.,is.factor),function(x){as.character(x) %>% iconv("latin1","UTF-8", "byte")  %>% stringi::stri_trans_general("Latin-ASCII")})

# Population raster (from 1990)
r_pop <- "Population/GPW_v3/gl_gpwv3_pcount_90_ascii_25.zip" %>% unzip(exdir=tempd) %>% grep("ag.asc$|min.asc$",.,value=TRUE) %>% raster::raster()
raster::crs(r_pop) <- raster::crs("+init=epsg:4326")

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
suppressWarnings({
sub_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,fnn := paste0("Ethnic/GREG/Processed/",geoset,"/","GREG_",iso3,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[iso3!="ZZZ",] %>% .[,exists := fnn %>% file.exists()] %>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F)
}); sub_dirz[,mean(exists)]

# Subset
sub_dirz <- sub_dirz[exists==FALSE,] %>% .[!duplicated(umap)]


######################################
# Set up parallelization
######################################

# Temporary directory
# tempd <- tempdir()

# Skip existing files?
skip_existing <- TRUE
# sub_dirz %>% as.data.frame()

k0 <- 418; sub_dirz[k0,fnn]
# sub_dirz[,grep("SEN",fnn)]

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1) %>% min(nrow(sub_dirz),.)

# PSOCK
cl <- parallel::makePSOCKcluster(ncores, outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("skip_existing","sub_dirz","admz","ncores","mem_all","mem0","log_file","tempd","greg","r_pop","sungeo_lib"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(dplyr))
parallel::clusterEvalQ(NULL, expr=library(sf))
parallel::clusterEvalQ(NULL, expr=library(R.utils))
parallel::clusterEvalQ(NULL, expr=library(stringr))
parallel::clusterEvalQ(NULL, expr=library(raster))
parallel::clusterEvalQ(NULL, expr=library(SUNGEO,lib.loc=sungeo_lib))
cntz_list <- parallel::parLapply(NULL,1:nrow(sub_dirz),function(k0){

# nrow(sub_dirz):1

# # Forking
# cntz_list <- parallel::mclapply(nrow(sub_dirz):1,function(k0){

# # # Single core
# cntz_list <- lapply(1:50,function(k0){print(k0)

  #######################
  # Begin script
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

		# Crop by bounding box
		bbx <- sf::st_bbox(map)
		suppressWarnings({
			suppressMessages({
				greg_ <- greg %>% sf::st_intersection(bbx[c("xmin","xmax","ymin","ymax")] %>% raster::extent() %>% as("SpatialPolygons") %>% sf::st_as_sf() %>% sf::st_set_crs(st_crs(greg)$input)) %>% sf::st_buffer(0)
			})
		})

		# Proceed if non-empty intersection
		if(nrow(greg_)>0){

			# Calculate # of groups
			map00.0 <- SUNGEO::poly2poly_ap(
	                   poly_from = greg_,
	                   poly_to = map,
	                   poly_to_id = idvar,
	                   varz ="GROUP1",
	                   funz = function(x,w){length(unique(x))},
	                   char_varz = c("G1SHORTNAM"),
	                   char_assign = "all_overlap"
	                  ) %>% data.table::setnames(c("G1SHORTNAM"),c("GREG_GROUPS")) %>% as.data.table() %>% .[,GREG_NGROUPS := GREG_GROUPS %>% stringr::str_split("\\|") %>% sapply(length)] #%>% .[,.SD,.SDcols=c(idvar,"GREG_GROUPS","GREG_NGROUPS")]

			# Calculate ELF (area weights)
			suppressWarnings({
				suppressMessages({
					elf_aw <- sf::st_intersection(greg_,map) %>% dplyr::group_by(get(idvar),G1SHORTNAM) %>% dplyr::summarize() %>% as.data.table() %>% data.table::setnames(names(.)[1],idvar) %>% .[,int_area:=sf::st_as_sf(.) %>% sf::st_area()] %>% .[,map_area := sum(int_area), by=idvar] %>% .[,group_share := as.numeric(int_area/map_area)] %>% .[,GREG_ELF_AW := 1-sum(group_share^2),by=idvar] %>% .[,.SD,.SDcols=c(idvar,"GREG_ELF_AW")] %>% unique()
				})
			})
			elf_aw

			# Calculate ELF (pop weights)
			pop_ <- r_pop %>% raster::crop(bbx[c("xmin","xmax","ymin","ymax")] %>% raster::extent())
			pop_file <- pop_ %>% capture.output() %>% grep("^source",.,value=TRUE) %>% stringr::str_split(":",simplify=TRUE) %>% .[2] %>% stringr::str_trim()
			suppressWarnings({
				suppressMessages({
				elf_pw <- sf::st_intersection(greg_,map)  %>% dplyr::group_by(get(idvar),G1SHORTNAM) %>% dplyr::summarize() %>% as.data.table() %>% data.table::setnames(names(.)[1],idvar) %>% .[,pop_local := sf::st_as_sf(.) %>% sf::st_buffer(0) %>% sf::st_cast("MULTIPOLYGON")  %>% raster::extract(pop_,. ,fun=sum,factors=T,buffer=1000, small=T,na.rm=T)] %>% .[,pop_share := pop_local/sum(pop_local), by=idvar]  %>% .[,GREG_ELF_PW := 1-sum(pop_share^2),by=idvar] %>% .[,.SD,.SDcols=c(idvar,"GREG_ELF_PW")] %>% unique()
				})
			})
			elf_pw

			map00.0 <- map00.0 %>% merge(elf_aw,by=idvar) %>% merge(elf_pw,by=idvar)

			plot(map00.0 %>% sf::st_as_sf() %>% .["GREG_ELF_AW"]); # dev.off()

			# Extract covariates
			greg_mat <- map00.0 %>% .[,POLYGON_ID := paste0(sub_dirz[k0,paste0(iso3,"_",geoset,adm_yr,"_",adm)],"_",get(idvar))] %>% .[,.SD,.SDcols=c("POLYGON_ID",idvar,grep("^GREG",names(.),value=TRUE))]

			# # Delete temporary files
			rm(greg_,map00.0,elf_aw,elf_pw)
			if(pop_file!="memory"){file.remove(c(pop_file,gsub("grd$","gri",c(pop_file))))}

	  		# Save
			saveRDS(greg_mat,file=sub_dirz[k0,fnn])

			# Progress
			print(paste0(sub_dirz[k0,fnn] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub(".RDS","",.)," -- finished  :)"))
	        t2 <- Sys.time()
	        print(t2-t1)


		# Close if greg_>0
		}

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
# # q()
# # n
# # pkill -9 R
# # R

# unlink(tempd,recursive=TRUE)
unlink(".RData")
dir(paste0(tempd,"/raster"))