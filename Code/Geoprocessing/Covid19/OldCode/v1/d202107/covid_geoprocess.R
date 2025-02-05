# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO_Flu/Code/Preprocessing/flu_preprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO_Flu/Code/Preprocessing/flu_preprocess.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","readxl")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
sungeo_lib <- grep("R\\/x86",.libPaths(),value=T)
library(SUNGEO,lib.loc=sungeo_lib)

# Log file
log_file <- paste0("../r_log_errors_covid_",Sys.time() %>% gsub("-| |\\:|EDT","",.),".txt") 
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)
file.exists(log_file)


#####################################
# Prepare environment
#####################################


# Skip existing files?
skip_existing <- TRUE

# Set tempdir
temp <- tempdir()

dir(paste0("Covid19/JHU/Processed/GADM_simp/"))

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")
# admz <- c("SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
list.files("Covid19/JHU/Preprocessed",pattern="REPORTS")
sub_dirz <- list.files("Covid19/JHU/Preprocessed",pattern="REPORTS") %>% stringr::str_split("_|.RDS",simplify=T) %>% .[,c(3)] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), source="JHUCSSEC19", cov19_f0 = list.files("Covid19/JHU/Preprocessed",full.names=T,pattern="REPORTS")) %>% dplyr::arrange(iso3) %>% as.data.table() %>% merge({lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% stringr::str_split("_",simplify=T) %>% .[,1:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("geoset","iso3","adm_yr","adm")) %>% dplyr::mutate(geoset = geoset %>% as.character(),iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table()}) %>% dplyr::bind_rows()},by="iso3",all.x=T,all.y=F,allow.cartesian=TRUE) %>% 
	.[,fnn_simp_year := paste0("Covid19/JHU/Processed/",geoset,"_simp/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_YEAR.RDS")] %>% 
	.[,fnn_simp_month := paste0("Covid19/JHU/Processed/",geoset,"_simp/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_MONTH.RDS")] %>% 
	.[,fnn_simp_week := paste0("Covid19/JHU/Processed/",geoset,"_simp/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_WEEK.RDS")] %>% 
	.[,fnn_tess_year := paste0("Covid19/JHU/Processed/",geoset,"_tess/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_YEAR.RDS")] %>% 
	.[,fnn_tess_month := paste0("Covid19/JHU/Processed/",geoset,"_tess/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_MONTH.RDS")] %>% 
	.[,fnn_tess_week := paste0("Covid19/JHU/Processed/",geoset,"_tess/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_WEEK.RDS")] %>% 
	.[,fnn_panel_year := paste0("Covid19/JHU/Panel/",geoset,"/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_YEAR.RDS")] %>% 
	.[,fnn_panel_month := paste0("Covid19/JHU/Panel/",geoset,"/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_MONTH.RDS")] %>% 
	.[,fnn_panel_week := paste0("Covid19/JHU/Panel/",geoset,"/",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_WEEK.RDS")] %>% 
	.[,exists_simp_year := fnn_simp_year %>% file.exists()] %>% .[,exists_simp_month := fnn_simp_month %>% file.exists()] %>% .[,exists_simp_week := fnn_simp_week %>% file.exists()] %>% 
	.[,exists_tess_year := fnn_tess_year %>% file.exists()] %>% .[,exists_tess_month := fnn_tess_month %>% file.exists()] %>% .[,exists_tess_week := fnn_tess_week %>% file.exists()] %>% 
	.[,exists_panel_year := fnn_panel_year %>% file.exists()] %>% .[,exists_panel_month := fnn_panel_month %>% file.exists()] %>% .[,exists_panel_week := fnn_panel_week %>% file.exists()] %>% 
	merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F); 
mean(sub_dirz$exists_simp_year); mean(sub_dirz$exists_simp_month); mean(sub_dirz$exists_simp_week); mean(sub_dirz$exists_tess_year); mean(sub_dirz$exists_tess_month); mean(sub_dirz$exists_tess_week); 

# Keep only unique map sets
sub_dirz <- sub_dirz %>% .[!duplicated(umap)&!is.na(umap)]

# Keep only non-existing files
sub_dirz <- sub_dirz[!exists_simp_year|!exists_simp_month|!exists_simp_week|!exists_tess_year|!exists_tess_month|!exists_tess_week,]


# Dates
source("../Code/functions.R")
ticker <- make_ticker(date_min=20200101)

# Units and precision levels ## ADD GRID IDS
adz <- data.frame(
  sunit = c("ADM0","ADM1","ADM2","CST","PRIO","HEX05"),
  prec = c("adm0|adm1|adm2|settlement","adm1|adm2|settlement","adm2|settlement","adm1|adm2|settlement","adm2|settlement","adm2|settlement"),
  stringsAsFactors = F) %>% as.data.table()
timz <- data.frame(
  tunit = c("year","month","week"),
  tvar = c("YEAR","YRMO","WID"),
  prec = c("year|month|week|day","month|week|day","week|day"),
  stringsAsFactors=F) %>% as.data.table()


###########################
# Loop over files
###########################

k0 <- 8189; sub_dirz[k0,]
k0 <- sub_dirz[,grep("DEU",iso3)][4]; sub_dirz[k0,]
# sub_dirz %>% as.data.frame()

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

# PSOCK
# ncores <- min(nrow(sub_dirz),detectCores()/1.5)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("skip_existing","sub_dirz","admz","temp","ticker","timz","log_file","sungeo_lib","ncores","adz"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(dplyr))
parallel::clusterEvalQ(NULL, expr=library(stringr))
parallel::clusterEvalQ(NULL, expr=library(stringi))
parallel::clusterEvalQ(NULL, expr=library(sf))
parallel::clusterEvalQ(NULL, expr=library(SUNGEO,lib.loc=sungeo_lib))
cntz_list <- parLapply(NULL,nrow(sub_dirz):1,function(k0){

# # Forking
# cntz_list <- mclapply(1:nrow(sub_dirz),function(k0){

  # Error catching
  tryCatch({
    	
  		# Loop over time units
		t0 <- 2;  timz[t0]
		tunit_list <- lapply(1:nrow(timz),function(t0){

			# Execute only raw data exist
			if(sub_dirz[k0,file.exists(cov19_f0)]&(
				(!skip_existing|(skip_existing&(
					!file.exists(sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))])
					|
					!file.exists(sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))])
					|
					!file.exists(sub_dirz[k0,get(paste0("fnn_panel_",timz[t0,tunit]))])
					)
				)))){
			
				# Error catching
				tryCatch({

			        # Timer
			        t1 <- Sys.time()

			        # print(paste0("starting ", fnn_out_jhu))

					# Load data files for country, filter by precision
			    	jhu_data_ <- readRDS(sub_dirz[k0,cov19_f0]) %>% as.data.table() %>% .[,DATE:=as.character(DATE) %>% as.numeric()] %>% merge(ticker,by="DATE",all.x=T,all.y=F) %>% .[grepl(adz[sunit%in%sub_dirz[k0,adm],prec],GEO_PRECISION,ignore.case=TRUE)] #%>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),remove=F,na.fail=F)
			    	jhu_data_
					
					# Variable selection
					sum_jhu_data <- names(jhu_data_) %>% grep("_NEW$",.,value=T)
					max_jhu_data <- names(jhu_data_) %>% grep("_CUM$",.,value=T)

			        # Source functions
			        source("../Code/functions.R")

			        # Load polygons
			        suppressWarnings({
			        	suppressMessages({
				        map <- st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
				        })
			        })

			        # Proceed only if >0 rows
			        if(nrow(jhu_data_)>0&nrow(map)>0){
	          
						# Unique geo ID
						idvar <- paste0(sub_dirz[k0,adm],"_CODE")
						tvar <- timz[t0,tvar]
	      
						# Create ID if missing
						if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(sub_dirz[k0,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

						# f0_ <- 1
	          
			            # Aggregate measures for each time interval
						jhu_data_s <- jhu_data_ %>% .[,lapply(.SD,function(x){sum(x,na.rm=T)}),.SDcols=sum_jhu_data,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar)]
						jhu_data_m <- jhu_data_ %>% .[,lapply(.SD,function(x){max(x,na.rm=T)}),.SDcols=max_jhu_data,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar)]
						jhu_data_t <- jhu_data_s %>% merge(jhu_data_m,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar))
						rm(jhu_data_s,jhu_data_m)

						# Vector of time intervals
						cd_tint <- jhu_data_ %>% .[,get(tvar)] %>% unique() %>% .[!is.na(.)]
						all_tint <- ticker[,get(tvar) ] %>% unique() %>% .[.>=min(cd_tint) & .<=max(cd_tint)] %>% .[!is.na(.)]
						cd_tout <- all_tint[!all_tint%in%cd_tint]

						# # Fill empty time slots
						# if(length(cd_tout)>0){
						# 	cent_xy <- suppressWarnings({sf::st_centroid(map) %>% sf::st_coordinates() %>% .[1,]})
						# 	jhu_data_t <- data.table::rbindlist(list(jhu_data_t,data.table(LONGITUDE=cent_xy["X"],LATITUDE=cent_xy["Y"],tvar=cd_tout) %>% data.table::setnames("tvar",tvar) %>% .[,eval(c(sum_jhu_data,max_jhu_data)) := 0]),fill=T)
						# }
						# Add blank events to every dataset
						cent_xy <- suppressWarnings({sf::st_centroid(map) %>% sf::st_coordinates() %>% .[1,]})
						jhu_data_t <- data.table::rbindlist(list(jhu_data_t,data.table(LONGITUDE=cent_xy["X"],LATITUDE=cent_xy["Y"],tvar=all_tint) %>% data.table::setnames("tvar",tvar) %>% .[,eval(c(sum_jhu_data,max_jhu_data)) := 0]),fill=T)

						# jhu_data_t[,get(tvar) %>% unique() %>% sort()]
						# jhu_data_t[get(tvar)==202002,]
						###		
						# Simple overlay
						###

						# Execute only if file doesn't exist
						if(!skip_existing|(skip_existing&(!file.exists(sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))])))){
							
							# Simple overlay
							# t0_ <- all_tint[3]
							jhu_out <- lapply(all_tint,function(t0_){
								# print(t0_)
								t_mat_0 <- SUNGEO::point2poly_simp(
									polyz=map,
									pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% sf::st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map)),
									# pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% sf::st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(4326)) %>% sf::st_transform(st_crs(map)),
									varz=c(sum_jhu_data,max_jhu_data), na_val=0) %>% 
									as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",tvar) %>% dplyr::select(eval(tvar),names(map) %>% .[!grepl("geometry",.)],everything())
								t_mat_0
							}) %>% dplyr::bind_rows() %>% as.data.table() %>% .[order(get(idvar),get(tvar)),] %>% data.table::setnames(toupper(names(.))) %>% .[,POLYGON_ID := paste0(sub_dirz[k0,paste0(iso3,"_",geoset,adm_yr,"_",adm,"_")],get(idvar))] %>% .[,.SD,.SDcols=c("POLYGON_ID",timz[t0,tvar],grep("^CASES|^DEATHS|^RECOVERED",names(.),value=T))]

							# Save
							saveRDS(jhu_out,file=sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))])
				            print(paste0(sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))] %>% stringr::str_split("\\/") %>% sapply(dplyr::last) %>% gsub("\\.RDS","",.)," -- finished simp :)"))


						# Close "skip_existing" if statement
						}

						### 
						# Tessellation 
						###

						# Execute only if file doesn't exist
						if(!skip_existing|(skip_existing&(!file.exists(sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))])))){

							# Error catching
							tryCatch({

								# Tessellation
								# t0_ <- all_tint[2]
								suppressWarnings({
								suppressMessages({
								jhu_tess <- lapply(all_tint,function(t0_){#print(t0_)
										t_mat_0 <- SUNGEO::point2poly_tess(
											polyz=map,
											pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% sf::st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map)),
											poly_id=idvar,
											varz=c(sum_jhu_data,max_jhu_data),
											funz=function(x,w){sum(x*w,na.rm=T)}) %>% 
										as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",tvar) %>% dplyr::select(eval(tvar),names(map) %>% .[!grepl("geometry",.)],everything())
									t_mat_0
								}) %>% dplyr::bind_rows() %>% as.data.table() %>% .[order(get(idvar),get(tvar)),] %>% data.table::setnames(toupper(names(.)))  %>% .[,POLYGON_ID := paste0(sub_dirz[k0,paste0(iso3,"_",geoset,adm_yr,"_",adm,"_")],get(idvar))] %>% .[,.SD,.SDcols=c("POLYGON_ID",timz[t0,tvar],grep("^CASES|^DEATHS|^RECOVERED",names(.),value=T))]
								})
								})
								# Save
								saveRDS(jhu_tess,file=sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))])
					            print(paste0(sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))] %>% stringr::str_split("\\/") %>% sapply(dplyr::last) %>% gsub("\\.RDS","",.)," -- finished tess :)"))
	          
					        # Close error catch
					        },error=function(e){print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, paste(source,iso3,geoset,adm_yr,adm," TESSELATION")]));print(e)})

  						# Close "skip_existing" if statement
						}

						###
						# Combine into single panel
						###

						# Execute only if file doesn't exist
						if(!skip_existing|(skip_existing&(
							file.exists(sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))])
							&
							!file.exists(sub_dirz[k0,get(paste0("fnn_panel_",timz[t0,tunit]))])
							))){
							if(file.exists(sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))])){
								# Open
								jhu_out <- readRDS(sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))])
								jhu_tess <- readRDS(sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))])
								# Combine
								jhu_panel <- jhu_out %>% merge(jhu_tess,by=c("POLYGON_ID",tvar),all.x=TRUE,all.y=FALSE)
								# Save
								saveRDS(jhu_panel,file=sub_dirz[k0,get(paste0("fnn_panel_",timz[t0,tunit]))])
					            print(paste0(sub_dirz[k0,get(paste0("fnn_panel_",timz[t0,tunit]))] %>% stringr::str_split("\\/") %>% sapply(dplyr::last) %>% gsub("\\.RDS","",.)," -- finished panel :)"))
					        }
					        if(!file.exists(sub_dirz[k0,get(paste0("fnn_tess_",timz[t0,tunit]))])){
								# Open
								jhu_out <- readRDS(sub_dirz[k0,get(paste0("fnn_simp_",timz[t0,tunit]))])
								# Combine
								jhu_panel <- jhu_out %>% dplyr::bind_cols(matrix(NA_real_,nrow=nrow(.),ncol=length(c(sum_jhu_data,max_jhu_data))) %>% data.table::as.data.table() %>% data.table::setnames(paste0(c(sum_jhu_data,max_jhu_data),"_AW")))
								# Save
								saveRDS(jhu_panel,file=sub_dirz[k0,get(paste0("fnn_panel_",timz[t0,tunit]))])
					            print(paste0(sub_dirz[k0,get(paste0("fnn_panel_",timz[t0,tunit]))] %>% stringr::str_split("\\/") %>% sapply(dplyr::last) %>% gsub("\\.RDS","",.)," -- finished panel :)"))
					        }

					    # Close "skip_existing" if statement
						}

				        t2 <- Sys.time()
				        print(t2-t1)

						# # Close "skip_existing" if statement
						# }


					# Close if statement [nrow(map)>0]
					}

			        # Clean workspace
			        rm(list=ls()[grep("jhu_data_|cnet_|map",ls())])
			        # gc(reset=T)

			     # Close error catch
		        },error=function(e){print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, paste(source,iso3,geoset,adm_yr,adm)]));print(e)})

			# Close if statement (file exists)
			}

		# Close timz loop
		})

  # Close error catch
  },error=function(e){
  write(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, paste(source,iso3,geoset,adm_yr,adm)]), file = log_file,append=TRUE);
  print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, paste(source,iso3,geoset,adm_yr,adm)]));
  print(e)})

# Close parLapply
})
parallel::stopCluster(cl)
gc()
# q()
# n
# pkill -9 R
# R

# # Close mclapply
# },mc.cores = min(nrow(sub_dirz),detectCores()-2))
# # gc()