# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
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
library(SUNGEO)


# ###############################################
# ###############################################
# ###############################################
# ## JHU + Barcelo panel data
# ###############################################
# ###############################################
# ###############################################

# # Load JHU global
# jhu_data <- list.files("Covid19/JHU/Raw/",pattern="jhu_data", full.names=T) %>% rev() %>% .[1] %>% readRDS() %>% .[,GEO_ID := paste0(LONGITUDE,"_",LATITUDE) %>% as.factor() %>% as.numeric()] %>% .[order(GEO_ID,DATE_ALT)] %>% .[,CASES_NEW := c(NA_real_,diff(CONFIRMED) %>% abs()),by=GEO_ID] %>% .[,DEATHS_NEW := c(NA_real_,diff(DEATHS) %>% abs()),by=GEO_ID] %>% .[,RECOVERED_NEW := c(NA_real_,diff(RECOVERED) %>% abs()),by=GEO_ID] %>% data.table::setnames(c("CONFIRMED","DEATHS","RECOVERED"),c("CASES_CUM","DEATHS_CUM","RECOVERED_CUM")) %>% .[,.SD,.SDcols=c("GEO_ID","ADM0_NAME","ADM1_NAME","ADM2_NAME","LONGITUDE","LATITUDE","GEO_PRECISION","DATE_ALT","CASES_CUM","DEATHS_CUM","RECOVERED_CUM","CASES_NEW","DEATHS_NEW","RECOVERED_NEW")] %>% data.table::setnames("DATE_ALT","DATE") %>% .[is.na(ADM1_NAME),ADM1_NAME := ADM0_NAME %>% str_split(", |,",simplify=T) %>% apply(1,function(x){rev(x[!is.na(x)&nchar(x)>0])[2]})] %>% .[is.na(ADM2_NAME),ADM2_NAME := ADM0_NAME %>% str_split(", |,",simplify=T) %>% apply(1,function(x){rev(x[!is.na(x)&nchar(x)>0])[3]})] %>% .[,ADM0_NAME := ADM0_NAME %>% str_split(", |,",simplify=T) %>% apply(1,function(x){rev(x[!is.na(x)&nchar(x)>0])[1]})] %>% .[,GEO_PRECISION := apply(.SD,1,function(x){paste0("ADM",length(x[!is.na(x)&nchar(x)>0])-1)}),.SDcols=c("ADM0_NAME","ADM1_NAME","ADM2_NAME")] %>% .[,ISO_A3 := ADM0_NAME %>% countrycode("country.name","iso3c")] %>% dplyr::select(GEO_ID,ISO_A3,everything())
# jhu_data[ADM0_NAME%in%c("Saint Martin","St. Martin"),ISO_A3 := "MAF"]
# jhu_data[ADM0_NAME%in%c("Channel Islands"),ISO_A3 := "CHI"]
# jhu_data[ADM0_NAME%in%c("Kosovo"),ISO_A3 := "XKX"]
# jhu_data[ADM0_NAME%in%c("Micronesia"),ISO_A3 := "FSM"]
# jhu_data[ADM0_NAME%in%c("South"),ISO_A3 := "KOR"]
# jhu_data[ADM0_NAME%in%c("South"),GEO_PRECISION := "ADM0"]
# jhu_data[ADM0_NAME%in%c("South"),c("ADM0_NAME","ADM1_NAME") := .("South Korea",NA_character_)]
# # Save country files
# disag <- sort(unique(jhu_data$ISO_A3))
# mclapply(disag,function(d0){
# 	saveRDS(jhu_data[ISO_A3%in%d0,],file=paste0("Covid19/JHU/Preprocessed/JHU_REPORTS_",d0,".RDS"))
# 	},mc.cores=detectCores()-1)


# # Load JHU USA, convert to long, merge
# jhu_deaths <- list.files("Covid19/JHU/Raw/",pattern="time_series_covid19_deaths_US", full.names=T) %>% rev() %>% .[1] %>% readRDS()
# jhu_deaths_long <- jhu_deaths %>% melt(., id.vars = c("FIPS"),measure.vars = grep("^D_",names(.),value=T)) %>% .[order(FIPS,variable)] %>% .[!is.na(FIPS)&nchar(FIPS)>0,] %>% .[,delta := c(value[1],diff(value) %>% abs()),by=FIPS]  %>% data.table::setnames(c("variable","value","delta"),c("DATE","DEATHS_CUM","DEATHS_NEW"))
# jhu_cases <- list.files("Covid19/JHU/Raw/",pattern="time_series_covid19_confirmed_US", full.names=T) %>% rev() %>% .[1] %>% readRDS()
# jhu_cases_long <- jhu_cases %>% melt(., id.vars = c("FIPS"),measure.vars = grep("^D_",names(.),value=T)) %>% .[order(FIPS,variable)] %>% .[!is.na(FIPS)&nchar(FIPS)>0,] %>% .[,delta := c(value[1],diff(value) %>% abs()),by=FIPS]  %>% data.table::setnames(c("variable","value","delta"),c("DATE","CASES_CUM","CASES_NEW"))
# jhu_long <- jhu_cases[,.SD,.SDcols=names(jhu_cases)[!grepl("^D",names(jhu_cases))]] %>% merge(jhu_cases_long,by="FIPS") %>% merge(jhu_deaths_long,by=c("FIPS","DATE")) %>% .[,DATE := gsub("^D_","",DATE)]
# rm(jhu_cases,jhu_deaths,jhu_cases_long,jhu_deaths_long)
# # Save country files
# disag <- sort(unique(jhu_long$ISO3))
# mclapply(disag,function(d0){
# 	saveRDS(jhu_long[ISO3%in%d0,],file=paste0("Covid19/JHU/Preprocessed/JHU_TS_",d0,".RDS"))
# 	},mc.cores=detectCores()-1)

# # Load Barcelo, convert to long
# dir("Covid19/CoronaNet/Raw")
# cnet <- list.files("Covid19/CoronaNet/Raw/",pattern="geo\\.RDS",full.names=T) %>% readRDS() %>% as.data.table() %>% .[,geometry:=NULL] %>% .[,GEO_ID := paste0(longitude,"_",latitude) %>% as.factor() %>% as.numeric()] %>% .[, CNET_NPI := 1*grepl("Emergency|Monitoring|Resources|Distancing|Closure|Regulation|Lockdown|Quarantine|Restriction|Curfew|Border|Gatherings|Awareness|Anti-Disinformation|Other Policy|Hygiene",type)] %>% .[, CNET_PI := 1*grepl("Testing|Vaccine",type)] %>% .[, CNET_NPI_CLOSE := 1*grepl("Closure|Regulation|Lockdown|Quarantine|Restriction|Curfew|Border|Gatherings",type)] %>% .[, CNET_NPI_PINFO := 1*grepl("Awareness|Anti-Disinformation",type)] %>% .[, CNET_NPI_ENFOR := 1*grepl("Fines|Penalties|Jail",compliance)] %>% .[,CNET_NEW := 1*grepl("new",entry_type)] %>% .[,CNET_CHANGE := 1*grepl("Change",update_type)] %>% .[,CNET_STRENGTHEN := 1*grepl("Change",update_type)*grepl("Strength",update_level)] %>% .[,CNET_END := 1*grepl("End",update_type)] %>% data.table::setnames(c("country","province","city"),c("ADM0_NAME","ADM1_NAME","ADM2_NAME")) %>% .[,.SD,.SDcols=c("GEO_ID","ISO_A3","ADM0_NAME","ADM1_NAME","ADM2_NAME","longitude","latitude","geo_precision","date_start","date_end",grep("^CNET_",names(.),value=T))] %>% data.table::setnames(toupper(names(.))) %>% .[is.na(DATE_END)|nchar(DATE_END)==0,DATE_END:=Sys.Date() %>% as.character()] %>% .[DATE_END<DATE_START,c("DATE_START","DATE_END") := .(DATE_END,DATE_START)]
# cnet <- mclapply(1:nrow(cnet),function(o0){
# 	# cat(o0,"\r")
# 	# print(o0)
# 	cnet_sub <- cnet[o0,] %>% .[,seq(as.Date(DATE_START),as.Date(DATE_END),by="day")] %>% data.table(DATE = .) %>% bind_cols(cnet[rep(o0,nrow(.)),]) %>% .[,c("CNET_NEW","CNET_CHANGE","CNET_STRENGTHEN","CNET_END") := lapply(.SD,function(x){c(x[1],rep(0,nrow(.)-1))}),.SDcols=c("CNET_NEW","CNET_CHANGE","CNET_STRENGTHEN","CNET_END")]
# 	cnet_sub
# },mc.cores=detectCores()-1) %>% dplyr::bind_rows() %>% .[,c("DATE_START","DATE_END") := NULL] %>% .[,DATE:=gsub("-","",DATE)] %>% .[DATE <= Sys.Date() %>% as.character() %>% gsub("-","",.),]
# # Save country files
# disag <- sort(unique(cnet$ISO_A3)) %>% .[nchar(.)==3]
# mclapply(disag,function(d0){
# 	saveRDS(cnet[ISO_A3%in%d0,],file=paste0("Covid19/CoronaNet/Preprocessed/CNET_",d0,".RDS"))
# 	},mc.cores=detectCores()-1)

# # Clear memory
# rm(list=ls())
# ls()


###############################################
###############################################
###############################################
# Loop over admz
###############################################
###############################################
###############################################

rm(list=ls())

# List of countries to skip
skip_poly <- NULL

# Skip existing files?
skip_existing <- FALSE

# Set tempdir
temp <- tempdir()

# List of countries in xSub
# sub_dirz <- list.files("ProcessedData/USA/CityLevel", full.names=T) %>% data.table(f0 = .,iso3 = "USA", source = "SUNGEO_Flu")

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

# Subset
# date_range <- range(c(jhu_data$DATE,jhu_long$DATE,cnet$DATE) %>% .[nchar(.)==8],na.rm=T)
date_range <- c("20200101",Sys.Date() %>% as.character() %>% gsub("-","",.))
ticker <- ticker[as.character(ticker$DATE)>=date_range[1]&as.character(ticker$DATE)<=date_range[2],]
head(ticker)

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")

# Units and precision levels
adz <- data.frame(
  sunit = c("ADM0","ADM1","ADM2","CST"),
  prec = c("ADM0|ADM1|ADM2|ADM3|settlement","ADM1|ADM2|ADM3|settlement","ADM2|ADM3|settlement","ADM1|ADM2|ADM3|settlement"),
  stringsAsFactors = F) %>% as.data.table()
timz <- data.frame(
  tunit = c("year","month","week"),
  tvar = c("YEAR","YRMO","WID"),
  prec = c("year|month|week|day","month|week|day","week|day"),
  stringsAsFactors=F) %>% as.data.table()

# ...

# Loop over admz
a0 <- 6; admz[a0]
admz_list <- lapply( 1:length(admz),function(a0){

  print(paste0(admz[a0],", ",a0,"/",length(admz)))

  # List of boundary files
  admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table()
  admz_dir <- admz_dir[iso3 %in% "USA",] %>% .[.N:1,]
  # admz_dir[,which(iso3 %in% "DEU"&yr %in%1818)]

  # Proceed if at least one adm file exists
  if(nrow(admz_dir)>0){

    # Number of cores
    mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
    mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
    ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

    # Loop over files
    a0_ <- 1; admz_dir[a0_,]
    a0_list <- mclapply(1:nrow(admz_dir),function(a0_){
    	
		# Loop over time units
		t0 <- 1;  timz[t0]
		tunit_list <- lapply(1:nrow(timz),function(t0){


			# ####
			# # JHU
			# ####

			# # Output file name
			# fnn_out_jhu <- paste0("JHU_",admz_dir[a0_,iso3],"_",admz[a0],admz_dir[a0_,yr],"_",admz_dir[a0_,adm],"_",timz[t0,tunit] %>% toupper(),".RDS")

			# # Execute only raw data exist
			# if(file.exists(paste0("Covid19/JHU/Preprocessed/JHU_REPORTS_",admz_dir[a0_,iso3],".RDS"))&(
			# 	(!skip_existing|(skip_existing&(!fnn_out_jhu%in%dir(paste0("Covid19/JHU/Processed/",admz[a0],"_simp/")))))
			# 	|(!skip_existing|(skip_existing&(!fnn_out_jhu%in%dir(paste0("Covid19/JHU/Processed/",admz[a0],"_tess/")))))
			# 	)
			# 	){
			
			# 	# Error catching
			# 	tryCatch({

			#         # Timer
			#         t1 <- Sys.time()

			#         print(paste0("starting ", fnn_out_jhu))

			# 		# Load data files for country, filter by precision
			#     	jhu_data_ <- readRDS(paste0("Covid19/JHU/Preprocessed/JHU_REPORTS_",admz_dir[a0_,iso3],".RDS")) %>% as.data.table() %>% .[,DATE:=as.character(DATE)] %>% merge(ticker,by="DATE",all.x=T,all.y=F) %>% .[grepl(adz[sunit%in%admz_dir[a0_,adm],prec],GEO_PRECISION)] #%>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),remove=F,na.fail=F)
					
			# 		# Variable selection
			# 		sum_jhu_data <- names(jhu_data_) %>% grep("_NEW$",.,value=T)
			# 		max_jhu_data <- names(jhu_data_) %>% grep("_CUM$",.,value=T)

			#         # Source functions
			#         source("../Code/functions.R")

			#         # Load polygons
			#         suppressWarnings({
		 #        	suppressMessages({
			#         map <- st_read(admz_dir[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
			#         # map <- st_read(admz_dir[a0_,f0], quiet=T) %>% SUNGEO::utm_select() %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
			#         })
			#         })

			#         # Proceed only if >0 rows
			#         if(nrow(jhu_data_)>0&nrow(map)>0){
	          
			# 			# Unique geo ID
			# 			idvar <- paste0(admz_dir[a0_,adm],"_CODE")
			# 			tvar <- timz[t0,tvar]
	      
			# 			# Create ID if missing
			# 			if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(admz_dir[a0_,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

			# 			f0_ <- 1
	          
			#             # Aggregate measures for each time interval
			# 			jhu_data_s <- jhu_data_ %>% .[,lapply(.SD,function(x){sum(x,na.rm=T)}),.SDcols=sum_jhu_data,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar)]
			# 			jhu_data_m <- jhu_data_ %>% .[,lapply(.SD,function(x){max(x,na.rm=T)}),.SDcols=max_jhu_data,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar)]
			# 			jhu_data_t <- jhu_data_s %>% merge(jhu_data_m,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar))
			# 			rm(jhu_data_s,jhu_data_m)

			# 			# Vector of time intervals
			# 			cd_tint <- jhu_data_ %>% .[,get(tvar)] %>% unique() %>% .[!is.na(.)]
			# 			all_tint <- ticker[,tvar ] %>% unique() %>% .[.>=min(cd_tint) & .<=max(cd_tint)] %>% .[!is.na(.)]
			# 			cd_tout <- all_tint[!all_tint%in%cd_tint]

			# 			# Fill empty time slots
			# 			if(length(cd_tout)>0){
			# 				jhu_data_t <- data.table::rbindlist(list(jhu_data_t,data.table(LONGITUDE=0,LATITUDE=0,tvar=cd_tout) %>% data.table::setnames("tvar",tvar)),fill=T)
			# 			}

			# 			# Simple overlay
			# 			# Execute only if file doesn't exist
			# 			if(!skip_existing|(skip_existing&(!fnn_out_jhu%in%dir(paste0("Covid19/JHU/Processed/",admz[a0],"_simp/"))))){

			# 	            print(paste0(fnn_out_jhu %>% gsub(".RDS","",.)," -- starting simp"))
							
			# 				# Simple overlay
			# 				# t0_ <- all_tint[3]
			# 				jhu_out <- lapply(all_tint,function(t0_){
			# 					# print(t0_)
			# 					t_mat_0 <- SUNGEO::point2poly_simp(
			# 						polyz=map,
			# 						pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% sf::st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map)),
			# 						# pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% sf::st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(4326)) %>% sf::st_transform(st_crs(map)),
			# 						varz=c(sum_jhu_data,max_jhu_data), na_val=0) %>% 
			# 						as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",tvar) %>% dplyr::select(eval(tvar),names(map) %>% .[!grepl("geometry",.)],everything())
			# 					t_mat_0
			# 				}) %>% dplyr::bind_rows() %>% as.data.table() %>% .[order(get(idvar),get(tvar)),] %>% data.table::setnames(toupper(names(.))) %>% .[,POLYGON_ID := NULL]
			# 				# Save
			# 				saveRDS(jhu_out,file=paste0("Covid19/JHU/Processed/",admz[a0],"_simp/",fnn_out_jhu))
			# 	            print(paste0(fnn_out_jhu %>% gsub(".RDS","",.)," -- finished simp"))


			# 			# Close "skip_existing" if statement
			# 			}

			# 			# Tessellation 
			# 			# Execute only if file doesn't exist
			# 			if(!skip_existing|(skip_existing&(!fnn_out_jhu%in%dir(paste0("Covid19/JHU/Processed/",admz[a0],"_tess/"))))){

			# 	            print(paste0(fnn_out_jhu %>% gsub(".RDS","",.)," -- starting tess"))

			# 				# Tessellation (hide for now)
			# 				# t0_ <- all_tint[2]
			# 				suppressWarnings({
			# 				suppressMessages({
			# 				jhu_tess <- lapply(all_tint,function(t0_){#print(t0_)
			# 					t_mat_0 <- SUNGEO::point2poly_tess(
			# 						polyz=map,
			# 						pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map)),
			# 						poly_id=idvar,
			# 						varz=c(sum_jhu_data,max_jhu_data),
			# 						funz=function(x,w){sum(x*w,na.rm=T)}) %>% 
			# 					as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",tvar) %>% dplyr::select(eval(tvar),names(map) %>% .[!grepl("geometry",.)],everything())
			# 					t_mat_0
			# 				}) %>% dplyr::bind_rows() %>% as.data.table() %>% .[order(get(idvar),get(tvar)),] %>% data.table::setnames(toupper(names(.))) %>% .[,POLYGON_ID := NULL]
			# 				})
			# 				})
			# 				# Save
			# 				saveRDS(jhu_tess,file=paste0("Covid19/JHU/Processed/",admz[a0],"_tess/",fnn_out_jhu))
			# 	            print(paste0(fnn_out_jhu %>% gsub(".RDS","",.)," -- finished tess"))
	          
			# 			# Close "skip_existing" if statement
			# 			}

			# 		# Close if statement [nrow(map)>0]
			# 		}

			#         t2 <- Sys.time()
			#         print(t2-t1)

			#         # Clean workspace
			#         rm(list=ls()[grep("jhu_data_|cnet_|map",ls())])
			#         # gc(reset=T)

			#         print(paste0("finishing ", fnn_out_jhu))

		 #        # Close error catch
		 #        },error=function(e){message(paste0("ERROR!!! ",fnn_out_jhu %>% gsub(".RDS","",.)));message(e)})

			# # Close if statement (file exists)
			# }

			####
			# CNET
			####

			# Output file name
			fnn_out_cnet <- paste0("CNET_",admz_dir[a0_,iso3],"_",admz[a0],admz_dir[a0_,yr],"_",admz_dir[a0_,adm],"_",timz[t0,tunit] %>% toupper(),".RDS")

			# Execute only if raw data exist
			if(file.exists(paste0("Covid19/CoronaNet/Preprocessed/CNET_",admz_dir[a0_,iso3],".RDS"))&(
				(!skip_existing|(skip_existing&(!fnn_out_cnet%in%dir(paste0("Covid19/CoronaNet/Processed/",admz[a0],"_simp/")))))
				|(!skip_existing|(skip_existing&(!fnn_out_cnet%in%dir(paste0("Covid19/CoronaNet/Processed/",admz[a0],"_tess/")))))
				)
				){

				# Error catching
				tryCatch({

			        # Timer
			        t1 <- Sys.time()

			        print(paste0("starting ", fnn_out_cnet))

					# Load data files for country, filter by precision
					cnet_ <- readRDS(paste0("Covid19/CoronaNet/Preprocessed/CNET_",admz_dir[a0_,iso3],".RDS")) %>% as.data.table() %>% .[,DATE:=as.character(DATE)] %>% merge(ticker,by="DATE",all.x=T,all.y=F) %>% .[grepl(adz[sunit%in%admz_dir[a0_,adm],prec],GEO_PRECISION)] #%>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),remove=F,na.fail=F)

					# Variable selection
					sum_cnet <- names(cnet_) %>% grep("NEW$|CHANGE$|STRENGTHEN$|END$",.,value=T)
					max_cnet <- names(cnet_) %>% grep("CNET_NPI|CNET_PI",.,value=T)

		        # Source functions
		        source("../Code/functions.R")

		        # Load polygons
		        suppressWarnings({
	        	suppressMessages({
		        map <- st_read(admz_dir[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% sf::st_buffer(0)
		        })
	        	})

		        # Proceed only if >0 rows
		        if(nrow(cnet_)>0&nrow(map)>0){
	          
						# Unique geo ID
						idvar <- paste0(admz_dir[a0_,adm],"_CODE")
						tvar <- timz[t0,tvar]
	      
						# Create ID if missing
						if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(admz_dir[a0_,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}
	          
            # Aggregate measures for each time interval
						cnet_s <- cnet_ %>% .[,lapply(.SD,function(x){sum(x,na.rm=T)}),.SDcols=sum_cnet,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar)]
						cnet_m <- cnet_ %>% .[,lapply(.SD,function(x){max(x,na.rm=T)}),.SDcols=max_cnet,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar)]
						cnet_t <- cnet_s %>% merge(cnet_m,by=c("GEO_ID","LONGITUDE","LATITUDE",tvar))
						rm(cnet_s,cnet_m)

						# Vector of time intervals
						cd_tint <- cnet_ %>% .[,get(tvar)] %>% unique() %>% .[!is.na(.)]
						all_tint <- ticker[,tvar ] %>% unique() %>% .[.>=min(cd_tint) & .<=max(cd_tint)] %>% .[!is.na(.)]
						cd_tout <- all_tint[!all_tint%in%cd_tint]

						# Fill empty time slots
						if(length(cd_tout)>0){
							cnet_t <- data.table::rbindlist(list(cnet_t,data.table(LONGITUDE=0,LATITUDE=0,tvar=cd_tout) %>% data.table::setnames("tvar",tvar)),fill=T)
						}

						# Simple overlay
						# Execute only if file doesn't exist
						if(!skip_existing|(skip_existing&(!fnn_out_cnet%in%dir(paste0("Covid19/CoronaNet/Processed/",admz[a0],"_simp/"))))){

							print(paste0(fnn_out_cnet %>% gsub(".RDS","",.)," -- starting simp"))
	            
							# Simple overlay
							t0_ <- all_tint[1]
							cnet_out <- lapply(all_tint,function(t0_){
								# print(t0_)
								t_mat_0 <- SUNGEO::point2poly_simp(
									polyz=map,
									pointz=cnet_t %>% .[get(tvar) %in% t0_,] %>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map)),
									varz=c(sum_cnet,max_cnet), na_val=0) %>% 
									as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",tvar) %>% dplyr::select(eval(tvar),names(map) %>% .[!grepl("geometry",.)],everything())
								t_mat_0
							}) %>% dplyr::bind_rows() %>% as.data.table()  %>% .[order(get(idvar),get(tvar)),] %>% data.table::setnames(toupper(names(.))) %>% .[,POLYGON_ID := NULL]
							# Save
							saveRDS(cnet_out,file=paste0("Covid19/CoronaNet/Processed/",admz[a0],"_simp/",fnn_out_cnet))
				            print(paste0(fnn_out_cnet %>% gsub(".RDS","",.)," -- finished simp"))

						# Close "skip_existing" if statement
						}

						# Tessellation 
						# Execute only if file doesn't exist
						if(!skip_existing|(skip_existing&(!fnn_out_cnet%in%dir(paste0("Covid19/CoronaNet/Processed/",admz[a0],"_tess/"))))){

				            print(paste0(fnn_out_cnet %>% gsub(".RDS","",.)," -- starting tess"))

							# Tessellation 
							suppressWarnings({
							suppressMessages({
							cnet_tess <- lapply(all_tint,function(t0_){#print(t0_)
								t_mat_0 <- SUNGEO::point2poly_tess(
									polyz=map,
									pointz=cnet_t %>% .[get(tvar) %in% t0_,] %>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map)),
									poly_id=idvar,
									varz=c(sum_cnet,max_cnet),
									funz=function(x,w){sum(x*w,na.rm=T)}) %>% 
								as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",tvar) %>% dplyr::select(eval(tvar),names(map) %>% .[!grepl("geometry",.)],everything())
								t_mat_0
							}) %>% dplyr::bind_rows() %>% as.data.table() %>% .[order(get(idvar),get(tvar)),] %>% data.table::setnames(toupper(names(.))) %>% .[,POLYGON_ID := NULL]
							})
							})
							# # Save
							saveRDS(cnet_tess,file=paste0("Covid19/CoronaNet/Processed/",admz[a0],"_tess/",fnn_out_cnet))
				            print(paste0(fnn_out_cnet %>% gsub(".RDS","",.)," -- finished tess"))

	          
						# Close "skip_existing" if statement
						}

					# Close if statement [nrow(map)>0]
					}

		        t2 <- Sys.time()
		        print(t2-t1)

		        # Clean workspace
		        rm(list=ls()[grep("jhu_data_|cnet_|map",ls())])
		        # gc(reset=T)

		        print(paste0("finishing ", fnn_out_cnet))

		        # Close error catch
		        },error=function(e){message(paste0("ERROR!!! ",fnn_out_cnet %>% gsub(".RDS","",.)));message(e)})
		
			# Close if statement (file exists)
			}

		# Close timz loop
		})

	# Close mclapply [a0_, admz_dir_]
	},mc.cores = min(nrow(admz_dir), ncores))

  # Close if statement [if nrow(admz_dir_)>0]
  }

# Close mclapply [a0, admz]
})