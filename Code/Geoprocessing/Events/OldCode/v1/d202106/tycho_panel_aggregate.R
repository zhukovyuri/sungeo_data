# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/EventData/Geoprocessing/tycho_panel_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/EventData/Geoprocessing/tycho_panel_aggregate.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
library(SUNGEO)

###############################################
###############################################
###############################################
## Tycho panel
###############################################
###############################################
###############################################

# List of countries to skip
skip_poly <- NULL

# Skip existing files?
skip_existing <- TRUE

# Set tempdir
temp <- tempdir()

# List of countries in Tycho
sub_dirz <- list.files("../../SUNGEO_Flu/Data/RawData/USA/Tycho\ Data", pattern="geo\\.RDS", full.names=T) %>% data.table(f0 = ., source = "Tycho", iso3 = "USA") %>% .[,.(source,iso3,f0)] 
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
# ticker <- ticker[as.character(ticker$DATE)>=range(as.character(events$DATE),na.rm=T)[1]&as.character(ticker$DATE)<=range(as.character(events$DATE),na.rm=T)[2],] 
ticker <- ticker %>% as.data.table()
head(ticker); rm(ticker2,datez,datez2)

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")

# Units and precision levels
adz <- data.frame(
  sunit = c("ADM0","ADM1","ADM2","CST"),
  prec = c("adm0|adm1|adm2|adm3","adm1|adm2|adm3","adm2|adm3","adm1|adm2|adm3"),
  stringsAsFactors = F) %>% as.data.table()
timz <- data.frame(
  tunit = c("year","month","week"),
  tvar = c("YEAR","YRMO","WID"),
  prec = c("year|month|week|day","month|week|day","week|day"),
  stringsAsFactors=F) %>% as.data.table()

###########################
# Loop over country-years
###########################
k0 <- 1; sub_dirz[k0,]
# sub_dirz %>% as.data.frame()

# # PSOCK
# ncores <- min(nrow(sub_dirz),detectCores()/1.5)
# cl <- parallel::makePSOCKcluster(ncores, outfile="")
# parallel::setDefaultCluster(cl)
# parallel::clusterExport(NULL,c("skip_existing","skip_poly","sub_dirz","admz","temp","ticker","datez","adz","timz"),envir = environment())
# parallel::clusterEvalQ(NULL, expr=library(data.table))
# parallel::clusterEvalQ(NULL, expr=library(tidyverse))
# parallel::clusterEvalQ(NULL, expr=library(sf))
# parallel::clusterEvalQ(NULL, expr=library(V8))
# parallel::clusterEvalQ(NULL, expr=library(stringi))
# parallel::clusterEvalQ(NULL, expr=library(raster))
# parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
# cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# # Forking
# cntz_list <- mclapply(1:nrow(sub_dirz),function(k0){

# Progress
# print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)))

# Load Tycho, convert to dt
tycho_data <- readRDS(sub_dirz[k0,f0]) %>% .[(!is.na(longitude))&(!is.na(latitude)),] %>% dplyr::mutate_if(is.factor, list(~ as.character(.))) %>% as.data.table()   %>% data.table::setnames(c("CountValue","Fatalities","geo_precision"),c("CASES","DEATHS","GEOPRECISION")) %>% .[,TIMEPRECISION := "week"] %>% .[,.(osm_id,WID,GEOPRECISION,TIMEPRECISION,ConditionName,CASES,DEATHS,longitude,latitude)] 

# Collapse Tycho
tycho_geo <- tycho_data[,.(osm_id,WID,longitude,latitude)] %>% unique() 
condz <- tycho_data[,unique(ConditionName)] %>% data.table(ConditionName=.) %>% .[,CABB:= ConditionName %>% abbreviate() %>% toupper()]
for(co_ in 1:nrow(condz)){
  tycho_geo <- tycho_geo %>% merge(tycho_data[ConditionName%in%condz[co_,ConditionName],lapply(.SD,function(x){sum(x,na.rm=T)}),.SDcols=c("CASES","DEATHS"),by=c("osm_id","WID")] %>% data.table::setnames(c("CASES","DEATHS"),c("CASES","DEATHS") %>% paste0(condz[co_,CABB],"_",.)),by=c("osm_id","WID"),all.x=T,all.y=F)
}
tycho_data <- tycho_geo[,lapply(.SD,function(x){tidyr::replace_na(x,0)})] %>% .[,c("GEOPRECISION","TIMEPRECISION") := .("adm3","week") ] %>% merge(ticker[!duplicated(WID),],by="WID",all.x=T,all.y=F); rm(tycho_geo)

# Loop over admz
a0 <- 1; admz[a0]
for(a0 in length(admz):1){

  print(paste0(sub_dirz$source[k0]," ",sub_dirz$iso3[k0]," ",admz[a0],", ",a0,"/",length(admz)))

  # List of boundary files
  admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table()
  admz_dir_ <- admz_dir[iso3 %in% sub_dirz$iso3[k0],]

  # Proceed if at least one adm file exists
  if(nrow(admz_dir_)>0){

    # Number of cores
    mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
    mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
    ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

    # Loop over files
    a0_ <- 2; admz_dir_[a0_,]
    mclapply(1:nrow(admz_dir_),function(a0_){
    # for(a0_ in 1:nrow(admz_dir_)){
   
      # Loop over time units
      t0 <- 1
      tunit_list <- lapply(1:nrow(timz),function(t0){

        # Error catching
        tryCatch({

        # File name
        fnn <- paste0("Tycho_",sub_dirz$source[k0],"_",sub_dirz$iso3[k0],"_",admz[a0],admz_dir_$yr[a0_],"_",admz_dir_$adm[a0_],"_",timz[t0,tunit] %>% toupper(),".RDS")

        # Execute only if one of the files doesn't exist
        if(!skip_existing|(skip_existing&(!fnn%in%dir(paste0("EventData/Tycho/Processed/Tycho_Panel/",admz[a0],"_simp/"))))){

          print(paste0(fnn %>% gsub("Tycho_|.RDS","",.)," -- started simple overlay"))

          # Timer
          t1 <- Sys.time()

          # Source functions
          source("../Code/functions.R")

          # Variable selection
          sum_varz <- names(tycho_data) %>% (function(.){.[grep("CASES$|DEATHS$",.)]})

          # Load polygons
          map <- st_read(admz_dir_[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

          # Proceed only if >0 rows
          if(nrow(map)>0&nrow(tycho_data[grepl(adz[sunit%in%admz_dir_[a0_,adm],prec],GEOPRECISION)&grepl(timz[t0,prec],TIMEPRECISION),])>0){

            # Create sf layer
            tycho_sf <- st_as_sf(tycho_data[grepl(adz[sunit%in%admz_dir_[a0_,adm],prec],GEOPRECISION)&grepl(timz[t0,prec],TIMEPRECISION),],coords = c("longitude", "latitude"),crs=st_crs(map)) %>% dplyr::mutate(TUNIT = timz[t0,tvar] %>% get())
            
            # Unique geo ID
            idvar <- paste0(admz_dir_[a0_,adm],"_CODE")
            
            # Vector of time intervals
            tycho_tint <- tycho_data[,timz[t0,tvar] %>% get()] %>% unique()
            all_tint <- ticker[,timz[t0,tvar] %>% get()] %>% unique() %>% .[.>=min(tycho_tint) & .<=max(tycho_tint)]
            # tycho_tout <- all_tint[!all_tint%in%tycho_tint]

            # Event counts each time interval
            t0_ <- 1919
            tycho_out <- lapply(all_tint,function(t0_){#print(t0_)
              t_mat_0 <- point2poly_simp(polyz=map,
                  pointz=tycho_sf[tycho_sf$TUNIT %in% t0_,],
                  varz=sum_varz, na_val=0) %>% as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",timz[t0,tvar]) %>% dplyr::select(timz[t0,tvar],names(map) %>% .[!grepl("geometry",.)],everything())
              t_mat_0
            }) %>% dplyr::bind_rows() %>% merge(y=ticker %>% .[!duplicated(timz[t0,tvar] %>% get())],by=timz[t0,tvar],all.x=T,all.y=F) %>% dplyr::select(names(ticker),everything()) %>% dplyr::arrange(timz[t0,tvar] %>% get(),idvar %>% get()) %>% as.data.table()
            tycho_out

            # Save 
            saveRDS(tycho_out,file=paste0("EventData/Tycho/Processed/Tycho_Panel/",admz[a0],"_simp/",fnn))

            print(paste0(fnn %>% gsub("Tycho_|.RDS","",.)," -- finished simple overlay"))

          # Close if statement [nrow(map)>0]
          }

          t2 <- Sys.time()
          print(t2-t1)

        # Close "skip_existing" if statement
        }

        # Clean workspace
        rm(list=ls()[grep("tycho_out|tycho_sf|tycho_tint|all_tint",ls())])
        # gc(reset=T)

        # Close error catch
        },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("Tycho_|.RDS","",.)," simp"));message(e)})

      # Close timz loop
      })

    # Close for loop [a0_, admz_dir_]
    # }
    },mc.cores=ncores)

  # Close if statement [if nrow(admz_dir_)>0]
  }

# Close for loop [a0, admz]
}

# # Close parLapply
# })
# parallel::stopCluster(cl)
# gc()

# # Close mclapply
# },mc.cores = min(nrow(sub_dirz),detectCores()-2))
# # gc()

# # Delete temp files
# unlink(temp, recursive = T)
