# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/EventData/Geoprocessing/xsub_panel_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/EventData/Geoprocessing/xsub_panel_aggregate.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
sungeo_lib <- grep("R\\/x86",.libPaths(),value=T)
library(SUNGEO,lib.loc=sungeo_lib)

# sessionInfo()

# Log file
log_file <- paste0("../r_log_errors_xsub_",Sys.time() %>% gsub("-| |\\:|EDT","",.),".txt") 
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)
file.exists(log_file)


# old_packages <- installed.packages(lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.6/")
# head(old_packages[, 1])
# new_packages <- installed.packages()
# missing_packages <- as.data.frame(old_packages[!old_packages[,"Package"] %in% new_packages[,"Package"],])
# install.packages(missing_packages$Package,dependencies=TRUE)

###############################################
###############################################
###############################################
## xSub panel
###############################################
###############################################
###############################################

# Skip existing files?
skip_existing <- TRUE

# Set tempdir
temp <- tempdir()

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
sub_dirz <- dir("EventData/xSub/Events") %>% stringr::str_split("_|.RDS",simplify=T) %>% .[,1:2] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("source","iso3")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), source = source %>% as.character() , xsub_f0 = list.files("EventData/xSub/Events",full.names=T)) %>% dplyr::arrange(source,iso3) %>% as.data.table() %>% .[!grepl("MELTT|GTD|ITERATE|MIPT",source),] %>% merge({lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% stringr::str_split("_",simplify=T) %>% .[,1:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("geoset","iso3","adm_yr","adm")) %>% dplyr::mutate(geoset = geoset %>% as.character(),iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table()}) %>% dplyr::bind_rows()},by="iso3",all.x=T,all.y=F,allow.cartesian=TRUE) %>% .[,fnn_year := paste0("EventData/xSub/Processed/",geoset,"/","xSub_",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_YEAR.RDS")] %>% .[,fnn_month := paste0("EventData/xSub/Processed/",geoset,"/","xSub_",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_MONTH.RDS")] %>% .[,fnn_week := paste0("EventData/xSub/Processed/",geoset,"/","xSub_",source,"_",iso3,"_",geoset,adm_yr,"_",adm,"_WEEK.RDS")] %>% .[,exists_year := fnn_year %>% file.exists()] %>% .[,exists_month := fnn_month %>% file.exists()] %>% .[,exists_week := fnn_week %>% file.exists()] %>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F); 
mean(sub_dirz$exists_year); mean(sub_dirz$exists_month); mean(sub_dirz$exists_week); sum(sub_dirz$exists_year); sum(sub_dirz$exists_month); sum(sub_dirz$exists_week)

# Subset
# sub_dirz <- sub_dirz[!exists_year|!exists_month|!exists_week,]

# Date fix
sub_dirz <- sub_dirz[paste0(source,"_",iso3)%in%c("ESOCIraqSIGACT_IRQ","ESOCMexicoHomicide_MEX","NIRI_GBR","NIRI_IRL","PITF_COL","PITF_DZA","PITF_UGA","yzChechnya_RUS")]
# # Delete processed files
# filez2go <- sub_dirz[,fnn_year %>% c(.,fnn_month) %>% c(.,fnn_week)]
# for(i0 in seq_along(filez2go)){file.remove(filez2go[i0])}; rm(filez2go)

# Subset specific sources
# sub_dirz <- sub_dirz[grepl("COCACW|ESOCM",source),]

# Keep only unique map sets
sub_dirz <- sub_dirz %>% .[!duplicated(umap)]

# Import time id matrix
source("../Code/functions.R")
ticker <- make_ticker()
head(ticker)
# ticker <- ticker[as.character(ticker$DATE)>=range(as.character(events$DATE),na.rm=T)[1]&as.character(ticker$DATE)<=range(as.character(events$DATE),na.rm=T)[2],]

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
# Loop over country-years
###########################

k0 <- 34; sub_dirz[k0,]
sub_dirz[,grep("ESOC",source)][10]
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
cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# nrow(sub_dirz)

# # Forking
# cntz_list <- mclapply(1:nrow(sub_dirz),function(k0){

  # Error catching
  tryCatch({

    # # Progress
    # print(paste0("starting k0=",k0," " ,sub_dirz[k0, paste(source,iso3,geoset,adm_yr,adm)]))
    
    # Load xSub, convert to dt
    load(sub_dirz[k0,xsub_f0])
    xsub_data <- indata %>% as.data.table() %>% {if(.[,all(nchar(DATE)==4&DATE>=ticker[,min(YEAR)]&DATE<=ticker[,max(YEAR)])]) .[,DATE := paste0(DATE,"0101") %>% as.numeric()] else .} %>% dplyr::filter((!is.na(LONG))&(!is.na(LAT))) %>% dplyr::mutate_if(is.factor, list(~ as.character(.))) %>% merge(ticker,by="DATE",all.x=T,all.y=F) %>% as.data.table() ; rm(indata)

    # Loop over time units
    t0 <- 1
    tunit_list <- lapply(1:nrow(timz),function(t0){

      # Execute only if one of the files doesn't exist
      if(!skip_existing|(skip_existing
        &(!file.exists(sub_dirz[k0,get(paste0("fnn_",timz[t0,tunit]))])
        ))){

        # Timer
        t1 <- Sys.time()

        # Source functions
        source("../Code/functions.R")

        # Variable selection
        sum_varz <- names(xsub_data) %>% (function(.){.[grep("^INITIATOR|^TARGET|^DYAD|^SIDE|^ACTION_ANY",.)]})

        # Load polygons
        map <- st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

        # Proceed only if >0 rows
        if(nrow(map)>0&nrow(xsub_data[grepl(adz[sunit%in%sub_dirz[k0,adm],prec],GEOPRECISION)&grepl(timz[t0,prec],TIMEPRECISION),])>0){

          # Create sf layer
          xsub_sf <- st_as_sf(xsub_data[grepl(adz[sunit%in%sub_dirz[k0,adm],prec],GEOPRECISION)&grepl(timz[t0,prec],TIMEPRECISION),],coords = c("LONG", "LAT"),crs=st_crs(map)) %>% dplyr::mutate(TUNIT = timz[t0,tvar] %>% get())
          
          # Unique geo ID
          idvar <- paste0(sub_dirz[k0,adm],"_CODE")
          if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(sub_dirz[k0,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

          # Vector of time intervals
          xsub_tint <- xsub_data[,timz[t0,tvar] %>% get()] %>% unique()
          all_tint <- ticker[,timz[t0,tvar] %>% get()] %>% unique() %>% .[.>=min(xsub_tint,na.rm=TRUE) & .<=max(xsub_tint,na.rm=TRUE)]
          # xsub_tout <- all_tint[!all_tint%in%xsub_tint]

          # Event counts each time interval
          # t0_ <- 1993
          xsub_out <- lapply(all_tint,function(t0_){#print(t0_)
            xsub_t0 <- xsub_sf[xsub_sf$TUNIT %in% t0_,]
            if(nrow(xsub_t0)>0){
              t_mat_0 <- point2poly_simp(polyz=map,
                  pointz=xsub_t0,
                  varz=sum_varz, na_val=0) %>% as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",timz[t0,tvar]) %>% dplyr::select(timz[t0,tvar],names(map) %>% .[!grepl("geometry",.)],everything())
            }
            if(nrow(xsub_t0)==0){
              xsub_t0 <- st_as_sf(xsub_data[1,] %>% .[,c("LONG","LAT") := .(0,0)],coords = c("LONG", "LAT"),crs=st_crs(map)) %>% dplyr::mutate(TUNIT = timz[t0,tvar] %>% get())
              t_mat_0 <- point2poly_simp(polyz=map,
                  pointz=xsub_t0,
                  varz=sum_varz, na_val=0) %>% as.data.table() %>% dplyr::select(-geometry) %>% dplyr::mutate(TUNIT=t0_) %>% data.table::setnames("TUNIT",timz[t0,tvar]) %>% dplyr::select(timz[t0,tvar],names(map) %>% .[!grepl("geometry",.)],everything())
            }
            t_mat_0
          }) %>% dplyr::bind_rows() %>% merge(y=ticker %>% .[!duplicated(timz[t0,tvar] %>% get())],by=timz[t0,tvar],all.x=T,all.y=F) %>% dplyr::select(names(ticker),everything()) %>% dplyr::arrange(timz[t0,tvar] %>% get(),idvar %>% get()) %>% as.data.table()
          xsub_out

          # Save 
          saveRDS(xsub_out,file=sub_dirz[k0,get(paste0("fnn_",timz[t0,tunit]))])

          print(paste0(sub_dirz[k0,get(paste0("fnn_",timz[t0,tunit]))] %>% stringr::str_split("\\/") %>% sapply(last) %>% gsub(".RDS","",.)," -- finished :)"))

        # Close if statement [nrow(map)>0]
        }

        t2 <- Sys.time()
        print(t2-t1)

      # Close "skip_existing" if statement
      }

      # Clean workspace
      rm(list=ls()[grep("xsub_out|xsub_sf|xsub_tint|all_tint",ls())])
      # gc(reset=T)


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

# # Delete temp files
# unlink(temp, recursive = T)
