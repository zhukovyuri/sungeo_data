# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/EventData/Geoprocessing/xsub_clea_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/EventData/Geoprocessing/xsub_clea_aggregate.R")' &
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
## xSub x CLEA
###############################################
###############################################
###############################################

# List of countries to skip
skip_poly <- NULL

# Skip existing files?
skip_existing <- TRUE

# Source custom functions
source("../Code/functions.R")

# Set tempdir
temp <- tempdir()


# List of country-years in CLEA
clea_dirz <- dir("Elections/CLEA/Preprocessed") %>% str_split("_|.RData",simplify=T) %>% .[,2:3] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), f0 = list.files("Elections/CLEA/Preprocessed",full.names=T)) %>% arrange(iso3,yr) %>% as.data.table()
clea_dirz

# List of countries in xSub
sub_dirz <- list.files("../../XSub/Data/Upload/data_rdata_event", full.names=T) %>% .[!grepl("/MELTT|/MIPT|/GTD|/ITERATE",.)] %>% data.frame(f0 = .,stringsAsFactors=F) %>% dplyr::mutate(source = f0 %>% str_split("_", simplify = T) %>% .[,3] %>% as.character() %>% gsub("event/","",.),iso3 = f0 %>% str_split("_", simplify = T) %>% .[,4] %>% as.character()) %>% dplyr::select(source,iso3,f0) %>% as.data.table()

# Dates
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors = FALSE) %>% as.data.table()

# Keep only xSub-CLEA overlap
sub_dirz <- sub_dirz[iso3%in%clea_dirz$iso3,]

# List of boundary sets
dir("Admin")
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")

###########################
# Loop over country-years
###########################
k0 <- 102; sub_dirz[k0,]
# sub_dirz %>% as.data.frame()

# # PSOCK
# ncores <- min(nrow(sub_dirz),detectCores()/1.5)
# cl <- parallel::makePSOCKcluster(ncores, outfile="")
# parallel::setDefaultCluster(cl)
# parallel::clusterExport(NULL,c("skip_existing","skip_poly","sub_dirz","clea_dirz","admz","temp"),envir = environment())
# parallel::clusterEvalQ(NULL, expr=library(data.table))
# parallel::clusterEvalQ(NULL, expr=library(tidyverse))
# parallel::clusterEvalQ(NULL, expr=library(sf))
# parallel::clusterEvalQ(NULL, expr=library(V8))
# parallel::clusterEvalQ(NULL, expr=library(stringi))
# parallel::clusterEvalQ(NULL, expr=library(raster))
# parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
# cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# Forking
cntz_list <- mclapply(1:nrow(sub_dirz),function(k0){

  # Progress
  print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)))

  # Load xSub, convert to dt, filter by precision
  load(sub_dirz[k0,f0])
  xsub_data <- indata %>% filter((!is.na(LONG))&(!is.na(LAT))&(TIMEPRECISION%in%c("day"))) %>% dplyr::mutate_if(is.factor, list(~ as.character(.))) %>% as.data.table() %>% as.data.table(); rm(indata)

  # Extract years (keep only overlap with CLEA)  
  yrz <- xsub_data$DATE %>% substr(1,4) %>% unique() %>% sort() %>% intersect(clea_dirz[iso3%in%sub_dirz[k0,iso3]][,yr])

  # Run if not empty set
  if(length(yrz)>0){

  # Loop over years
  y0 <- 1
  year_list <- lapply(1:length(yrz),function(y0){

    # Loop over admz
    a0 <- 1; admz[a0]
    for(a0 in 1:length(admz)){
      print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0]," ",admz[a0],", ",a0,"/",length(admz)))

      # List of boundary files
      admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table()
      admz_dir_ <- admz_dir[iso3 %in% sub_dirz$iso3[k0],]

      # Proceed if at least one adm file exists
      if(nrow(admz_dir_)>0){

        # Loop over files
        a0_ <- 2; admz_dir_[a0_,]
        for(a0_ in 1:nrow(admz_dir_)){

          # Find months of election
          load(clea_dirz[iso3%in%sub_dirz[k0,iso3]&yr%in%yrz[y0],f0])
          yrmoz <- clea_mat$yrmo %>% unique()

          # Loop over months
          ym0 <- 1
          yrmo_list <- lapply(1:length(yrmoz),function(ym0){

            # Error catching
            tryCatch({

            # File name
            fnn <- paste0("xSub_",sub_dirz$source[k0],"_",sub_dirz$iso3[k0],"_",yrmoz[ym0],"_",admz[a0],admz_dir_$yr[a0_],"_",admz_dir_$adm[a0_],".RDS")

            # Execute only if one of the files doesn't exist
            if(!skip_existing|(skip_existing&(!fnn%in%dir(paste0("EventData/xSub/Processed/xSub_CLEA/",admz[a0],"_simp/"))))){

              # Timer
              t1 <- Sys.time()

              # Source functions
              source("../Code/functions.R")

              # Variable selection
              sum_varz <- names(xsub_data) %>% (function(.){.[grep("^INITIATOR|^TARGET|^DYAD|^SIDE|^ACTION_ANY",.)]})

               # Window selection
              windowz <- c(30,60,90)

              # Load polygons
              map <- st_read(admz_dir_[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

              # Proceed only if >0 rows
              if(nrow(map)>0){

                # Create sf layer
                xsub_sf <- st_as_sf(xsub_data,coords = c("LONG", "LAT"),crs=st_crs(map))

                # Unique geo ID
                idvar <- paste0(admz_dir_[a0_,adm],"_CODE")
                
                # Vector of dates on month of election
                wind_t <- ticker[YRMO%in%yrmoz[ym0],DATE]

                # Run if non-empty set
                if(sum(xsub_sf$DATE %in% c(wind_t))>0){

                  # Event counts during month of election
                  t_mat_0 <- point2poly_simp(polyz=map,
                      pointz=xsub_sf[xsub_sf$DATE%in%wind_t,],
                      varz=sum_varz, na_val=0)

                  # Loop over windows
                  wind_list <- lapply(windowz,function(w0){
                    # Extract dates
                    wind_post <- ticker[YRMO%in%yrmoz[ym0],((max(TID)+1):(max(TID)+w0))] %>% (function(.){ticker[TID%in%.,DATE]})
                    wind_pre <- ticker[YRMO%in%yrmoz,((min(TID)-1):(min(TID)-w0))] %>% (function(.){ticker[TID%in%.,DATE]})
                    # Point-in-poly
                    pre_mat_0 <- point2poly_simp(polyz=map,
                        pointz=xsub_sf[xsub_sf$DATE%in%wind_pre,],
                        varz=sum_varz, na_val=0)
                    post_mat_0 <- point2poly_simp(polyz=map,
                        pointz=xsub_sf[xsub_sf$DATE%in%wind_post,],
                        varz=sum_varz, na_val=0)
                    list(pre=pre_mat_0,post=post_mat_0)
                  })

                  # Combine
                  wind_mat_0 <- merge(t_mat_0,wind_list[[1]]$pre %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[1])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[1]]$post %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[1])),by=idvar,all.x=T,all.y=F) 
                  if(length(windowz)>1){
                    for(w0_ in 2:length(windowz)){
                    wind_mat_0 <- merge(wind_mat_0,wind_list[[w0_]]$pre %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[w0_])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[w0_]]$post %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[w0_])),by=idvar,all.x=T,all.y=F)
                    }
                  }

                  # Re-order, re-name columns
                  xsub_out <- wind_mat_0 %>% as.data.table() %>% .[,c("SOURCE","ISO3","YEAR","YRMO") := list(xsub_data$SOURCE[1], sub_dirz[k0,iso3], yrz[y0],yrmoz[ym0])] %>% dplyr::select(SOURCE,ISO3,YRMO,YEAR,names(map) %>% .[!grepl("^geometry",.)],everything()) 
                  xsub_out

                  # Save 
                  saveRDS(xsub_out,file=paste0("EventData/xSub/Processed/xSub_CLEA/",admz[a0],"_simp/",fnn))

                  # Visualize
                  tryCatch({
                    plot_var <- "ACTION_ANY"
                    xsub_sf_ <- xsub_out %>% st_as_sf()
                    png(paste0("EventData/xSub/Maps/xSub_CLEA/",admz[a0],"_simp/",fnn %>% gsub(".RDS",paste0("_pre",max(windowz),".png"),.)),width=4*1.618,height=4,units="in",res=150)
                    plot_yz(xsub_sf_,paste0(plot_var,"_pre",max(windowz)),plot_title=paste0(yrmoz[ym0]," ",plot_var,"_pre",max(windowz)),breaks_style="pretty")
                    dev.off()
                  },error=function(e){})

                  print(paste0(fnn %>% gsub("xSub_|.RDS","",.)," -- finished simple overlay"))

                # Close if statement [non-empty wind_t]
                }

              # Close if statement [nrow(map)>0]
              }

              t2 <- Sys.time()
              print(t2-t1)

            # Close "skip_existing" if statement
            }

            # Clean workspace
            rm(list=ls()[grep("xsub_out|xsub_sf_|wind_|t_mat",ls())])
            # gc(reset=T)

            # Close error catch
            },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("xSub_|.RDS","",.)," simp"));message(e)})

          # Close yrmo loop
          })

        # Close for loop [a0_, admz_dir_]
        }

      # Close if statement [if nrow(admz_dir)>0]
      }

    # Close for loop [a0, admz]
    }

  # Close lapply [y0, yrz]
  })

  # Close if statement [at least one yrz]
  }

# # Close parLapply
# })
# parallel::stopCluster(cl)
# gc()

# Close mclapply
},mc.cores = min(nrow(sub_dirz),detectCores()-2))
# gc()

# # Delete temp files
# unlink(temp, recursive = T)
