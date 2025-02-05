# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_aggregate.R")' &a
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_aggregate.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%"zhubu"){setwd("/media/zhukov/sg1/Dropbox/SUNGEO/Data/")}
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
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages,loaded.packages)

# Install SUNGEO
# install.packages("devtools")
# withr::with_libpaths(new=sungeo_lib,devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE, force=TRUE))
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
sungeo_lib <- grep("R\\/x86",.libPaths(),value=T)
library(SUNGEO,lib.loc=sungeo_lib)
# sessionInfo()

# Log file
log_file <- paste0("../r_log_errors_clea2gred_",Sys.time() %>% gsub("-| |\\:|EDT","",.),".txt") 
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)


###############################################
###############################################
###############################################
## CLEA
###############################################
###############################################
###############################################


# Skip existing files?
skip_existing <- FALSE

# Source custom functions
source("../Code/functions.R")

# Set tempdir
temp <- tempdir()

# List of boundary sets
admz <- c("GRED")

# Full list of files to be processed
sub_dirz <- dir("Elections/CLEA/Preprocessed") %>% stringr::str_split("_|.RDS",simplify=T) %>% .[,2:3] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","clea_yr")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), clea_yr = clea_yr %>% as.character() %>% as.numeric(), clea_f0 = list.files("Elections/CLEA/Preprocessed",full.names=T)) %>% arrange(iso3,clea_yr) %>% as.data.table() %>% merge({lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% stringr::str_split("_",simplify=T) %>% .[,1:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("geoset","iso3","adm_yr","adm")) %>% dplyr::mutate(geoset = geoset %>% as.character(),iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table()}) %>% dplyr::bind_rows()},by="iso3",all.x=T,all.y=F,allow.cartesian=TRUE) %>% .[,fnn_simp := paste0("Elections/CLEA/Processed/",geoset,"_direct/","CLEA_",iso3,"_",clea_yr,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[,exists_simp := fnn_simp %>% file.exists()] %>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F); mean(sub_dirz$exists_simp); 

# Subset
# sub_dirz <- sub_dirz[!exists_simp,]

# Keep only unique map sets
sub_dirz <- sub_dirz %>% unique(by=c("clea_yr","umap"))
nrow(sub_dirz)

# sub_dirz[,fnn_simp]

# sub_dirz[,.(iso3,clea_yr)] %>% unique() %>% as.data.frame()

###########################
# Loop over country-years
###########################

k0 <- 1490; sub_dirz[k0,]
# k0 <- nrow(sub_dirz)
# k0 <- 170145; sub_dirz[k0,]
# sub_dirz %>% as.data.frame()

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

# # PSOCK
# ncores <- min(nrow(sub_dirz),ncores)
# cl <- parallel::makePSOCKcluster(ncores, outfile="")
# parallel::setDefaultCluster(cl)
# parallel::clusterExport(NULL,c("sungeo_lib","skip_existing","skip_poly","sub_dirz","admz","temp","ncores","mem_all","mem0","log_file"),envir = environment())
# parallel::clusterEvalQ(NULL, expr=library(data.table))
# parallel::clusterEvalQ(NULL, expr=library(tidyverse))
# parallel::clusterEvalQ(NULL, expr=library(sf))
# # parallel::clusterEvalQ(NULL, expr=library(V8))
# parallel::clusterEvalQ(NULL, expr=library(stringi))
# parallel::clusterEvalQ(NULL, expr=library(raster))
# parallel::clusterEvalQ(NULL, expr=library(SUNGEO,lib.loc=sungeo_lib))
# cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# # Single core
# cntz_list <- lapply(1:nrow(sub_dirz),function(k0){print(k0)

# Forking
cntz_list <- mclapply(nrow(sub_dirz):1,function(k0){

  # print(paste0("starting k0=",k0))

  # Error catching
  tryCatch({

    # Execute only if one of the files doesn't exist
    if(!skip_existing|(skip_existing
      &(!file.exists(sub_dirz[k0,fnn_simp])
      ))){

      # Source functions
      source("../Code/functions.R")
      ls()

      # Specify variables
      char_varz <- c("cst_n","cst","yr","yrmo","noncontested","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
      mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
      sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

      # Load polygons
      map <- sf::st_read(sub_dirz[k0,adm_f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

      # # Check empty geometries
      # sf::st_is_empty(map) %>% map[.,]
      # sf::st_is_valid(map)

      # Proceed only if >0 rows
      if(nrow(map)>0){

      # Unique geo ID
      idvar <- paste0(sub_dirz[k0,adm],"_CODE")

      # Create ID if missing
      if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(sub_dirz[k0,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

      # Load CLEA
      clea_mat <- readRDS(sub_dirz[k0,clea_f0]) 

      cst_dt <- clea_mat %>% .[,eval(idvar) := cst]
      
      # cst_dt <- clea_mat %>% data.table::setnames(c("iso3c","ctr_n","cst","cst_n","yrmo","yr"),c("ADM0_ISO3","ADM0_NAME","CST_CODE","CST_NAME","YRMO","YEAR"),skip_absent=TRUE)

      # # Create point layer
      # suppressWarnings({
      #   suppressMessages({
      #     cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map), na.fail=FALSE) %>% st_crop(st_bbox(map))
      #   })
      # })

      # Proceed only if >0 rows
      if(nrow(cst_dt)>0){


      ###########################
      # Direct merge
      ###########################

      # Error catching
      tryCatch({

      # Skip if file exists
      if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn_simp]))){

        # Timer
        t1 <- Sys.time()

        # Aggregate over polygons
        clea_chars_0 <- cst_dt[,.SD,.SDcols=char_varz[!char_varz%in%names(map)],by=idvar] 
        clea_means_0 <- cst_dt[,.SD,.SDcols=mean_varz,by=idvar]
        clea_sums_0 <- cst_dt[,.SD,.SDcols=sum_varz,by=idvar]

        # Merge
        clea_out <- map %>% 
          merge(clea_chars_0 %>% as.data.table() %>% dplyr::select(c(idvar,char_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
          merge(clea_means_0 %>% as.data.table() %>% dplyr::select(c(idvar,mean_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
          merge(clea_sums_0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
          data.table::setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","CLEA_YEAR","CLEA_YRMO"),skip_absent=TRUE) %>% 
          dplyr::mutate(POLYGONS = sub_dirz[k0,fnn_simp] %>% str_split("_",simplify=T) %>% .[,4] ) %>%
          dplyr::select(POLYGONS,ADM0_ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
          data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
        clea_out

        # Save 
        saveRDS(clea_out,file=sub_dirz[k0,fnn_simp])

        print(paste0(sub_dirz[k0,fnn_simp] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub(".RDS","",.)," -- finished direct merge :)"))
        t2 <- Sys.time()
        print(t2-t1)

      # Close "skip_existing" if statement
      }

      # Clean workspace
      ls()
      rm(list=ls()[grep("clea_out|_char|_means|_sums",ls())])
      gc(reset=T)

      # Close error catch
      },error=function(e){
        write(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0,fnn_simp] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub("CLEA_|.RDS","",.)," simp"), file = log_file,append=TRUE);
        print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0,fnn_simp] %>% stringr::str_split("/",simplify=T) %>% .[5]  %>% gsub("CLEA_|.RDS","",.)," simp"));
      # message(e)
      })



    ################################
    # Close file loop
    ################################

    # Close if statement (if >0 csf rows)
    } # else {print(paste0("SKIPPED k0=",k0))}

    # Close if statement (if >0 map rows)
    } # else {print(paste0("SKIPPED k0=",k0))}

  # Close existing
  }

  # Close error catch
  },error=function(e){
    write(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, paste(iso3,clea_yr,geoset,adm_yr,adm)]), file = log_file,append=TRUE);
    print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0, paste(iso3,clea_yr,geoset,adm_yr,adm)]));
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

# # Delete temp files
# unlink(temp, recursive = T)
 