# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/Population/Geoprocessing/emort_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/Population/Geoprocessing/emort_aggregate.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","SUNGEO")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# install.packages("SUNGEO",dependencies=T)
# install.packages("devtools")
# library(devtools)
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEOdev_0.1.0.tar.gz", repo = NULL, type="source")


###############################################
###############################################
###############################################
## Excess mortality
###############################################
###############################################
###############################################

# List of countries to skip
skip_poly <- NULL

# Skip existing files?
skip_existing <- FALSE

# Source custom functions
# source("../Code/functions.R")

# Set tempdir
temp <- tempdir()

# Load data
ipums_exc <- readRDS("../../SUNGEO_Flu/Data/RawData/USA/ExcessMortality/ipums_1918.RDS") %>% data.table::setnames(grep("^MORT",names(.),value=T),paste0("I_",grep("^MORT",names(.),value=T)))
bailey_exc <- readRDS("../../SUNGEO_Flu/Data/RawData/USA/ExcessMortality/bailey_1918.RDS") %>% data.table::setnames(grep("^MORT",names(.),value=T),paste0("B_",grep("^MORT",names(.),value=T)))

# List of boundary sets
dir("Admin")
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")

###########################
# Loop over country-years
###########################


# # PSOCK
# ncores <- min(nrow(sub_dirz),detectCores()/1.5)
# cl <- parallel::makePSOCKcluster(ncores, outfile="")
# parallel::setDefaultCluster(cl)
# parallel::clusterExport(NULL,c("skip_existing","skip_poly","sub_dirz","admz","temp"),envir = environment())
# parallel::clusterEvalQ(NULL, expr=library(data.table))
# parallel::clusterEvalQ(NULL, expr=library(tidyverse))
# parallel::clusterEvalQ(NULL, expr=library(sf))
# parallel::clusterEvalQ(NULL, expr=library(V8))
# parallel::clusterEvalQ(NULL, expr=library(stringi))
# parallel::clusterEvalQ(NULL, expr=library(raster))
# parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
# cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# Number of cores
mem0 <- sapply(ls(),function(x){object.size(get(x))/1000}) %>% sum()
mem_all <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE)) 
ncores <- min(floor(mem_all/mem0),parallel::detectCores()-1)

# Forking
# cntz_list <- mclapply(nrow(sub_dirz):1,function(k0){

# # Progress
# print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$adm1[k0]," ",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)))

# Loop over admz
a0 <- 6; admz[a0]
for(a0 in length(admz):1){#print(paste0("a0=",a0))
# for(a0 in length(admz)){#print(paste0("a0=",a0))
print(paste0("USA ",admz[a0],", ",a0,"/",length(admz)))

# List of boundary files
admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table() %>% .[adm!="ADM0",]
admz_dir_ <- admz_dir[iso3 %in% "USA",]
# admz_dir_ <- admz_dir_[grep("NHGIS_USA_1910",f0),] # Subset just 1910

# Proceed if at least one adm file exists
if(nrow(admz_dir_)>0){


  # Loop over files
  a0_ <- 3; admz_dir_[a0_,]
  admz_list <- mclapply(1:nrow(admz_dir_),function(a0_){
  	# for(a0_ in 1:nrow(admz_dir_)){
  	print(paste0("a0_= ",a0_,"/",nrow(admz_dir_)))

    # File name
    fnn <- paste0("emort_","USA","_",admz[a0],admz_dir_$yr[a0_],"_",admz_dir_$adm[a0_],".RDS")

    # Execute only if one of the files doesn't exist
    dir("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/")
    if(!skip_existing|(skip_existing
      &(!fnn%in%dir(paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_simp/"))
      |!fnn%in%dir(paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_tess/"))
      |!fnn%in%dir(paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_krig/"))))){

      # # Timer
      # t1 <- Sys.time()

      # Source functions
      source("../Code/functions.R")
      ls()

      # Specify variables
      sum_varz_b <- mean_varz_b <- grep("\\d{4}E",names(bailey_exc),value=T)
      sum_varz_i <- mean_varz_i <- grep("\\d{4}E",names(ipums_exc),value=T)
      sum_varz_b <- grep("\\d{4}E$",sum_varz_b,value=T)
      sum_varz_i <- grep("\\d{4}E$",sum_varz_i,value=T)

      # Load polygons
      map <- st_read(admz_dir_[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

      # Proceed only if >0 rows
      if(nrow(map)>0){

        # Unique geo ID
        idvar <- paste0(admz_dir_[a0_,adm],"_CODE")

        # Create ID if missing
        if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(admz_dir_[a0_,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

        # Subset map (US-specific)
        if("CST_NAME"%in%names(map)){map$ADM1_NAME <- map$CST_NAME %>% gsub(" [0-9]+","",.) %>% str_to_title()}
        map$STATE_ABB <- map %>% as.data.table() %>% .[,ADM1_NAME %>% gsub(" Territory","",.) %>% setNames(state.abb, state.name)[.]] 
        map[grep("District",map$ADM1_NAME),"STATE_ABB"] <- "DC"
        # map <- map[map$STATE_ABB%in%oc_mat[,unique(state)],]
        # plot(map["geometry"])

        # Proceed if more than 0 rows
        if(nrow(map)>0){

          # Create point layers
          suppressWarnings({
            bailey_sf <- suppressMessages(
              st_as_sf(bailey_exc,coords = c("longitude", "latitude"),crs=st_crs(map)) %>% st_crop(st_bbox(map))
            )
            ipums_sf <- suppressMessages(
              st_as_sf(ipums_exc,coords = c("longitude", "latitude"),crs=st_crs(map)) %>% st_crop(st_bbox(map))
            )
          })

          ###########################
          # Simple overlay method
          ###########################

          # print("start simp")

          # Error catching
          tryCatch({

          # Skip if file exists
          if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_simp/")))){

            print(paste0(fnn %>% gsub("emort_|.RDS","",.)," -- started simple overlay"))

            # Timer
            t1 <- Sys.time()

            # Aggregate over polygons
            emort_means_i <- point2poly_simp(polyz=map,pointz=ipums_sf,varz=mean_varz_i,funz=function(x){mean(x,na.rm=T)},na_val=NA) %>% data.table::setnames(mean_varz_i,paste0(mean_varz_i,"_MEAN"))
			emort_means_b <- point2poly_simp(polyz=map,pointz=bailey_sf,varz=mean_varz_b,funz=function(x){mean(x,na.rm=T)},na_val=NA) %>% data.table::setnames(mean_varz_b,paste0(mean_varz_b,"_MEAN"))
            emort_sums_i <- point2poly_simp(polyz=map,pointz=ipums_sf,varz=sum_varz_i,funz=function(x){sum(x,na.rm=T)},na_val=NA) %>% data.table::setnames(sum_varz_i,paste0(sum_varz_i,"_SUM"))
			emort_sums_b <- point2poly_simp(polyz=map,pointz=bailey_sf,varz=sum_varz_b,funz=function(x){sum(x,na.rm=T)},na_val=NA) %>% data.table::setnames(sum_varz_b,paste0(sum_varz_b,"_SUM"))

            # Merge
            emort_out <- map %>% 
              merge(emort_means_i %>% as.data.table() %>% dplyr::select(c(idvar,paste0(mean_varz_i,"_MEAN")) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
              merge(emort_means_b %>% as.data.table() %>% dplyr::select(c(idvar,paste0(mean_varz_b,"_MEAN")) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
              merge(emort_sums_i %>% as.data.table() %>% dplyr::select(c(idvar,paste0(sum_varz_i,"_SUM")) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
              merge(emort_sums_b %>% as.data.table() %>% dplyr::select(c(idvar,paste0(sum_varz_b,"_SUM")) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
              dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,3] ) %>%
              dplyr::select(POLYGONS,ADM0_ISO3,everything()) %>% 
              data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% data.table::setnames(names(.),gsub("PCT_MEAN","PCT",names(.))) %>% data.table::setnames(names(.),gsub("SD_MEAN","SD",names(.)))
            emort_out

            # Save 
            saveRDS(emort_out,file=paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_simp/",fnn))

            
            print(paste0(fnn %>% gsub("emort_|.RDS","",.)," -- finished simple overlay"))

            t2 <- Sys.time()
            print(t2-t1)

          # Close "skip_existing" if statement
          }

          # Clean workspace
          ls()
          rm(list=ls()[grep("emort_out|_char|_means|_sums",ls())])
          gc(reset=T)

          # Close error catch
          },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("ocgub_|.RDS","",.)," simp"));message(e)})

          ###########################
          # Voronoi method
          ###########################

          # print("start tess")

          # Error catching
          tryCatch({

          # Skip if file exists
          if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_tess/")))){

          print(paste0(fnn %>% gsub("emort_|.RDS","",.)," -- started tess"))

          # Timer
          t1 <- Sys.time()

          # Tesselation
          emort_tess_i <- point2poly_tess(
            pointz=ipums_sf,
            polyz=map,
            poly_id=idvar,
            methodz=c("aw"),
            varz=list(sum_varz_i,mean_varz_i),
            funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
            return_tess=FALSE
            ) %>% as.data.table() %>% data.table::setnames(names(.),gsub("aw\\.x","aw_SUM",names(.))) %>% data.table::setnames(names(.),gsub("aw\\.y|aw$","aw_MEAN",names(.))) %>% sf::st_as_sf()
          emort_tess_b <- point2poly_tess(
            pointz=bailey_sf,
            polyz=map,
            poly_id=idvar,
            methodz=c("aw"),
            varz=list(sum_varz_b,mean_varz_b),
            funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
            return_tess=FALSE
            ) %>% as.data.table() %>% data.table::setnames(names(.),gsub("aw\\.x","aw_SUM",names(.))) %>% data.table::setnames(names(.),gsub("aw\\.y|aw$","aw_MEAN",names(.))) %>% sf::st_as_sf()
          
            # Rename variables
			emort_out <- map %>% 
              merge(emort_tess_i %>% as.data.table() %>% dplyr::select(c(idvar,paste0(mean_varz_i,"_aw_MEAN"),paste0(sum_varz_i,"_aw_SUM")) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
              merge(emort_tess_b %>% as.data.table() %>% dplyr::select(c(idvar,paste0(mean_varz_b,"_aw_MEAN"),paste0(sum_varz_b,"_aw_SUM")) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
              as.data.table() %>%
              dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,3] ) %>%
              dplyr::select(POLYGONS,ADM0_ISO3,everything()) %>% 
              data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% data.table::setnames(names(.),gsub("PCT_AW_MEAN","AW_PCT",names(.))) %>% data.table::setnames(names(.),gsub("SD_AW_MEAN","AW_SD",names(.)))
            emort_out
            
            # Save 
            saveRDS(emort_out,file=paste0("../../SUNGEO_Flu/Data/ProcessedData/USA/ExcessMortality/",admz[a0],"_tess/",fnn))

            print(paste0(fnn %>% gsub("emort_|.RDS","",.)," -- finished tess"))

            t2 <- Sys.time()
            print(t2-t1)

          # Close "skip_existing" if statement
          }

          # Clean workspace
          rm(list=ls()[grep("emort_out|_char|_means|_sums",ls())])
          gc(reset=T)

          # Close error catch
          },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("CLEA_|.RDS","",.)," tess"));message(e)})



        ################################
        # Close country-year loop
        ################################

      # t2 <- Sys.time()
      # print(t2-t1)

      # Close if statement (if >0 oc_mat rows)
      }

    # Close if statement (if >0 map rows)
    }

    # t2 <- Sys.time()
    # print(t2-t1)

  # Close existing
  }

# # Close for loop (a0_, admz_dir)
# }
# Close mclapply
},mc.cores = min(nrow(admz_dir_)/2,ncores/2))


# Close if statement (if at least one adm boundaries file)
}

# Close for loop (a0, admz)
}

# # Close parLapply
# })
# parallel::stopCluster(cl)
# gc()

# # Close mclapply
# },mc.cores = min(nrow(sub_dirz),ncores))
# # gc()

# # Delete temp files
# unlink(temp, recursive = T)
 