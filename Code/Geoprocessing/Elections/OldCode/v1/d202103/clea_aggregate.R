# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_aggregate.R")' &
# tail -f nohup.out
# R
# q("no")
# exit

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
# remove.packages("SUNGEO")
# withr::with_libpaths(new=sungeo_lib,devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE, force=TRUE))
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
sungeo_lib <- grep("R\\/x86",.libPaths(),value=T)
library(SUNGEO,lib.loc=sungeo_lib)
# sessionInfo()

# Log file
log_file <- "r_log_errors.txt"
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)


###############################################
###############################################
###############################################
## CLEA
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

# List of boundary sets
admz <- c("GADM","GAUL","GBHGIS","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","HEXGRID","PRIOGRID")

# Full list of files to be processed
sub_dirz <- dir("Elections/CLEA/Preprocessed") %>% stringr::str_split("_|.RDS",simplify=T) %>% .[,2:3] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","clea_yr")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), clea_yr = clea_yr %>% as.character() %>% as.numeric(), clea_f0 = list.files("Elections/CLEA/Preprocessed",full.names=T)) %>% arrange(iso3,clea_yr) %>% as.data.table() %>% merge({lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% stringr::str_split("_",simplify=T) %>% .[,1:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("geoset","iso3","adm_yr","adm")) %>% dplyr::mutate(geoset = geoset %>% as.character(),iso3 = iso3 %>% as.character() %>% gsub("GB-EAW","GBR",.), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,adm_yr) %>% as.data.table()}) %>% dplyr::bind_rows()},by="iso3",all.x=T,all.y=F,allow.cartesian=TRUE) %>% .[,fnn_simp := paste0("Elections/CLEA/Processed/",geoset,"_simp/","CLEA_",iso3,"_",clea_yr,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[,fnn_tess := paste0("Elections/CLEA/Processed/",geoset,"_tess/","CLEA_",iso3,"_",clea_yr,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[,fnn_krig := paste0("Elections/CLEA/Processed/",geoset,"_krig/","CLEA_",iso3,"_",clea_yr,"_",geoset,adm_yr,"_",adm,".RDS")] %>% .[,exists_simp := fnn_simp %>% file.exists()] %>% .[,exists_tess := fnn_tess %>% file.exists()] %>% .[,exists_krig := fnn_krig %>% file.exists()] %>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F); mean(sub_dirz$exists_simp); mean(sub_dirz$exists_tess); sum(sub_dirz$exists_simp); sum(sub_dirz$exists_tess)

# Subset
sub_dirz <- sub_dirz[!exists_simp|!exists_tess,]
# sub_dirz <- sub_dirz[geoset%in%"GBHGIS"]

# Keep only unique map sets
sub_dirz <- sub_dirz %>% unique(by=c("clea_yr","umap"))
nrow(sub_dirz)

# sub_dirz[,fnn_simp]

# sub_dirz[,.(iso3,clea_yr)] %>% unique() %>% as.data.frame()

###########################
# Loop over country-years
###########################

k0 <- 1501; sub_dirz[k0,]
k0 <- nrow(sub_dirz); sub_dirz[k0,]

# k0 <- sub_dirz[,which(adm_yr==1911&clea_yr==1910)]; sub_dirz[k0,]
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
      |!file.exists(sub_dirz[k0,fnn_tess])
      # |!file.exists(sub_dirz[k0,fnn_krig])
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

      # SHGIS exceptions
      if(sub_dirz[k0,.(iso3=="UKR",geoset=="SHGIS",adm_yr==1945) %>% unlist() %>% all()]){
        map <- sf::st_set_precision(map, 1e5)
      }
      if(sub_dirz[k0,.(iso3=="RUS",geoset=="SHGIS",adm_yr==1937) %>% unlist() %>% all()]){
        map <- sf::st_set_precision(map, 1e5)
      }

      # Proceed only if >0 rows
      if(nrow(map)>0){

      # Unique geo ID
      idvar <- paste0(sub_dirz[k0,adm],"_CODE")

      # Create ID if missing
      if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(sub_dirz[k0,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

      # Load CLEA
      clea_mat <- readRDS(sub_dirz[k0,clea_f0])

      # Create point layer
      suppressWarnings({
        suppressMessages({
          cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map), na.fail=FALSE) %>% st_crop(st_bbox(map))
        })
      })

      # Proceed only if >0 rows
      if(nrow(cst_sf)>0&suppressWarnings({
        suppressMessages({sf::st_intersects(cst_sf,map) %>% unlist() %>% length()})})>0){

      # plot(map["geometry"])
      # plot(cst_sf["geometry"],col="red",add=T)
      # dev.off()
      # sf::st_is_within_distance(cst_sf,map,dist=100000) %>% unlist()
      # sf::st_is_empty(map %>% sf::st_buffer(0))

      ###########################
      # Simple overlay method
      ###########################

      # Error catching
      tryCatch({

      # Skip if file exists
      if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn_simp]))){

        # Timer
        t1 <- Sys.time()

        # Aggregate over polygons
        clea_chars_0 <- point2poly_simp(polyz=map,pointz=cst_sf,varz=char_varz[!char_varz%in%names(map)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
        clea_means_0 <- point2poly_simp(polyz=map,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
        clea_sums_0 <- point2poly_simp(polyz=map,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)

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

        print(paste0(sub_dirz[k0,fnn_simp] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub(".RDS","",.)," -- finished simple overlay :)"))
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

      ###########################
      # Voronoi method
      ###########################

      # Error catching
      tryCatch({

      # Skip if file exists
      if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn_tess]))){

        # Timer
        t1 <- Sys.time()

        if(sub_dirz$clea_yr[k0]<1990){
            # Tesselation
            clea_tess_list <- SUNGEO::point2poly_tess(
              pointz=cst_sf,
              polyz=map,
              poly_id=idvar,
              methodz=c("aw"),
              varz=list(sum_varz,mean_varz),
              pycno_varz=sum_varz,
              char_varz=char_varz,
              funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
              return_tess=TRUE
              )
            clea_tess_list$result <- clea_tess_list$result %>% bind_cols(clea_tess_list$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_aw$")) %>% dplyr::mutate_if(is.character, list(~NA_character_)) %>% dplyr::mutate_if(is.numeric, list(~NA_real_)) %>% data.table::setnames(names(.),gsub("_aw$","_pw",names(.))) %>% as.data.table())
            # # Check pycno
            # sum(clea_tess_list$result$vv1_aw)==sum(cst_sf$vv1)
        }
        if(sub_dirz$clea_yr[k0]>=1990){
          # Unzip population raster
          v_pop <- c(4,3)[c(sub_dirz$clea_yr[k0]>=2000,sub_dirz$clea_yr[k0]<2000)]
          fnam1 <- c("/gpw-v4-population-count-rev11_20","/gl_gpwv3_pcount_")[c(sub_dirz$clea_yr[k0]>=2000,sub_dirz$clea_yr[k0]<2000)]
          fnam2 <- c("_2pt5_min_asc.zip","_ascii_25.zip")[c(sub_dirz$clea_yr[k0]>=2000,sub_dirz$clea_yr[k0]<2000)]
          r_years <- cbind(1990:2024,rep(seq(1990,2020,by=5),each=5))
          con_pop <- unzip(zipfile = paste0("Population/GPW_v",v_pop,fnam1,substr(r_years[match(sub_dirz$clea_yr[k0],r_years[,1]),2],3,4),fnam2), exdir = temp)
          r <- raster(con_pop[grep("ag.asc$|min.asc$",con_pop)])
          # Crop raster by country extent
          b <- as(raster::extent(sf::st_bbox(map)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
          crs(b) <- crs(r)
          b <- sf::st_as_sf(b) 
          r_crop <- raster::crop(r, b)

          # Tesselation        
          clea_tess_list <- point2poly_tess(
            pointz=cst_sf,
            polyz=map %>% sf::st_buffer(0),
            poly_id=idvar,
            methodz=c("aw"),
            varz=list(sum_varz,mean_varz),
            pycno_varz=sum_varz,
            char_varz=char_varz,
            funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
            return_tess=TRUE
            )
          clea_tess_list_pw <- point2poly_tess(
            pointz=cst_sf,
            polyz=map %>% sf::st_buffer(0),
            poly_id=idvar,
            methodz=c("pw"),
            pop_raster=r_crop,
            varz=list(sum_varz,mean_varz),
            pycno_varz=sum_varz,
            char_varz=char_varz,
            funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
            return_tess=TRUE
            )
          clea_tess_list$result <- clea_tess_list$result %>% bind_cols(clea_tess_list_pw$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_pw$"))  %>% as.data.table())
        }


        # Rename variables
        clea_out <- clea_tess_list[["result"]] %>%
          data.table::setnames(old=c(paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr","yrmo","noncontested"),new=c(paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"CLEA_YEAR","CLEA_YRMO","CLEA_NONCONTESTED"),skip_absent=TRUE) %>% 
          dplyr::mutate(POLYGONS = sub_dirz[k0,fnn_tess] %>% str_split("_",simplify=T) %>% .[,4] ) %>%
          dplyr::select(POLYGONS,ADM0_ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
          data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())

        # Save 
        saveRDS(clea_out,file=sub_dirz[k0,fnn_tess])

        print(paste0(sub_dirz[k0,fnn_tess] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub(".RDS","",.)," -- finished tess :)"))
        t2 <- Sys.time()
        print(t2-t1)

      # Close "skip_existing" if statement
      }

      # Clean workspace
      rm(list=ls()[grep("clea_out|geo_vor|_char|_means|_sums",ls())])
      gc(reset=T)

      # Close error catch
      },error=function(e){
        write(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0,fnn_tess] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub("CLEA_|.RDS","",.)," tess"), file = log_file,append=TRUE);
        print(paste0("ERROR!!! k0=",k0," " ,sub_dirz[k0,fnn_tess] %>% stringr::str_split("/",simplify=T) %>% .[5] %>% gsub("CLEA_|.RDS","",.)," tess"));
      # message(e)
      })


      # ###########################
      # # Kriging
      # ###########################

      # # Error catching
      # tryCatch({

      # # Skip if file exists
      # if(!skip_existing|(skip_existing&!file.exists(sub_dirz[k0,fnn_krig]))){

      # # Timer
      # t1 <- Sys.time()

      #   # Ordinary kriging 
      #   suppressMessages({
      #     suppressWarnings({
      #       clea_out <- SUNGEO::point2poly_krige(
      #         pointz=cst_sf,
      #         polyz=map %>% st_buffer(0),
      #         yvarz=mean_varz,
      #         messagez=sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$clea_yr[k0])) %>% as.data.table()
      #     })
      #   })

      #   # Rename variables
      #   clea_out[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      #   clea_out <- clea_out %>%
      #     data.table::setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","CLEA_YEAR","CLEA_YRMO"),skip_absent=TRUE) %>% 
      #     dplyr::mutate(POLYGONS = sub_dirz[k0,fnn_krig] %>% str_split("_",simplify=T) %>% .[,4] ) %>%
      #     dplyr::select(POLYGONS,ADM0_ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
      #     data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
        
      #   # Save 
      #   saveRDS(clea_out,file=sub_dirz[k0,fnn_tess])

      # print(paste0(sub_dirz[k0,fnn_krig] %>% stringr::str_split("/",simplify=T) %>% .[5]  %>% gsub("CLEA_|.RDS","",.)," -- finished kriging :)"))
      # t2 <- Sys.time()
      # print(t2-t1)

      # # Close "skip_existing" if statement
      # }

      # # Clean workspace
      # rm(list=ls()[grep("clea_out|_char|_means|_sums",ls())])
      # gc(reset=T)

      # # Close error catch
      # },error=function(e){print(paste0("ERROR!!! ",sub_dirz[k0,fnn_krig] %>% stringr::str_split("/",simplify=T) %>% .[5]  %>% gsub("CLEA_|.RDS","",.)," krig"));
      # # message(e)
      # })

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
 