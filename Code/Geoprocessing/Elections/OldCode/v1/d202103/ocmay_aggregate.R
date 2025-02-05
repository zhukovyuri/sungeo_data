# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/ocmay_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/ocmay_aggregate.R")' &
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
# install.packages("devtools")
# library(devtools)
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
library(SUNGEO)

###############################################
###############################################
###############################################
## OurCampaigns Mayoral
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

# List of elections
oc <- readRDS("../../SUNGEO_Flu/Data/RawData/USA/OurCampaigns/may_raw.RDS") %>% as.data.table() 
sub_dirz <- dir("Elections/OurCampaigns/Preprocessed/Mayoral") %>% str_split("_|.RDS",simplify=T) %>% .[,2:3] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("RaceID","yr")) %>% dplyr::mutate(RaceID = RaceID %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), f0 = list.files("Elections/OurCampaigns/Preprocessed/Mayoral",full.names=T)) %>% as.data.table() %>% .[,iso3 := "USA"] %>% .[,.(iso3,RaceID,yr,f0)] %>% arrange(iso3,RaceID,yr) %>% as.data.table() %>% .[, state := match(RaceID,oc$RaceID) %>% oc[.,State]] %>% .[, city := match(RaceID,oc$RaceID) %>% oc[.,gsub(unique(State) %>% paste0(", ",.,"| \\(",.,"\\)| \\[",.,"\\]| ",.,collapse="|"),"",City)]]
rm(oc)

# State-years
syr_dirz <- sub_dirz[,.(state,yr)] %>% unique()

which(syr_dirz$state=="NY"&syr_dirz$yr==1909)

# List of boundary sets
dir("Admin")
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")
admz <- c("GRED")


###########################
# Loop over state-years
###########################

k0 <- 4; syr_dirz[k0,]
sub_dirz[state%in%syr_dirz[k0,state]&yr%in%syr_dirz[k0,yr],]
# sub_dirz %>% as.data.frame()

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

# Forking (mc)
cntz_list <- mclapply(1:nrow(syr_dirz),function(k0){

# Error catching
tryCatch({

  # Progress
  # print(paste0(syr_dirz$state[k0]," ",syr_dirz$yr[k0],", ",k0,"/",nrow(syr_dirz)," (",round(k0/nrow(syr_dirz),3)*100,"%)"))  

  # sub_dirz[state%in%syr_dirz[k0,state]&yr%in%syr_dirz[k0,yr],]

  # Loop over admz
  a0 <- 1; admz[a0]
  for(a0 in length(admz):1){#print(paste0("a0=",a0))
  # for(a0 in length(admz)){#print(paste0("a0=",a0))
    
    # print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$RaceID[k0]," ",sub_dirz$yr[k0]," ",admz[a0],", ",a0,"/",length(admz)))

    # List of boundary files
    admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table() %>% .[adm!="ADM0",]
    admz_dir_ <- admz_dir[iso3 %in% sub_dirz[state%in%syr_dirz[k0,state]&yr%in%syr_dirz[k0,yr],iso3],]
    admz_dir_
    # admz_dir_ <- admz_dir_[grep("NHGIS_USA_1910",f0),] # Subset just 1910

    # Proceed if at least one adm file exists
    if(nrow(admz_dir_)>0){

      # Loop over files
      a0_ <- 17; admz_dir_[a0_,]
      for(a0_ in 1:nrow(admz_dir_)){#print(paste0("a0_=",a0_))

        # File name
        fnn <- paste0("ocmay_",sub_dirz[state%in%syr_dirz[k0,state]&yr%in%syr_dirz[k0,yr],paste0(iso3,"_",state,"_",yr)],"_",admz[a0],admz_dir_$yr[a0_],"_",admz_dir_$adm[a0_],".RDS") %>% .[1]

        # Execute only if one of the files doesn't exist
        if(!skip_existing|(skip_existing
          &(!fnn%in%dir(paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_simp/"))
          |!fnn%in%dir(paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_tess/"))
          # |!fnn%in%dir(paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_krig/"))
          ))){

          # # Timer
          # t1 <- Sys.time()

          # Source functions
          source("../Code/functions.R")

          # Specify variables
          char_varz <- c("osm_id","election_id","city","state","year","yrmo","noncontested","incumb_ptyname","incumb_pty","incumb_can","win1_ptyname","win1_pty","win1_can","win2_ptyname","win2_pty","win2_can")
          mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","cvs1_incumb","pvs1_incumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
          sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

          # Load polygons
          map <- st_read(admz_dir_[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 

          # Proceed only if >0 rows
          if(nrow(map)>0){

            # Unique geo ID
            idvar <- paste0(admz_dir_[a0_,adm],"_CODE")

            # Create ID if missing
            if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(admz_dir_[a0_,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

            # Load oc (combine if multiple files)
            oc_mat <- lapply(sub_dirz[state%in%syr_dirz[k0,state]&yr%in%syr_dirz[k0,yr],f0],readRDS) %>% dplyr::bind_rows() %>% .[vv1>1,] %>% .[,city := gsub(unique(state) %>% paste0(", ",.,"| \\(",.,"\\)| \\[",.,"\\]| ",.,collapse="|"),"",city) %>% stringr::str_trim()]

            # oc_mat <- readRDS(sub_dirz[k0,f0])
            oc_mat[,c("cv1_margin","pv1_margin","v1_margin"):=.(votes_margin)]

            # # Add missing geocodes (WTF????)
            # if(all(is.na(oc_mat$longitude))){
            #   oc_mat[,query := paste0(sname,", ",state)]
            #   oc_geo <- list.files("../../SUNGEO_Flu/Data/RawData/USA/OurCampaigns",pattern="\\.RDS",full.names=T) %>% .[grepl("_geo.RDS",.)] %>% readRDS(.) %>% as.data.table() %>%  .[,query := paste0(sname,", ",State)] %>% .[!duplicated(query),]
            #   oc_mat[,c("longitude","latitude") := match(query,oc_geo$query) %>% oc_geo[.,.(longitude,latitude)]]
            #   oc_mat[,query := NULL]
            #   rm(oc_geo)
            # }

            # Subset map (US-specific)
            if("CST_NAME"%in%names(map)){map$ADM1_NAME <- map$CST_NAME %>% gsub(" [0-9]+","",.) %>% str_to_title()}
            map$STATE_ABB <- map %>% as.data.table() %>% .[,ADM1_NAME %>% gsub(" Territory","",.) %>% setNames(state.abb, state.name)[.]] 
            map[grep("District",map$ADM1_NAME),"STATE_ABB"] <- "DC"
            map <- map[map$STATE_ABB%in%oc_mat[,unique(state)],]
            # plot(map["geometry"])

            # Drop overlapping geometries
            suppressWarnings({
              suppressMessages({
                map <- sf::st_within(map,map) %>% unlist() %>% table() %>% .[.==1] %>% names() %>% as.numeric() %>% map[.,]
              })
            })

            # Proceed if more than 0 rows
            if(nrow(map)>0&nrow(oc_mat[!is.na(longitude),])>0){

              # Create point layer
              suppressWarnings({
                cst_sf <- suppressMessages(
                  st_as_sf(oc_mat[!is.na(longitude),],coords = c("longitude", "latitude"),crs=st_crs(map)) %>% st_crop(st_bbox(map))
                )
              })

              # plot(cst_sf["geometry"])
              # plot(map["geometry"],add=T)

              # # Missing variables
              # if(mean(is.na(cst_sf$win2_ptyname))==1){cst_sf$win2_ptyname <- ""}
              # if(mean(is.na(cst_sf$win2_pty))==1){cst_sf$win2_pty <- ""}
              # if(mean(is.na(cst_sf$win2_can))==1){cst_sf$win2_can <- ""}
 
              ###########################
              # Simple overlay method
              ###########################

              # print("start simp")

              # Error catching
              tryCatch({

              # Skip if file exists
              if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_simp/")))){

                # print(paste0(fnn %>% gsub("ocmay_|.RDS","",.)," -- started simple overlay"))

                # Timer
                t1 <- Sys.time()

                # Aggregate over polygons
                oc_chars_0 <- point2poly_simp(polyz=map,pointz=cst_sf,varz=char_varz[!char_varz%in%names(map)],char_varz=char_varz[!char_varz%in%names(map)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
                oc_means_0 <- point2poly_simp(polyz=map,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
                oc_sums_0 <- point2poly_simp(polyz=map,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)
                # oc_means_0 %>% as.data.frame()

                # Merge
                oc_out <- map %>% 
                  merge(oc_chars_0 %>% as.data.table() %>% dplyr::select(c(idvar,char_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
                  merge(oc_means_0 %>% as.data.table() %>% dplyr::select(c(idvar,mean_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
                  merge(oc_sums_0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
                  data.table::setnames(old=c("election_id","osm_id","city","state","year","yrmo"),new=c("OC_ID","OC_OSMID","OC_CITY_N","OC_ADM1_N","OC_YEAR","OC_YRMO"),skip_absent=TRUE) %>% 
                  dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,5] ) %>%
                  dplyr::select(POLYGONS,ADM0_ISO3,OC_YRMO,OC_YEAR,everything()) %>% 
                  data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
                oc_out

                # Save 
                saveRDS(oc_out,file=paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_simp/",fnn))

                # # Visualize
                # tryCatch({
                #     png(paste0("Elections/OurCampaigns/Maps/Mayoral/",admz[a0],"_simp/",fnn %>% gsub(".RDS","_000.png",.)),width=4*1.618,height=4,units="in",res=100)
                #     par(mar=c(0,0,0,0))
                #     plot(oc_out$geometry,lwd=.25)
                #     points(as(cst_sf,"Spatial"))
                #     dev.off()
                #   for(plot_var in mean_varz){
                #     if(oc_out %>% as.data.frame() %>% .[,plot_var %>% toupper()] %>% na.omit() %>% length() > 0){
                #       tryCatch({
                #       oc_sf <- oc_out %>% st_as_sf()
                #       png(paste0("Elections/OurCampaigns/Maps/Mayoral/",admz[a0],"_simp/",fnn %>% gsub(".RDS",paste0("_",plot_var,".png"),.)),width=4*1.618,height=4,units="in",res=100)
                #       plot_yz(oc_sf,plot_var %>% toupper())
                #       dev.off()
                #       },error=function(e){})
                #     }
                #   }
                # },error=function(e){})

                print(paste0(fnn %>% gsub("ocmay_|.RDS","",.)," -- finished simple overlay"))

                t2 <- Sys.time()
                print(t2-t1)

              # Close "skip_existing" if statement
              }

              # Clean workspace
              ls()
              rm(list=ls()[grep("oc_out|_char|_means|_sums",ls())])
              gc(reset=T)

              # Close error catch
              },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("ocmay_|.RDS","",.)," simp"));message(e)})

              ###########################
              # Voronoi method
              ###########################

              # print("start tess")

              # Error catching
              tryCatch({

              # Skip if file exists
              if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_tess/")))){

              # print(paste0(fnn %>% gsub("ocmay_|.RDS","",.)," -- started tess"))

              # Timer
              t1 <- Sys.time()

              if(syr_dirz[k0,yr]<1990){
                  # Tesselation
                  oc_tess_list <- point2poly_tess(
                    pointz=cst_sf,
                    polyz=map %>% sf::st_buffer(0),
                    poly_id=idvar,
                    methodz=c("aw"),
                    varz=list(sum_varz,mean_varz),
                    char_varz=char_varz,
                    funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
                    return_tess=TRUE
                    )
                  oc_tess_list$result <- oc_tess_list$result %>% bind_cols(oc_tess_list$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_aw$")) %>% dplyr::mutate_if(is.character, list(~NA_character_)) %>% dplyr::mutate_if(is.numeric, list(~NA_real_)) %>% data.table::setnames(names(.),gsub("_aw$","_pw",names(.))) %>% as.data.table())
                }
                if(syr_dirz[k0,yr]>=1990){
                  # Unzip population raster
                  v_pop <- c(4,3)[c(syr_dirz$yr[k0]>=2000,syr_dirz$yr[k0]<2000)]
                  fnam1 <- c("/gpw-v4-population-count-rev11_20","/gl_gpwv3_pcount_")[c(syr_dirz$yr[k0]>=2000,syr_dirz$yr[k0]<2000)]
                  fnam2 <- c("_2pt5_min_asc.zip","_ascii_25.zip")[c(syr_dirz$yr[k0]>=2000,syr_dirz$yr[k0]<2000)]
                  r_years <- cbind(1990:2024,rep(seq(1990,2020,by=5),each=5))
                  con_pop <- unzip(zipfile = paste0("Population/GPW_v",v_pop,fnam1,substr(r_years[match(syr_dirz$yr[k0],r_years[,1]),2],3,4),fnam2), exdir = temp)
                  r <- raster(con_pop[grep("ag.asc$|min.asc$",con_pop)])
                  # Crop raster by country extent
                  b <- as(raster::extent(st_bbox(map)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
                  crs(b) <- crs(r)
                  b <- st_as_sf(b) 
                  r_crop <- crop(r, b)
                  oc_tess_list <- point2poly_tess(
                    pointz=cst_sf,
                    polyz=map %>% sf::st_buffer(0),
                    poly_id=idvar,
                    methodz=c("aw"),
                    varz=list(sum_varz,mean_varz),
                    char_varz=char_varz,
                    funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
                    return_tess=TRUE
                    )
                  oc_tess_list_pw <- point2poly_tess(
                    pointz=cst_sf,
                    polyz=map %>% sf::st_buffer(0),
                    poly_id=idvar,
                    methodz=c("pw"),
                    pop_raster=r_crop,
                    varz=list(sum_varz,mean_varz),
                    char_varz=char_varz,
                    funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
                    return_tess=TRUE
                    )
                  oc_tess_list$result <- oc_tess_list$result %>% bind_cols(oc_tess_list_pw$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_pw$"))  %>% as.data.table())
                }

                # Rename variables
                # geo_vor <- oc_tess_list[["tess"]] %>%
                #   data.table::setnames(old=c("election_id","osm_id","city","state","year","yrmo"),new=c("OC_ID","OC_OSMID","OC_CITY_N","OC_ADM1_N","OC_YEAR","OC_YRMO"),skip_absent=TRUE) %>% 
                #   dplyr::select(OC_YRMO,OC_YEAR,everything()) %>% 
                #   data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
                oc_out <- oc_tess_list[["result"]] %>%
                  data.table::setnames(old=c(paste0(rep(c("election_id","osm_id","city","state"),each=2),c("_aw","_pw")),"year","yrmo","noncontested_aw"),new=c(paste0(rep(c("OC_ID","OC_OSMID","OC_CITY_N","OC_ADM1_N"),each=2),c("_AW","_PW")),"OC_YEAR","OC_YRMO","OC_NONCONTESTED"),skip_absent=TRUE) %>% 
                  dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,4] ) %>%
                  dplyr::select(POLYGONS,OC_YRMO,OC_YEAR,everything()) %>% 
                  data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
                # plot(oc_out["CONTEST_P1_AW"])

                # Save 
                saveRDS(oc_out,file=paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_tess/",fnn))

                # # Visualize
                # tryCatch({
                #     png(paste0("Elections/OurCampaigns/Maps/Mayoral/",admz[a0],"_tess/",fnn %>% gsub(".RDS","_000_vor.png",.)),width=4*1.618,height=4,units="in",res=100)
                #     par(mar=c(0,0,0,0))
                #     plot(geo_vor$geometry,border="blue",lwd=.25)
                #     plot(map$geometry,border="red",lwd=.25,add=T)
                #     dev.off()
                #   for(plot_var in mean_varz){
                #     if(geo_vor %>% as.data.frame() %>% .[,plot_var %>% toupper()] %>% na.omit() %>% length() > 0){
                #       tryCatch({
                #       oc_sf <- oc_out %>% st_as_sf()
                #       png(paste0("Elections/OurCampaigns/Maps/Mayoral/",admz[a0],"_tess/",fnn %>% gsub(".RDS",paste0("_",plot_var %>% toupper(),"_AW.png"),.)),width=4*1.618,height=4,units="in",res=100)
                #       plot_yz(oc_sf,plot_var %>% toupper() %>% paste0(.,"_AW"))
                #       dev.off()
                #       },error=function(e){})
                #     }
                #   }
                # },error=function(e){})

                print(paste0(fnn %>% gsub("ocmay_|.RDS","",.)," -- finished tess"))

                t2 <- Sys.time()
                print(t2-t1)

              # Close "skip_existing" if statement
              }

              # Clean workspace
              rm(list=ls()[grep("oc_out|geo_vor|_char|_means|_sums",ls())])
              gc(reset=T)

              # Close error catch
              },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("CLEA_|.RDS","",.)," tess"));message(e)})


              # ###########################
              # # Kriging
              # ###########################

              # # Error catching
              # tryCatch({

              # # Skip if file exists
              # if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_krig/")))){

              #   # Ordinary kriging 
              #   suppressMessages({
              #     suppressWarnings({
              #       oc_out <- point2poly_krig(
              #         pointz=cst_sf,
              #         polyz=map %>% st_buffer(0),
              #         varz=mean_varz,
              #         messagez=sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$adm1[k0],"_",sub_dirz$yr[k0])) %>% as.data.table()
              #     })
              #   })

              #   # Rename variables
              #   oc_out[,c("year","yrmo"):=list(cst_sf$year[1],cst_sf$yrmo[1])]
              #   oc_out <- oc_out %>%
              #     data.table::setnames(old=c("sid","sname","state","year","yrmo"),new=c("OC_SID","OC_SID_N","OC_ADM1_N","OC_YEAR","OC_YRMO"),skip_absent=TRUE) %>% 
              #     dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,4] ) %>%
              #     dplyr::select(POLYGONS,ADM0_ISO3,OC_YRMO,OC_YEAR,everything()) %>% 
              #     data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
                
              #   # Save 
              #   saveRDS(oc_out,file=paste0("Elections/OurCampaigns/Processed/Mayoral/",admz[a0],"_krig/",fnn))

              #   # Visualize
              #   tryCatch({
              #     for(plot_var in mean_varz){#print(plot_var)
              #       if(oc_out %>% as.data.frame() %>% .[,plot_var %>% toupper() %>% paste0(.,"_KR")] %>% na.omit() %>% length() > 0){
              #         tryCatch({
              #           png(paste0("Elections/OurCampaigns/Maps/Mayoral/",admz[a0],"_krig/",fnn %>% gsub(".RDS",paste0("_",plot_var,".png"),.)),width=4*1.618,height=4,units="in",res=100)
              #           plot_yz(oc_out,paste0(plot_var %>% toupper(),"_KR"))
              #           dev.off()
              #         },error=function(e){})
              #       }
              #     }
              #   },error=function(e){})

              # print(paste0(fnn %>% gsub("ocmay_|.RDS","",.)," -- finished kriging"))

              # # Close "skip_existing" if statement
              # }

              # # Clean workspace
              # rm(list=ls()[grep("oc_out|_char|_means|_sums",ls())])
              # gc(reset=T)

              # # Close error catch
              # },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("ocmay_|.RDS","",.)," krig"));message(e)})

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

    # Close for loop (a0_, admz_dir)
    }

  # Close if statement (if at least one adm boundaries file)
  }

# Close for loop (a0, admz)
}


# Close error catch
},error=function(e){message(paste0("ERROR!!! k0=",k0," " ,syr_dirz$state[k0]," ",syr_dirz$yr[k0]));message(e)})

# # Close parLapply
# })
# parallel::stopCluster(cl)
# gc()

# Close mclapply
},mc.cores = min(nrow(syr_dirz),ncores))
gc()

# # Delete temp files
# unlink(temp, recursive = T)
 