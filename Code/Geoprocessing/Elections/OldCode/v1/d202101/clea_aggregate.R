# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_aggregate.R")' &
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
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
library(SUNGEO)

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

# List of country-years in CLEA
sub_dirz <- dir("Elections/CLEA/Preprocessed") %>% str_split("_|.RData",simplify=T) %>% .[,2:3] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), f0 = list.files("Elections/CLEA/Preprocessed",full.names=T)) %>% arrange(iso3,yr) %>% as.data.table()
which(sub_dirz$iso3=="USA"&sub_dirz$yr==1918)

# List of boundary sets
dir("Admin")
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS")

###########################
# Loop over country-years
###########################
k0 <- 1795; sub_dirz[k0,]
sub_dirz %>% as.data.frame()

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
cntz_list <- mclapply(1:nrow(sub_dirz),function(k0){

  # Progress
  print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)))

  # Loop over admz
  a0 <- 6; admz[a0]
  for(a0 in length(admz):1){
    print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0]," ",admz[a0],", ",a0,"/",length(admz)))

    # List of boundary files
    admz_dir <- dir(paste0("Admin/",admz[a0],"/Simplified")) %>% str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), yr = yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% arrange(iso3,yr) %>% as.data.table()
    admz_dir_ <- admz_dir[iso3 %in% sub_dirz$iso3[k0],]

    # Proceed if at least one adm file exists
    if(nrow(admz_dir_)>0){

      # Loop over files
      a0_ <- 39; admz_dir_[a0_,]
      for(a0_ in 1:nrow(admz_dir_)){

        # File name
        fnn <- paste0("CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",admz[a0],admz_dir_$yr[a0_],"_",admz_dir_$adm[a0_],".RDS")

        # Execute only if one of the files doesn't exist
        if(!skip_existing|(skip_existing
          &(!fnn%in%dir(paste0("Elections/CLEA/Processed/",admz[a0],"_simp/"))
          |!fnn%in%dir(paste0("Elections/CLEA/Processed/",admz[a0],"_tess/"))
          |!fnn%in%dir(paste0("Elections/CLEA/Processed/",admz[a0],"_krig/"))))){

          # Timer
          t1 <- Sys.time()

          # Source functions
          source("../Code/functions.R")
          ls()

          # Specify variables
          char_varz <- c("cst_n","cst","yr","yrmo","noncontested","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
          mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
          sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

          # Load polygons
          map <- st_read(admz_dir_[a0_,f0], quiet=T) %>% dplyr::mutate_if(is.factor,as.character) %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) 


          # Proceed only if >0 rows
          if(nrow(map)>0){

          # Unique geo ID
          idvar <- paste0(admz_dir_[a0_,adm],"_CODE")

          # Create ID if missing
          if(!idvar%in%names(map)){map$TEMPID <- map %>% as.data.table() %>% .[,paste0(admz_dir_[a0_,adm],"_NAME") %>% get() %>% as.factor() %>% as.numeric()]; names(map)[names(map)=="TEMPID"] <- idvar}

          # Load CLEA
          load(sub_dirz[k0,f0])
          clea_mat

          # Create point layer
          suppressWarnings({
            cst_sf <- suppressMessages(
              st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map)) %>% st_crop(st_bbox(map))
            )
          })


          
            
          ###########################
          # Simple overlay method
          ###########################

          # Error catching
          tryCatch({

          # Skip if file exists
          if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Elections/CLEA/Processed/",admz[a0],"_simp/")))){

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
              dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,4] ) %>%
              dplyr::select(POLYGONS,ADM0_ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
              data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
            clea_out

            # Save 
            saveRDS(clea_out,file=paste0("Elections/CLEA/Processed/",admz[a0],"_simp/",fnn))

            # Visualize
            tryCatch({
                png(paste0("Elections/CLEA/Maps/",admz[a0],"_simp/",fnn %>% gsub(".RDS","_000.png",.)),width=4*1.618,height=4,units="in",res=100)
                par(mar=c(0,0,0,0))
                plot(clea_out$geometry,lwd=.25)
                points(as(cst_sf,"Spatial"))
                dev.off()
              for(plot_var in mean_varz){
                if(clea_out %>% as.data.frame() %>% .[,plot_var %>% toupper()] %>% na.omit() %>% length() > 0){
                  tryCatch({
                  clea_sf <- clea_out %>% st_as_sf()
                  png(paste0("Elections/CLEA/Maps/",admz[a0],"_simp/",fnn %>% gsub(".RDS",paste0("_",plot_var,".png"),.)),width=4*1.618,height=4,units="in",res=100)
                  plot_yz(clea_sf,plot_var %>% toupper())
                  dev.off()
                  },error=function(e){})
                }
              }
            },error=function(e){})

          print(paste0(fnn %>% gsub("CLEA_|.RDS","",.)," -- finished simple overlay"))

          # Close "skip_existing" if statement
          }

          # Clean workspace
          ls()
          rm(list=ls()[grep("clea_out|_char|_means|_sums",ls())])
          gc(reset=T)

          # Close error catch
          },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("CLEA_|.RDS","",.)," simp"));message(e)})

          ###########################
          # Voronoi method
          ###########################

          # Error catching
          tryCatch({

          # Skip if file exists
          if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Elections/CLEA/Processed/",admz[a0],"_tess/")))){

          if(sub_dirz$yr[k0]<1990){
              # Tesselation
              clea_tess_list <- point2poly_tess(
                pointz=cst_sf,
                polyz=map,
                poly_id=idvar,
                methodz=c("aw"),
                varz=list(sum_varz,mean_varz),
                char_varz=char_varz,
                funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
                return_tess=TRUE
                )
              clea_tess_list$result <- clea_tess_list$result %>% bind_cols(clea_tess_list$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_aw$")) %>% dplyr::mutate_if(is.character, list(~NA_character_)) %>% dplyr::mutate_if(is.numeric, list(~NA_real_)) %>% data.table::setnames(names(.),gsub("_aw$","_pw",names(.))) %>% as.data.table())
            }
            if(sub_dirz$yr[k0]>=1990){
              # Unzip population raster
              v_pop <- c(4,3)[c(sub_dirz$yr[k0]>=2000,sub_dirz$yr[k0]<2000)]
              fnam1 <- c("/gpw-v4-population-count-rev11_20","/gl_gpwv3_pcount_")[c(sub_dirz$yr[k0]>=2000,sub_dirz$yr[k0]<2000)]
              fnam2 <- c("_2pt5_min_asc.zip","_ascii_25.zip")[c(sub_dirz$yr[k0]>=2000,sub_dirz$yr[k0]<2000)]
              r_years <- cbind(1990:2024,rep(seq(1990,2020,by=5),each=5))
              con_pop <- unzip(zipfile = paste0("Population/GPW_v",v_pop,fnam1,substr(r_years[match(sub_dirz$yr[k0],r_years[,1]),2],3,4),fnam2), exdir = temp)
              r <- raster(con_pop[grep("ag.asc$|min.asc$",con_pop)])
              # Crop raster by country extent
              b <- as(raster::extent(st_bbox(map)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
              crs(b) <- crs(r)
              b <- st_as_sf(b) 
              r_crop <- crop(r, b)
              # Tesselation        
              clea_tess_list <- point2poly_tess(
                pointz=cst_sf,
                polyz=map,
                poly_id=idvar,
                methodz=c("aw","pw"),
                pop_raster=r_crop,
                varz=list(sum_varz,mean_varz),
                char_varz=char_varz,
                funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
                return_tess=TRUE
                )
            }

            # Rename variables
            geo_vor <- clea_tess_list[["tess"]] %>%
              data.table::setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","CLEA_YEAR","CLEA_YRMO"),skip_absent=TRUE) %>% 
              dplyr::select(ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
              data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
            clea_out <- clea_tess_list[["result"]] %>%
              data.table::setnames(old=c(paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c(paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"CLEA_YEAR","CLEA_YRMO","CLEA_NONCONTESTED"),skip_absent=TRUE) %>% 
              dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,4] ) %>%
              dplyr::select(POLYGONS,ADM0_ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
              dplyr::select(-one_of("yr_pw","yrmo_pw","noncontested_pw")) %>% 
              data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())

            # Save 
            saveRDS(clea_out,file=paste0("Elections/CLEA/Processed/",admz[a0],"_tess/",fnn))

            # Visualize
            tryCatch({
                png(paste0("Elections/CLEA/Maps/",admz[a0],"_tess/",fnn %>% gsub(".RDS","_000_vor.png",.)),width=4*1.618,height=4,units="in",res=100)
                par(mar=c(0,0,0,0))
                plot(geo_vor$geometry,border="blue",lwd=.25)
                plot(map$geometry,border="red",lwd=.25,add=T)
                dev.off()
              for(plot_var in mean_varz){
                if(geo_vor %>% as.data.frame() %>% .[,plot_var %>% toupper()] %>% na.omit() %>% length() > 0){
                  tryCatch({
                  clea_sf <- clea_out %>% st_as_sf()
                  png(paste0("Elections/CLEA/Maps/",admz[a0],"_tess/",fnn %>% gsub(".RDS",paste0("_",plot_var %>% toupper(),"_AW.png"),.)),width=4*1.618,height=4,units="in",res=100)
                  plot_yz(clea_sf,plot_var %>% toupper() %>% paste0(.,"_AW"))
                  dev.off()
                  },error=function(e){})
                }
              }
            },error=function(e){})

          print(paste0(fnn %>% gsub("CLEA_|.RDS","",.)," -- finished tess"))

          # Close "skip_existing" if statement
          }

          # Clean workspace
          rm(list=ls()[grep("clea_out|geo_vor|_char|_means|_sums",ls())])
          gc(reset=T)

          # Close error catch
          },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("CLEA_|.RDS","",.)," tess"));message(e)})


          # ###########################
          # # Kriging
          # ###########################

          # # Error catching
          # tryCatch({

          # # Skip if file exists
          # if(!skip_existing|(skip_existing&!fnn%in%dir(paste0("Elections/CLEA/Processed/",admz[a0],"_krig/")))){

          #   # Ordinary kriging 
          #   suppressMessages({
          #     suppressWarnings({
          #       clea_out <- point2poly_krig(
          #         pointz=cst_sf,
          #         polyz=map %>% st_buffer(0),
          #         varz=mean_varz,
          #         messagez=sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])) %>% as.data.table()
          #     })
          #   })

          #   # Rename variables
          #   clea_out[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
          #   clea_out <- clea_out %>%
          #     data.table::setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","CLEA_YEAR","CLEA_YRMO"),skip_absent=TRUE) %>% 
          #     dplyr::mutate(POLYGONS = fnn %>% str_split("_",simplify=T) %>% .[,4] ) %>%
          #     dplyr::select(POLYGONS,ADM0_ISO3,CLEA_YRMO,CLEA_YEAR,everything()) %>% 
          #     data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
            
          #   # Save 
          #   saveRDS(clea_out,file=paste0("Elections/CLEA/Processed/",admz[a0],"_krig/",fnn))

          #   # Visualize
          #   tryCatch({
          #     for(plot_var in mean_varz){#print(plot_var)
          #       if(clea_out %>% as.data.frame() %>% .[,plot_var %>% toupper() %>% paste0(.,"_KR")] %>% na.omit() %>% length() > 0){
          #         tryCatch({
          #           png(paste0("Elections/CLEA/Maps/",admz[a0],"_krig/",fnn %>% gsub(".RDS",paste0("_",plot_var,".png"),.)),width=4*1.618,height=4,units="in",res=100)
          #           plot_yz(clea_out,paste0(plot_var %>% toupper(),"_KR"))
          #           dev.off()
          #         },error=function(e){})
          #       }
          #     }
          #   },error=function(e){})

          # print(paste0(fnn %>% gsub("CLEA_|.RDS","",.)," -- finished kriging"))

          # # Close "skip_existing" if statement
          # }

          # # Clean workspace
          # rm(list=ls()[grep("clea_out|_char|_means|_sums",ls())])
          # gc(reset=T)

          # # Close error catch
          # },error=function(e){message(paste0("ERROR!!! ",fnn %>% gsub("CLEA_|.RDS","",.)," krig"));message(e)})

        ################################
        # Close country-year loop
        ################################


        # Close if statement (if >0 map rows)
        }

        t2 <- Sys.time()
        print(t2-t1)

      # Close existing
      }

    # Close for loop (a0_, admz_dir)
    }

  # Close if statement (if at least one adm boundaries file)
  }

# Close for loop (a0, admz)
}

# # Close parLapply
# })
# parallel::stopCluster(cl)
# gc()

# Close mclapply
},mc.cores = min(nrow(sub_dirz),ncores))
# gc()

# # Delete temp files
# unlink(temp, recursive = T)
 