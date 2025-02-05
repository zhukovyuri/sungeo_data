# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/gred_aggregate_2.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/gred_aggregate_2.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%"ubu"){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%"zhukov"){setwd("~/Dropbox/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
# list.of.packages <- c("sf",
#                       "maptools",
#                       "parallel",
#                       "countrycode",
#                       "rgdal",
#                       "data.table",
#                       "deldir",
#                       "rgeos",
#                       "lwgeom",
#                       "raster",
#                       "tidyverse",
#                       "rmapshaper",
#                       "Rfast",
#                       "automap")
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# library(devtools)
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
library(SUNGEO)



###############################################
###############################################
###############################################
## GRED
###############################################
###############################################
###############################################

# List of countries with broken geometries
fix_gred <- c("SWE")

# List of countries to skip
skip_gred <- c("ZZZ")

# List of countries to simplify polygons
simp_gred <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

# Skip existing files?
skip_existing <- FALSE

# Custom functions
source("../Code/functions.R")

# Set tempdir
temp <- tempdir()

# Load GRED polygons
filez <- dir("Admin/GRED/")[grep(".zip$",dir("Admin/GRED/"))]
vz <- filez %>% str_extract("(\\d)+")
temp <- tempdir()
f0 <- 1
con <- unzip(zipfile = paste0("Admin/GRED/",filez[f0]), exdir = temp)
sub_dirz <- data.frame(
  f1 = temp,
  f2 = sapply(strsplit(con,"\\.|/"),"[",4),
  f3 = sapply(strsplit(con,"\\.|/"),"[",5),
  f4 = sapply(strsplit(con,"\\.|/"),"[",6),
  stringsAsFactors = FALSE
) %>% distinct(f4,.keep_all = TRUE) %>% drop_na(f4) %>% as.data.table()
sub_dirz[,ctr_n := f4 %>% strsplit("_") %>% sapply("[",3)]
sub_dirz[,iso3 := ctr_n %>% countrycode("country.name","iso3c")]
sub_dirz[,yr := f4 %>% strsplit("_") %>% sapply("[",4)]

# Subset
sub_dirz <- sub_dirz[yr>=1990,]
sub_filez <- sub_dirz$f4

# Load CLEA data
load("Elections/CLEA/clea_lc_20190617/clea_lc_20190617.rdata")
clea <- clea_lc_20190617 %>% as.data.table(); rm(clea_lc_20190617)
# Fix character encoding issue (convert all to ASCII)
clea <- clea %>% dplyr::mutate_if(sapply(clea,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% as.data.table()
# clea$cst_n %>% stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")
# Add country codes
clea[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
clea[ctr_n%in%"Micronesia",ISO3 := "FSM"]
clea[ctr_n%in%"Kosovo",ISO3 := "XKX"]
# Subset >1990
clea_full <- clea
save(clea_full,file=paste0(temp,"/clea_full.RData"))
rm(clea_full)
clea <- clea[yr>=1990,]

# Keep only overlap
clea <- clea[paste0(ISO3,"_",yr) %in% paste0(sub_dirz$iso3,"_",sub_dirz$yr),]
clea[,.(ISO3,yr)] %>% unique()
sub_dirz[,.(iso3,yr)]

# Load coordinates
load("Elections/CLEA/GeoCode/cst_best_3.RData")
# Fix character encoding issue (convert all to ASCII)
geo_mat <- geo_mat %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% as.data.table()
# Add country codes
geo_mat[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat

# Load Natural Earth boundaries
con_ne <- unzip(zipfile = "Admin/NE/ne_10m_admin_0_countries.zip", exdir = temp)

# Load simple world map
data(wrld_simpl)

# Loop over years
y0 <- 5
yrz <- unique(clea$yr) %>% sort() %>% intersect(1990:2014)
for(y0 in 1:length(yrz)){
  print(yrz[y0])

  # Subset CLEA by year
  clea_yr <- clea[yr%in%yrz[y0],]
  clea_cntz <- clea_yr[,ISO3] %>% unique() %>% sort()

  # Exceptions
  clea_cntz <- clea_cntz[!clea_cntz%in%skip_gred]

  # Unzip population raster
  v_pop <- c(4,3)[c(yrz[y0]>=2000,yrz[y0]<2000)]
  fnam1 <- c("/gpw-v4-population-count-rev11_20","/gl_gpwv3_pcount_")[c(yrz[y0]>=2000,yrz[y0]<2000)]
  fnam2 <- c("_2pt5_min_asc.zip","_ascii_25.zip")[c(yrz[y0]>=2000,yrz[y0]<2000)]
  r_years <- cbind(1990:2024,rep(seq(1990,2020,by=5),each=5))
  con_pop <- unzip(zipfile = paste0("Population/GPW_v",v_pop,fnam1,substr(r_years[match(yrz[y0],r_years[,1]),2],3,4),fnam2), exdir = temp)
  r <- raster(con_pop[grep("ag.asc$|min.asc$",con_pop)])

  # Save country files to temp folder
  load(paste0(temp,"/clea_full.RData"))

  # # Forking
  # save_list <- mclapply(seq_along(clea_cntz),function(k0){
  #   # cat(round(k0/length(clea_cntz),2),"\r")
  #   clea_ <- clea_full[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]
  #   save(clea_,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
  # },mc.cores=min(length(clea_cntz),detectCores()-1))
    
  # PSOCK
  ncores <- min(length(clea_cntz),detectCores()-1)
  cl <- parallel::makePSOCKcluster(ncores)
  parallel::setDefaultCluster(cl)
  parallel::clusterExport(NULL,c("clea_cntz","clea_full","yrz","temp","y0"),envir = environment())
  parallel::clusterEvalQ(NULL, expr=library(data.table))
  # parallel::clusterEvalQ(NULL, expr=library(dplyr)) 
  save_list <- parLapply(NULL,seq_along(clea_cntz),function(k0){
    # cat(round(k0/length(clea_cntz),2),"\r")
    clea_ <- clea_full[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]
    save(clea_,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
  })
  parallel::stopCluster(cl)
  gc()

  dir(temp)
  rm(save_list,clea_full)
  gc(reset = TRUE, full = TRUE)


  ###########################
  # Loop over countries
  ###########################
  k0 <- 3

  # PSOCK
  ncores <- min(length(clea_cntz),detectCores()/2)
  cl <- parallel::makePSOCKcluster(ncores, outfile="")
  parallel::setDefaultCluster(cl)
  parallel::clusterExport(NULL,c("clea_cntz","clea_yr","yrz","temp","y0","clea_prep","geo_mat","skip_existing","fix_gred","simp_gred","plot_yz","sub_dirz","sub_filez","r","mymax","mymin","mymean","mysum","popcrop","wrld_simpl"),envir = environment())
  parallel::clusterEvalQ(NULL, expr=library(data.table))
  parallel::clusterEvalQ(NULL, expr=library(tidyverse))
  parallel::clusterEvalQ(NULL, expr=library(sf))
  parallel::clusterEvalQ(NULL, expr=library(rmapshaper))
  parallel::clusterEvalQ(NULL, expr=library(stringi))
  parallel::clusterEvalQ(NULL, expr=library(raster))
  parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
  cntz_list <- parLapply(NULL,seq_along(clea_cntz),function(k0){

  # # Forking
  # cntz_list <- mclapply(seq_along(clea_cntz),function(k0){

    t1 <- Sys.time()
    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)))

    tryCatch({

    ###########################
    # Extract variables
    ###########################

    load(paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
    clea_mat <- clea_prep(
      clea_i = clea_yr[ISO3%in%clea_cntz[k0],],
      clea_i_t1 = clea_[yr==max(yr),] %>% as.data.table())
    clea_mat
    rm(clea_)

    # Merge with long/lats
    geo_sub <- geo_mat[(ISO3==clea_cntz[k0]) & (cst_n %in% clea_yr[ISO3%in%clea_cntz[k0],cst_n]) & (yr %in% yrz[y0]),]
    clea_mat <- merge(clea_mat,geo_sub %>% dplyr::select(-cst_n),by=c("ctr_n","cst"))

    # Select variables
    char_varz <- c("cst_n","cst","yr","yrmo","noncontested","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
    mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
    sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

    ###########################
    # Direct merge
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GRED/"))){

      # Load GRED polygons
      gred <- read_sf(paste0(temp,"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f2],"/",yrz[y0],"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f4],".shp"))
      if(clea_cntz[k0]%in%fix_gred){gred <- gred %>% fix_geom()}

      # Fix character encoding issue
      gred <- gred %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Merge
      gred_mat <- merge(gred ,clea_mat %>% dplyr::select(-ctr_n,-ctr,-cst_n,-yr),by=c("cst"),all.x=T,all.y=F) %>% st_transform(4326)  %>% setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      # par(mar=c(0,0,0,0),mfrow=c(1,1)); plot(gred_mat["to1"],border="black",breaks=11);dev.off()

      # Save 
      save(gred_mat,file=paste0("Elections/CLEA/Processed/GRED/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # plot_var <- "PVS1_NINCUMB"
      # plot_yz(map=gred_mat,plot_var=plot_var %>% toupper()); dev.off()

      # Visualize
      for(plot_var in c(mean_varz)){
        tryCatch({
          if(gred_mat[plot_var %>% toupper()] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gred.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(map=gred_mat,plot_var=plot_var %>% toupper())
            dev.off()
            # plot(gred_mat[paste0(plot_var)],lwd=.25)
            # dev.off()
          }
        },error=function(e){})
      }

    }

    ###########################
    # Point-in-polygon method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GRED_PointInPoly/"))){

      # Copy
      clea_mat_pip <- clea_mat

      # Load GRED polygons
      gred <- read_sf(paste0(temp,"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f2],"/",yrz[y0],"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f4],".shp")) %>% st_transform(4326)
      if(clea_cntz[k0]%in%fix_gred){gred <- gred %>% fix_geom()}
      # gred$o0 <- row.names(gred)

      # Fix character encoding issue
      gred <- gred %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gred)){
        t1_ <- Sys.time()
        gred <- rmapshaper::ms_simplify(gred,keep=simp_gred[clea_cntz[k0]],keep_shapes=T) %>% fix_geom()
        t2_ <- Sys.time()
        print(t2_-t1_)
      }

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(gred))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(gred))
        )
      
      # Aggregate over polygons
      clea_chars_0 <- point2poly_simp(polyz=gred,pointz=cst_sf,varz=char_varz[!char_varz%in%names(gred)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_0 <- point2poly_simp(polyz=gred,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_0 <- point2poly_simp(polyz=gred,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)

      # Merge
      idvar <- "cst"
      gred_mat <- gred %>% 
        merge(clea_chars_0 %>% as.data.table() %>% dplyr::select(c(idvar,char_varz[!char_varz%in%names(gred)]) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_0 %>% as.data.table() %>% dplyr::select(c(idvar,mean_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
        setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::mutate(ISO3 = clea_cntz[k0], YEAR = yrz[y0]) %>% mutate(YRMO = cst_sf$yrmo[1] %>% as.numeric()) %>%
        dplyr::select(ISO3,YRMO,YEAR,everything()) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
        
      # Save 
      save(gred_mat,file=paste0("Elections/CLEA/Processed/GRED_PointInPoly/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      png(paste0("Elections/CLEA/Maps/GRED_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_cst.png"),width=4*1.618,height=4,units="in",res=100)
        par(mar=c(0,0,0,0))
        plot(gred$geometry,lwd=.25)
        points(as(cst_sf,"Spatial"))
      dev.off()
      for(plot_var in c(mean_varz)){
        tryCatch({
          if(gred_mat[plot_var %>% toupper()] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gred1.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(gred_mat,plot_var %>% toupper())
            # plot(gred_mat[paste0(plot_var) %>% toupper()],lwd=.25)
            dev.off()
          }
        },error=function(e){})
      }

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished PointInPoly"))

    # Clean workspace
    rm(list=ls()[grep("gred_mat|_char|_means|_sums|cst_sf|clea_mat_pip|^map",ls())])
    gc(reset=T)

    ###########################
    # Voronoi method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GRED_Voronoi/"))){

      # Load GRED polygons
      gred <- read_sf(paste0(temp,"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f2],"/",yrz[y0],"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f4],".shp")) %>% st_transform(4326)
      if(clea_cntz[k0]%in%fix_gred){gred <- gred %>% fix_geom()}
      format(object.size(gred),units="Mb")

      # Fix character encoding issue
      gred <- gred %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gred)){
        t1_ <- Sys.time()
        gred <- rmapshaper::ms_simplify(gred,keep=simp_gred[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        t2_ <- Sys.time()
        print(t2_-t1_)
      }
      format(object.size(gred),units="Mb")
 
      # Load country borders
      map0_ <- wrld_simpl[wrld_simpl$ISO3%in%clea_cntz[k0],] %>% st_as_sf() %>% st_transform(st_crs(gred))

      # # Buffer 
      # map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")
      # map0_ne_ <- suppressMessages(
      #   map0_ne[map0_ne$ISO3%in%clea_cntz[k0],] %>% st_buffer(dist=1)
      #   )

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(gred))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf %>% st_jitter(),st_bbox(map0_))
        )

      # Crop raster by country extent
      b <- as(raster::extent(st_bbox(map0_)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
      crs(b) <- crs(r)
      b <- st_as_sf(b) 
      r_crop <- crop(r, b)

      # Tesselation
      clea_gred_list <- point2poly_tess(
        pointz=cst_sf,
        polyz=gred,
        poly_id="cst",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        varz=list(sum_varz,mean_varz),
        char_varz=char_varz,
        funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
        return_tess=TRUE
        )
      clea_gred_list

      # Rename variables
      geo_vor <- clea_gred_list[["tess"]] %>%
        setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,everything()) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      clea_gred <- clea_gred_list[["result"]] %>%
        setnames(old=c(paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c(paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% mutate(ISO3 = clea_cntz[k0]) %>%
        dplyr::select(ISO3,YRMO,YEAR,everything()) %>% 
        dplyr::select(-one_of("yr_pw","yrmo_pw","noncontested_pw")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      clea_gred

      # Save 
      save(geo_vor,clea_gred,file=paste0("Elections/CLEA/Processed/GRED_Voronoi/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_000_voronoi.png"),width=4*1.618,height=4,units="in",res=100)
        par(mar=c(0,0,0,0))
        plot(geo_vor$geometry,lwd=.25)
        dev.off()
        for(plot_var in mean_varz){#print(plot_var)
          if(geo_vor[plot_var %>% toupper()] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_voronoi.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(geo_vor,plot_var %>% toupper())
            dev.off()
            png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_gred.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_gred,paste0(plot_var %>% toupper(),"_PW"))
            dev.off()
            png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_gred.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_gred,paste0(plot_var %>% toupper(),"_AW"))
            dev.off()
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished Voronoi"))

    # Clean workspace
    rm(list=ls()[grep("clea_gred|_char|_means|_sums|cst_sf",ls())])
    gc(reset=T)


    ###########################
    # Kriging
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GRED_Kriging/"))){

      # Load GRED polygons
      gred <- read_sf(paste0(temp,"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f2],"/",yrz[y0],"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f4],".shp")) %>% st_transform(st_crs("EPSG:4326"))
      if(clea_cntz[k0]%in%fix_gred){gred <- gred %>% fix_geom()}
      format(object.size(gred),units="Mb")

      # Fix character encoding issue
      gred <- gred %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gred)){
        t1_ <- Sys.time()
        suppressWarnings({
          gred <- rmapshaper::ms_simplify(gred,keep=simp_gred[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        })
        t2_ <- Sys.time()
        print(t2_-t1_)
      }
      format(object.size(gred),units="Mb")

      # Load country borders
      map0_ <- wrld_simpl[wrld_simpl$ISO3%in%clea_cntz[k0],] %>% st_as_sf() %>% fix_geom() %>% st_transform(st_crs(gred))
      st_crs(gred) <- st_crs(map0_)

      # # Country borders
      # map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")
      # map0_ <- map0_ne[map0_ne$ISO3%in%clea_cntz[k0],] %>% st_transform(4326)

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      suppressWarnings({
        cst_sf <-  suppressMessages(
          st_crop(cst_sf %>% st_jitter(),st_bbox(map0_) )
          )
      })

      # Ordinary kriging 
      suppressWarnings({
        clea_gred <- point2poly_krig(
          pointz=cst_sf,
          polyz=gred ,
          varz=mean_varz,
          messagez=clea_cntz[k0]) %>% as.data.table()
      })

      # Rename variables
      # clea_gred[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_gred <- clea_gred %>% mutate(ISO3 = clea_cntz[k0], yr= cst_sf$yr[1], yrmo = cst_sf$yrmo[1]) %>% setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,everything()) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      
      # Save 
      save(clea_gred,file=paste0("Elections/CLEA/Processed/GRED_Kriging/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        for(plot_var in mean_varz){#print(plot_var)
          if(clea_gred[paste0(plot_var %>% toupper(),"_KR")] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gred.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_gred,paste0(plot_var %>% toupper(),"_KR"))
            dev.off()
          }
        }
      },error=function(e){})


    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished Kriging"))

    # Clean workspace
    rm(list=ls()[grep("clea_gred|_char|_means|_sums|cst_sf",ls())])
    gc(reset=T)

  ################################
  # Close country loop
  ################################

  },error=function(e){print(paste0(clea_cntz[k0]," ",yrz[y0]," ERROR"))})

  t2 <- Sys.time()
  print(t2-t1)

  # Close parLapply
  })
  parallel::stopCluster(cl)
  gc()

  # # Close mclapply
  # },mc.cores = min(length(clea_cntz),detectCores()/2))
  # gc()

################################
# Close year loop
################################

# # Delete temporary GAUL files
# for(f0 in dir(temp)[grep(paste0(yrz[y0],".RData$"),dir(temp))]){
#   file.remove(paste0(temp,"/",f0))
# }

}





