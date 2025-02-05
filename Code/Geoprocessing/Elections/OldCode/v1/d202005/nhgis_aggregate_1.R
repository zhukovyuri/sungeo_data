# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/nhgis_aggregate_1.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/nhgis_aggregate_1.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhuk","zhukov")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","V8")
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
## NHGIS
###############################################
###############################################
###############################################

# List of countries with broken nhgis geometries
# fix_poly <- c("JPN","NIC","PAK","BRA","ITA","AUS")
fix_poly <- c("XYZ")

# List of countries to skip
# skip_poly <- c("USA","KEN","MLT","BEN")
skip_poly <- c("XYZ")

# List of countries to simplify polygons
simp_poly <- list(USA=.01)

# Skip existing files?
skip_existing <- TRUE

# Source custom functions
source("../Code/functions.R")

# Clear tempdir
# tempdir() %>% normalizePath() %>% paste0(., "/", dir(tempdir())) %>% unlink(recursive = TRUE)
dir(tempdir())

# Set tempdir
temp <- tempdir()

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
# clea <- clea[yr>=1990,]

# Load coordinates
load("Elections/CLEA/GeoCode/cst_best_v4.RData")
# Fix character encoding issue (convert all to ASCII)
geo_mat <- geo_mat %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% as.data.table()
# Add country codes
geo_mat[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat

# List of country-years in CLEA
sub_dirz <- clea[,.(ISO3,yr)] %>% unique() %>% data.table::setnames(names(.),tolower(names(.))) %>% arrange(yr,iso3) %>% dplyr::filter(!is.na(iso3)&yr<=2014) %>% as.data.table()

# Overlap with country-years in NHGIS
head(sub_dirz)
dir("Admin/IPUMS_NHGIS/Simplified")
nhg_cy <- dir("Admin/IPUMS_NHGIS/Simplified") %>% str_split("_|\\.geojson",simplify=TRUE) %>% (function(.){.[,2:3]}) %>% as.data.frame(stringsAsFactors = FALSE) %>% data.table::setnames(c("iso3","yr")) %>% unique() %>% dplyr::mutate(yr=as.numeric(yr)) %>% dplyr::filter(!is.na(yr)) %>% dplyr::select(iso3,yr) %>% dplyr::arrange(yr) %>% as.data.table()
nhg_cy <- nhg_cy %>% merge(data.frame(yr = range(nhg_cy$yr) %>% (function(x){floor((x[1]:x[2])/10)*10}), yr_alt = range(nhg_cy$yr) %>% (function(x){x[1]:x[2]}),stringsAsFactors=FALSE) %>% dplyr::mutate(yr = replace(yr, yr >= 2010, yr_alt[yr >= 2010])) %>% dplyr::mutate(yr = replace(yr, yr == 2016, 2015)),by="yr",all=T) %>% dplyr::select(iso3,yr_alt,yr) %>% data.table::setnames("yr","yr_dec") %>% as.data.table()
sub_dirz <- sub_dirz %>% merge(nhg_cy, by.x = c("iso3","yr"), by.y = c("iso3","yr_alt"),all.x=F,all.y=F) %>% dplyr::arrange(iso3,yr) %>% as.data.table()

# SUbset clea
load(paste0(temp,"/clea_full.RData"))
clea_full <- clea_full[ISO3%in%sub_dirz$iso3,]
save(clea_full,file=paste0(temp,"/clea_full.RData")); rm(clea_full)
clea <- clea[ISO3%in%sub_dirz$iso3,]

# Loop over years
print("Prepping temp files")
y0 <- 10
yrz <- unique(clea$yr) %>% sort() %>% intersect(sub_dirz$yr)
for(y0 in 1:length(yrz)){
  print(yrz[y0])

  # Subset CLEA by year
  clea_yr <- clea[yr%in%yrz[y0],]
  clea_cntz <- clea_yr[,ISO3] %>% unique() %>% sort()

  # Exceptions
  clea_cntz <- clea_cntz[!clea_cntz%in%skip_poly]

  # Overlap only 
  clea_cntz <- clea_cntz[clea_cntz%in%sub_dirz$iso3]

  # Pop weights available only for >1990
  if(yrz[y0]>=1990){
    
    # Unzip population raster
    v_pop <- c(4,3)[c(yrz[y0]>=2000,yrz[y0]<2000)]
    fnam1 <- c("/gpw-v4-population-count-rev11_20","/gl_gpwv3_pcount_")[c(yrz[y0]>=2000,yrz[y0]<2000)]
    fnam2 <- c("_2pt5_min_asc.zip","_ascii_25.zip")[c(yrz[y0]>=2000,yrz[y0]<2000)]
    r_years <- cbind(1990:2024,rep(seq(1990,2020,by=5),each=5))
    con_pop <- unzip(zipfile = paste0("Population/GPW_v",v_pop,fnam1,substr(r_years[match(yrz[y0],r_years[,1]),2],3,4),fnam2), exdir = temp)
    r <- raster(con_pop[grep("ag.asc$|min.asc$",con_pop)])

    # Save yearly data 
    save(r,clea_cntz,clea_yr,file=paste0(temp,"/clea_yr_",yrz[y0],".RData"))  
  }
  if(yrz[y0]<1990){
    
    # Save yearly data 
    save(clea_cntz,clea_yr,file=paste0(temp,"/clea_yr_",yrz[y0],".RData"))  
  }

  # Save country files to temp folder
  load(paste0(temp,"/clea_full.RData"))

  # # PSOCK
  # print("Prepping temp files")
  # ncores <- min(length(clea_cntz),detectCores()-1)
  # cl <- parallel::makePSOCKcluster(ncores)
  # parallel::setDefaultCluster(cl)
  # parallel::clusterExport(NULL,c("clea_cntz","clea_full","yrz","temp","y0","map0","map1","map2"),envir = environment())
  # parallel::clusterEvalQ(NULL, expr=library(data.table))
  # # parallel::clusterEvalQ(NULL, expr=library(dplyr)) 
  # save_list <- parLapply(NULL,seq_along(clea_cntz),function(k0){
  #   map0_ <- map0[map0$ISO3%in%clea_cntz[k0],]
  #   map1_ <- map1[map1$ISO3%in%clea_cntz[k0],]
  #   map2_ <- map2[map2$ISO3%in%clea_cntz[k0],]
  #   save(map0_,map1_,map2_,file=paste0(temp,"/NHGIS_",clea_cntz[k0],"_",yrz[y0],".RData"))
  #   clea_ <- clea_full[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]
  #   save(clea_,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
  # })
  # parallel::stopCluster(cl)
  # gc()

  # Forking
  k0 <- 1
  save_list <- mclapply(seq_along(clea_cntz),function(k0){
    clea_ <- clea_full[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]
    save(clea_,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
  },mc.cores=min(length(clea_cntz),detectCores()-1))

  dir(temp)
  rm(save_list,clea_full)
  gc(reset = TRUE, full = TRUE)

}

###########################
# Loop over country-years
###########################
k0 <- 1
sub_dirz[k0,]

# PSOCK
ncores <- min(nrow(sub_dirz),detectCores()/3)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("yrz","temp","y0","geo_mat","skip_existing","fix_poly","simp_poly","skip_poly","sub_dirz"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(tidyverse))
parallel::clusterEvalQ(NULL, expr=library(sf))
parallel::clusterEvalQ(NULL, expr=library(rmapshaper))
parallel::clusterEvalQ(NULL, expr=library(stringi))
parallel::clusterEvalQ(NULL, expr=library(raster))
parallel::clusterEvalQ(NULL, expr=library(countrycode))
parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
cntz_list <- parLapply(NULL,1:nrow(sub_dirz),function(k0){

# # Forking
# cntz_list <- mclapply(seq_along(clea_cntz),function(k0){


  # Execute only if one of the files doesn't exist
  if(!skip_existing|(skip_existing&!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_simp/")
    &!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_simp/")
    &!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_tess/")
    &!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_krig/"))){

    # Timer
    t1 <- Sys.time()
    print(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)))

    # Source functions
    source("../Code/functions.R")

    # Error catching
    tryCatch({

    ###########################
    # Extract variables
    ###########################

    # Load year data
    load(paste0(temp,"/clea_yr_",sub_dirz$yr[k0],".RData"))

    # Load country-year data
    load(paste0(temp,"/CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData"))
    clea_mat <- clea_prep(
      clea_i = clea_yr[ISO3%in%sub_dirz$iso3[k0],],
      clea_i_t1 = clea_[yr==max(yr),] %>% as.data.table())
    rm(clea_)

    # Merge with long/lats
    geo_sub <- geo_mat[(ISO3==sub_dirz$iso3[k0]) & (cst_n %in% clea_yr[ISO3%in%sub_dirz$iso3[k0],cst_n]) & (yr %in% sub_dirz$yr[k0]),]
    clea_mat <- merge(clea_mat,geo_sub %>% dplyr::select(-cst_n),by=c("ctr_n","cst"))

    # Select variables
    char_varz <- c("cst_n","cst","yr","yrmo","noncontested","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
    mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
    sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

   
    ###########################
    # Simple overlay method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_simp/"))){

      # Load NHGIS polygons
      map0_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM0_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      map1_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM1_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      map2_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM2_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      
      # Fix character encoding issue
      map0_ <- map0_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()
      map1_ <- map1_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()
      map2_ <- map2_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      # plot(map0_$geometry)
      # plot(cst_sf$geometry,add=T)
      suppressWarnings({
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )
      })
      
      # Aggregate over polygons
      clea_chars_0 <- point2poly_simp(polyz=map0_,pointz=cst_sf,varz=char_varz[!char_varz%in%names(map0_)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_0 <- point2poly_simp(polyz=map0_,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_0 <- point2poly_simp(polyz=map0_,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)
      clea_chars_1 <- point2poly_simp(polyz=map1_,pointz=cst_sf,varz=char_varz[!char_varz%in%names(map0_)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_1 <- point2poly_simp(polyz=map1_,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_1 <- point2poly_simp(polyz=map1_,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)
      clea_chars_2 <- point2poly_simp(polyz=map2_,pointz=cst_sf,varz=char_varz[!char_varz%in%names(map0_)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_2 <- point2poly_simp(polyz=map2_,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_2 <- point2poly_simp(polyz=map2_,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)

      # Merge
      idvar <- "ADM0_ISO3"
      clea_nhgis0 <- map0_ %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        merge(clea_chars_0 %>% as.data.table() %>% dplyr::select(c(idvar,char_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_0 %>% as.data.table() %>% dplyr::select(c(idvar,mean_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,everything())%>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_nhgis0
      idvar <- "ADM1_CODE"
      clea_nhgis1 <- map1_ %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        merge(clea_chars_1 %>% as.data.table() %>% dplyr::select(c(idvar,char_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_1 %>% as.data.table() %>% dplyr::select(c(idvar,mean_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_1 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,NHGIS_CODE_1,NHGIS_NAME_1,everything()) %>%
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_nhgis1
      idvar <- "ADM2_CODE"
      clea_nhgis2 <- map2_ %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        merge(clea_chars_2 %>% as.data.table() %>% dplyr::select(c(idvar,char_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_2 %>% as.data.table() %>% dplyr::select(c(idvar,mean_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_2 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,NHGIS_CODE_1,NHGIS_NAME_1,NHGIS_CODE_2,NHGIS_NAME_2,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_nhgis2

      # Save 
      save(clea_nhgis0,clea_nhgis1,clea_nhgis2,file=paste0("Elections/CLEA/Processed/NHGIS_simp/NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/NHGIS_simp/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_000.png"),width=4*1.618,height=4,units="in",res=100)
          par(mar=c(0,0,0,0))
          plot(clea_nhgis2$geometry,lwd=.25)
          points(as(cst_sf,"Spatial"))
        dev.off()
        for(plot_var in mean_varz){
          if(clea_nhgis1[,plot_var %>% toupper(),with=FALSE] %>% na.omit() %>% nrow() > 0){
            tryCatch({
            clea_sf <- clea_nhgis1 %>% st_as_sf()
            png(paste0("Elections/CLEA/Maps/NHGIS_simp/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_nhgis1.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_sf,plot_var %>% toupper())
            dev.off()
            clea_sf <- clea_nhgis2 %>% st_as_sf()
            png(paste0("Elections/CLEA/Maps/NHGIS_simp/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_nhgis2.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_sf,plot_var %>% toupper())
            dev.off()
            },error=function(e){})
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)," -- finished simple overlay"))

    # Clean workspace
    rm(list=ls()[grep("clea_nhgis|_char|_means|_sums|cst_sf|clea_mat_pip|^map",ls())])
    gc(reset=T)

    ###########################
    # Voronoi method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_tess/"))){

      # Load NHGIS polygons
      map0_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM0_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      map1_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM1_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      map2_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM2_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      
      # Fix character encoding issue
      map0_ <- map0_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()
      map1_ <- map1_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()
      map2_ <- map2_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      # plot(map0_$geometry)
      # plot(cst_sf$geometry,add=T)
      suppressWarnings({
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )
      })

      if(sub_dirz$yr[k0]<1990){
        # Tesselation
        clea_nhgis_list0 <- point2poly_tess(
          pointz=cst_sf,
          polyz=map0_ ,
          poly_id="ADM0_ISO3",
          methodz=c("aw"),
          varz=list(sum_varz,mean_varz),
          char_varz=char_varz,
          funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
          return_tess=TRUE
          )
        clea_nhgis_list0$result <- clea_nhgis_list0$result %>% bind_cols(clea_nhgis_list0$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_aw$")) %>% dplyr::mutate_if(is.character, list(~NA_character_)) %>% dplyr::mutate_if(is.numeric, list(~NA_real_)) %>% data.table::setnames(names(.),gsub("_aw$","_pw",names(.))) %>% as.data.table())
        clea_nhgis_list1 <- point2poly_tess(
          pointz=cst_sf,
          polyz=map1_ ,
          poly_id="ADM1_CODE",
          methodz=c("aw"),
          varz=list(sum_varz,mean_varz),
          char_varz=char_varz,
          funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
          return_tess=TRUE
          )
        clea_nhgis_list1$result <- clea_nhgis_list1$result %>% bind_cols(clea_nhgis_list1$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_aw$")) %>% dplyr::mutate_if(is.character, list(~NA_character_)) %>% dplyr::mutate_if(is.numeric, list(~NA_real_)) %>% data.table::setnames(names(.),gsub("_aw$","_pw",names(.))) %>% as.data.table())
        clea_nhgis_list2 <- point2poly_tess(
          pointz=cst_sf,
          polyz=map2_ ,
          poly_id="ADM2_CODE",
          methodz=c("aw"),
          varz=list(sum_varz,mean_varz),
          char_varz=char_varz,
          funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
          return_tess=TRUE
          )
        clea_nhgis_list2$result <- clea_nhgis_list2$result %>% bind_cols(clea_nhgis_list2$result %>% as.data.table() %>% dplyr::select(-"geometry") %>% dplyr::select(matches("_aw$")) %>% dplyr::mutate_if(is.character, list(~NA_character_)) %>% dplyr::mutate_if(is.numeric, list(~NA_real_)) %>% data.table::setnames(names(.),gsub("_aw$","_pw",names(.))) %>% as.data.table())
      }
      if(sub_dirz$yr[k0]>=1990){
        # Crop raster by country extent
        b <- as(raster::extent(st_bbox(map0_)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
        crs(b) <- crs(r)
        b <- st_as_sf(b) 
        r_crop <- crop(r, b)
        # Tesselation        
        clea_nhgis_list0 <- point2poly_tess(
          pointz=cst_sf,
          polyz=map0_ ,
          poly_id="ADM0_ISO3",
          methodz=c("aw","pw"),
          pop_raster=r_crop,
          varz=list(sum_varz,mean_varz),
          char_varz=char_varz,
          funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
          return_tess=TRUE
          )
         clea_nhgis_list1 <- point2poly_tess(
          pointz=cst_sf,
          polyz=map1_ ,
          poly_id="ADM1_CODE",
          methodz=c("aw","pw"),
          pop_raster=r_crop,
          varz=list(sum_varz,mean_varz),
          char_varz=char_varz,
          funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
          return_tess=TRUE
          )
        clea_nhgis_list2 <- point2poly_tess(
          pointz=cst_sf,
          polyz=map2_ ,
          poly_id="ADM2_CODE",
          methodz=c("aw","pw"),
          pop_raster=r_crop,
          varz=list(sum_varz,mean_varz),
          char_varz=char_varz,
          funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
          return_tess=TRUE
          )
      }

      # Rename variables
      geo_vor <- clea_nhgis_list2[["tess"]] %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% dplyr::select(-ISO3) %>%
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_nhgis0 <- clea_nhgis_list0[["result"]]  %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME",paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2",paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng","yr_pw","yrmo_pw","noncontested_pw")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_nhgis1 <- clea_nhgis_list1[["result"]]  %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME",paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2",paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,NHGIS_CODE_1,NHGIS_NAME_1,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng","yr_pw","yrmo_pw","noncontested_pw")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_nhgis2 <- clea_nhgis_list2[["result"]]  %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME",paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2",paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,NHGIS_CODE_1,NHGIS_NAME_1,NHGIS_CODE_2,NHGIS_NAME_2,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng","yr_pw","yrmo_pw","noncontested_pw")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())

      # Save 
      save(geo_vor,clea_nhgis0,clea_nhgis1,clea_nhgis2,file=paste0("Elections/CLEA/Processed/NHGIS_tess/NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/NHGIS_tess/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_000_voronoi.png"),width=4*1.618,height=4,units="in",res=100)
        par(mar=c(0,0,0,0))
        plot(geo_vor$geometry,lwd=.25)
        dev.off()
        for(plot_var in mean_varz){#print(plot_var)
          if(geo_vor[plot_var %>% toupper()] %>% na.omit() %>% nrow() > 0){
            tryCatch({
            png(paste0("Elections/CLEA/Maps/NHGIS_tess/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_voronoi.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(geo_vor,plot_var %>% toupper())
            dev.off()
            if(sub_dirz$yr[k0]>=1990){
              png(paste0("Elections/CLEA/Maps/NHGIS_tess/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_pw_nhgis2.png"),width=4*1.618,height=4,units="in",res=100)
              plot_yz(clea_nhgis2,paste0(plot_var %>% toupper(),"_PW"))
              dev.off()
            }
            png(paste0("Elections/CLEA/Maps/NHGIS_tess/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_aw_nhgis1.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_nhgis1,paste0(plot_var %>% toupper(),"_AW"))
            dev.off()
            png(paste0("Elections/CLEA/Maps/NHGIS_tess/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_aw_nhgis2.png"),width=4*1.618,height=4,units="in",res=100)
            plot_yz(clea_nhgis2,paste0(plot_var %>% toupper(),"_AW"))
            dev.off()
            },error=function(e){})
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)," -- finished tess"))

    # Clean workspace
    rm(list=ls()[grep("clea_nhgis|_char|_means|_sums|cst_sf|^map",ls())])
    gc(reset=T)


    ###########################
    # Kriging
    ###########################

    if(!skip_existing|(skip_existing&!paste0("NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData")%in%dir("Elections/CLEA/Processed/NHGIS_krig/"))){

      # Load NHGIS polygons
      map0_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM0_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      map1_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM1_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      map2_ <- paste0("Admin/IPUMS_NHGIS/Simplified/NHGIS_",sub_dirz$iso3[k0],"_",sub_dirz$yr_dec[k0],"_ADM2_wgs.geojson") %>% st_read(quiet=T) %>% st_transform(st_crs("EPSG:4326")) 
      
      # Fix character encoding issue
      map0_ <- map0_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()
      map1_ <- map1_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()
      map2_ <- map2_ %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      # plot(map0_$geometry)
      # plot(cst_sf$geometry,add=T)
      suppressWarnings({
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )
      })

      # Ordinary kriging 
      clea_nhgis2 <- clea_nhgis1 <- clea_nhgis0 <- NULL
      suppressWarnings({
        tryCatch({
        clea_nhgis0 <- point2poly_krig(
          pointz=cst_sf,
          polyz=map0_ %>% lwgeom::st_make_valid(),
          varz=mean_varz,
          messagez=sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])) %>% as.data.table()
        },error=function(e){print(paste0("point2poly_krig ERROR: ",sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])," adm0"))})
        tryCatch({
        clea_nhgis1 <- point2poly_krig(
          pointz=cst_sf,
          polyz=map1_  %>% lwgeom::st_make_valid(),
          varz=mean_varz,
          messagez=sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])) %>% as.data.table()
        },error=function(e){print(paste0("point2poly_krig ERROR: ",sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])," adm1"))})
        tryCatch({
        clea_nhgis2 <- point2poly_krig(
          pointz=cst_sf,
          polyz=map2_ %>% lwgeom::st_make_valid() ,
          varz=mean_varz,
          messagez=sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])) %>% as.data.table()
          },error=function(e){print(paste0("point2poly_krig ERROR: ",sub_dirz$iso3[k0] %>% paste0(.,"_",sub_dirz$yr[k0])," adm2"))})
      })

      # Rename variables
      tryCatch({
      clea_nhgis0[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_nhgis0 <- clea_nhgis0 %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      },error=function(e){})
      tryCatch({
      clea_nhgis1[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_nhgis1 <- clea_nhgis1 %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,NHGIS_CODE_1,NHGIS_NAME_1,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      },error=function(e){})
      tryCatch({
      clea_nhgis2[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_nhgis2 <- clea_nhgis2 %>% dplyr::mutate(ADM0_ISO3 = sub_dirz$iso3[k0], ADM0_NAME = countrycode::countrycode(sub_dirz$iso3[k0],origin="iso3c",destination="country.name")) %>% 
        data.table::setnames(old=c("ADM0_ISO3","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("ISO3","NHGIS_NAME_0","NHGIS_CODE_1","NHGIS_NAME_1","NHGIS_CODE_2","NHGIS_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        dplyr::select(ISO3,YRMO,YEAR,NHGIS_NAME_0,NHGIS_CODE_1,NHGIS_NAME_1,NHGIS_CODE_2,NHGIS_NAME_2,everything()) %>% 
        dplyr::select(-one_of("NHGISCTY","ICPSRCTY","ICPSRNAM","ICPSRCTYI","ICPSRSTI","ICPSRFIP","STATE","COUNTY","PID","X_CENTROID","Y_CENTROID","DECADE","NHGISST","ICPSRST","GISJOIN","SHAPE_AREA","SHAPE_LEN","STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        data.table::setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      },error=function(e){})

      # Save 
      save(clea_nhgis0,clea_nhgis1,clea_nhgis2,file=paste0("Elections/CLEA/Processed/NHGIS_krig/NHGIS_CLEA_",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],".RData"))

      # Visualize
      tryCatch({
        for(plot_var in mean_varz){#print(plot_var)
          if(clea_nhgis1[paste0(plot_var %>% toupper(),"_KR")] %>% na.omit() %>% nrow() > 0){
            tryCatch({
              png(paste0("Elections/CLEA/Maps/NHGIS_krig/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_nhgis1.png"),width=4*1.618,height=4,units="in",res=100)
              plot_yz(clea_nhgis1,paste0(plot_var %>% toupper(),"_KR"))
              dev.off()
              png(paste0("Elections/CLEA/Maps/NHGIS_krig/",sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],"_",plot_var,"_nhgis2.png"),width=4*1.618,height=4,units="in",res=100)
              plot_yz(clea_nhgis2,paste0(plot_var %>% toupper(),"_KR"))
              dev.off()
            },error=function(e){})
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(sub_dirz$iso3[k0],"_",sub_dirz$yr[k0],", ",k0,"/",nrow(sub_dirz)," -- finished Kriging"))

    # Clean workspace
    rm(list=ls()[grep("clea_nhgis|_char|_means|_sums|cst_sf|^map",ls())])
    gc(reset=T)

  ################################
  # Close country loop
  ################################

  },error=function(e){message(paste0(sub_dirz$iso3[k0]," ",sub_dirz$yr[k0]," ERROR"));message(e)})

  t2 <- Sys.time()
  print(t2-t1)

# Close existing
}

# Close parLapply
})
parallel::stopCluster(cl)
gc()

# # Close mclapply
# },mc.cores = min(length(clea_cntz),detectCores()/2))
# gc()

# # Delete temp files
# unlink(temp, recursive = T)
