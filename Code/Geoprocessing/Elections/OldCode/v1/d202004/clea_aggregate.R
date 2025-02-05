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
list.of.packages <- c("sf",
                      "maptools",
                      "parallel",
                      "countrycode",
                      "rgdal",
                      "data.table",
                      "deldir",
                      "rgeos",
                      "lwgeom",
                      "raster",
                      "tidyverse",
                      "rmapshaper",
                      "Rfast")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)



###############################################
###############################################
###############################################
## GAUL
###############################################
###############################################
###############################################

# List of countries with broken GAUL geometries
fix_gaul <- c("JPN","NIC","PAK","BRA","ITA")

# List of countries to skip
skip_gaul <- c("USA","KEN","MLT","BEN")

# List of countries to simplify polygons
simp_gaul <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

# Skip existing files?
skip_existing <- TRUE

# Function to check and fix broken geometries
fix_geom <- function(x,self_int=TRUE){
  if(self_int){
  if(sum(grepl("Self-inter",st_is_valid(x,reason=T)))>0){
    suppressMessages(
      x <- x %>% st_buffer(dist=0)
      )
  }}
  if(!self_int){
  if(sum(!st_is_valid(x))>0){
    suppressMessages(
      x <- x %>% st_buffer(dist=0)
      )
  }}
  x
}

# Custom summary functions
mymax <- function(x){
  c(max(x,na.rm=T),NA)[c(length(x)>0,length(x)==0|nrow(x)==0|is.null(x))]
}
mymin <- function(x){
  c(min(x,na.rm=T),NA)[c(length(x)>0,length(x)==0|nrow(x)==0|is.null(x))]
}
mymean <- function(x){
  c(mean(x,na.rm=T),NA)[c(length(x)>0,length(x)==0|nrow(x)==0|is.null(x))]
}
mysum <- function(x){
  c(sum(x,na.rm=T),NA)[c(length(x)>0,length(x)==0|nrow(x)==0|is.null(x))]
}

# Set tempdir
temp <- tempdir()

# Load CLEA data
load("Elections/CLEA/clea_lc_20190617/clea_lc_20190617.rdata")
clea <- clea_lc_20190617 %>% as.data.table(); rm(clea_lc_20190617)
# Add country codes
clea[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
clea[ctr_n%in%"Micronesia",ISO3 := "FSM"]
clea[ctr_n%in%"Kosovo",ISO3 := "XKX"]
# Subset >1990
clea_full <- clea
save(clea_full,file=paste0(temp,"/clea_full.RData"))
rm(clea_full)
clea <- clea[yr>=1990,]

# Load coordinates
load("Elections/CLEA/GeoCode/cst_best_1.RData")
# Add country codes
geo_mat[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat

# Load Natural Earth boundaries
con_ne <- unzip(zipfile = "Admin/NE/ne_10m_admin_0_countries.zip", exdir = temp)
map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")

# # Load VDEM
# vdem <- fread("Elections/VDEM/Country_Year_V-Dem_Full+others_CSV_v9/V-Dem-CY-Full+Others-v9.csv") %>% filter(year==yrz[y0]) %>% as.data.table()
# vdem[,ISO3 := countrycode(country_name,"country.name","iso3c")]

# Loop over years
y0 <- 20
yrz <- unique(clea$yr) %>% sort() %>% intersect(1990:2014)
# seq_along(yrz)
for(y0 in 1:length(yrz)){
  print(yrz[y0])

  # Load GAUL
  con <- unzip(zipfile = paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_0.zip"), exdir = temp)
  map0 <- read_sf(paste0(temp,"/g2015_",yrz[y0],"_0"))
  map0$ISO3 <- countrycode(map0$ADM0_NAME,"country.name","iso3c")
  con <- unzip(zipfile = paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_1.zip"), exdir = temp)
  map1 <- read_sf(paste0(temp,"/g2015_",yrz[y0],"_1"))
  map1$ISO3 <- countrycode(map1$ADM0_NAME,"country.name","iso3c")
  con <- unzip(zipfile = paste0("Admin/GAUL/g2015_",ifelse(yrz[y0]<2015,yrz[y0],2014),"_2.zip"), exdir = temp)
  map2 <- read_sf(paste0(temp,"/g2015_",yrz[y0],"_2"))
  map2$ISO3 <- countrycode(map2$ADM0_NAME,"country.name","iso3c")

  # Subset CLEA by year
  clea_yr <- clea[yr%in%yrz[y0],]
  clea_cntz <- clea_yr[,ISO3] %>% unique() %>% sort()
  clea_cntz <- clea_cntz[clea_cntz %in% map0$ISO3]
  # map0$ISO3 %>% unique() %>% sort()
  # map0$ADM0_NAME %>% unique() %>% sort()

  # Exceptions
  clea_cntz <- clea_cntz[!clea_cntz%in%skip_gaul]

  # Unzip population raster
  v_pop <- c(4,3)[c(yrz[y0]>=2000,yrz[y0]<2000)]
  fnam1 <- c("/gpw-v4-population-count-rev11_20","/gl_gpwv3_pcount_")[c(yrz[y0]>=2000,yrz[y0]<2000)]
  fnam2 <- c("_2pt5_min_asc.zip","_ascii_25.zip")[c(yrz[y0]>=2000,yrz[y0]<2000)]
  r_years <- cbind(1990:2024,rep(seq(1990,2020,by=5),each=5))
  con_pop <- unzip(zipfile = paste0("Population/GPW_v",v_pop,fnam1,substr(r_years[match(yrz[y0],r_years[,1]),2],3,4),fnam2), exdir = temp)
  r <- raster(con_pop[grep("ag.asc$|min.asc$",con_pop)])

  # Save country maps & rasters to temp folder
  save_list <- mclapply(seq_along(clea_cntz),function(k0){
    # Subset by country, check/fix geometries
    map0_ <- map0[map0$ISO3%in%clea_cntz[k0],] 
    map1_ <- map1[map1$ISO3%in%clea_cntz[k0],] 
    map2_ <- map2[map2$ISO3%in%clea_cntz[k0],] 
    if(clea_cntz[k0]%in%fix_gaul){
      map0_ <- map0_ %>% fix_geom()
      map1_ <- map1_ %>% fix_geom()
      map2_ <- map2_ %>% fix_geom()  
    }
    if(clea_cntz[k0]%in%"IND"){
      map0_ <- map0_[map0_$ADM0_NAME%in%"India",]
      map1_ <- map1_[map1_$ADM0_NAME%in%"India",]
      map2_ <- map2_[map2_$ADM0_NAME%in%"India",]
    }
    # print(clea_cntz[k0])
    # Crop raster by country extent
    b <- as(extent(st_bbox(map0_)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
    crs(b) <- crs(r)
    b <- st_as_sf(b) 
    r_crop <- crop(r, b)
    # plot(r_crop)
    save(map0_,map1_,map2_,r_crop,file=paste0(temp,"/GAUL_",clea_cntz[k0],"_",yrz[y0],".RData"))
  },mc.cores=min(length(clea_cntz),detectCores()-1))
  k0 <- 1
  load(paste0(temp,"/clea_full.RData"))
  save_list <- mclapply(seq_along(clea_cntz),function(k0){
    # cat(round(k0/length(clea_cntz),2),"\r")
    clea_ <- clea_full[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]
    save(clea_,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
  },mc.cores=min(length(clea_cntz),detectCores()-1))
  dir(temp)
  rm(map0,map1,map2,save_list,clea_full,r)
  gc(reset = TRUE)


  ###########################
  # Loop over countries
  ###########################
  k0 <- 2
  cntz_list <- mclapply(seq_along(clea_cntz),function(k0){

    t1 <- Sys.time()
    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)))

    tryCatch({

    # Subset CLEA
    clea_i <- clea_yr[ISO3%in%clea_cntz[k0],]
    load(paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
    clea_i_t1 <- clea_[yr==max(yr),] %>% as.data.table; rm(clea_)

    # Identify biggest seat-getter from last election
    clea_i_t1[,win_pty := pty_n[which.max(pv1)],by=cst]
    incumb_npty <- clea_i_t1[,unique(win_pty),by=cst] %>% select(V1) %>% table() %>% sort() %>% names() %>% last()
    if(length(incumb_npty)==0){incumb_npty <- NA}

    # Constituency-level summaries
    ctsz <- unique(clea_i$cst) %>% sort()
    c0 <- 1
    clea_mat <- lapply(seq_along(ctsz),function(c0){#print(c0)

      # Subset
      clea_sub <- clea_i %>% filter(cst==ctsz[c0]) %>% mutate_all(list(function(x){na_if(x,-990)})) %>% mutate_all(list(function(x){na_if(x,-992)})) %>% as.data.table()
      clea_sub_t1 <- clea_i_t1 %>% filter(tolower(cst_n)==tolower(clea_sub$cst_n[1])) %>% mutate_all(list(function(x){na_if(x,-990)})) %>% mutate_all(list(function(x){na_if(x,-992)})) %>% as.data.table()
      clea_sub
      clea_sub_t1

      # Candidate or party?
      c1_yes <- clea_sub %>% select(cv1) %>% unique() %>% nrow() > 1
      p1_yes <- clea_sub %>% select(pty_n) %>% unique() %>% nrow() > 1
      c2_yes <- clea_sub %>% select(cv2) %>% unique() %>% nrow() > 1
      p2_yes <- clea_sub %>% select(pv2) %>% unique() %>% nrow() > 1
      win1 <- c(which.max(clea_sub$pv1), which.max(clea_sub$cv1)) %>% na.omit() %>% unique()
      win1_pty <- which.max(clea_sub$pv1) %>% na.omit() %>% unique()
      win1_can <- which.max(clea_sub$cv1) %>% na.omit() %>% unique()
      win2 <- c(which.max(clea_sub$pv2), which.max(clea_sub$cv2)) %>% na.omit() %>% unique()
      win2_pty <- which.max(clea_sub$pv2) %>% na.omit() %>% unique()
      win2_can <- which.max(clea_sub$cv2) %>% na.omit() %>% unique()
      win1_t1 <- c(which.max(clea_sub_t1$pv1), which.max(clea_sub_t1$cv1)) %>% na.omit() %>% unique()
      win1_t1_pty <- which.max(clea_sub_t1$pv1) %>% na.omit() %>% unique()
      win1_t1_can <- which.max(clea_sub_t1$cv1) %>% na.omit() %>% unique()
      win2_t1 <- c(which.max(clea_sub_t1$pv2), which.max(clea_sub_t1$cv2)) %>% na.omit() %>% unique()
      win2_t1_pty <- which.max(clea_sub_t1$pv2) %>% na.omit() %>% unique()
      win2_t1_can <- which.max(clea_sub_t1$cv2) %>% na.omit() %>% unique()
      if(length(win1)>1&c1_yes){win1 <- which.max(clea_sub$cv1)}
      if(length(win2)>1&c2_yes){win1 <- which.max(clea_sub$cv2)}
      if(length(win1_t1)>1&c1_yes){win1_t1 <- which.max(clea_sub_t1$cv1)}
      if(length(win2_t1)>1&c2_yes){win1_t2 <- which.max(clea_sub_t1$cv2)}

      # Fill missing vote shares
      clea_sub[is.na(cvs1)&!is.na(vv1)&!is.na(cv1),cvs1 := cv1/vv1]
      clea_sub[is.na(pvs1)&!is.na(vv1)&!is.na(pv1),pvs1 := pv1/vv1]
      clea_sub[is.na(cvs2)&!is.na(vv2)&!is.na(cv2),cvs2 := cv2/vv2]
      clea_sub[is.na(pvs2)&!is.na(vv2)&!is.na(pv2),pvs2 := pv2/vv2]
      clea_sub_t1[is.na(cvs1)&!is.na(vv1)&!is.na(cv1),cvs1 := cv1/vv1]
      clea_sub_t1[is.na(pvs1)&!is.na(vv1)&!is.na(pv1),pvs1 := pv1/vv1]
      clea_sub_t1[is.na(cvs2)&!is.na(vv2)&!is.na(cv2),cvs2 := cv2/vv2]
      clea_sub_t1[is.na(pvs2)&!is.na(vv2)&!is.na(pv2),pvs2 := pv2/vv2]

      # Incumbent vote shares
      can_match <- agrepl(paste0(clea_sub_t1[win1_t1,"can",with=F],""),clea_sub$can,ignore.case = T)|grepl(paste0(clea_sub_t1[win1_t1,"can",with=F] %>% str_extract("\\w+$"),""),  clea_sub$can %>% str_extract("\\w+$"),ignore.case = T)
      can_match[gsub("[^[:alpha:]]","",clea_sub$can) %>% nchar() %>% sum() == 0] <- FALSE
      can_match[is.na(can_match)] <- FALSE
      pty_match <- agrepl(clea_sub_t1[win1_t1_pty,"pty_n",with=F],clea_sub$pty_n,ignore.case = T)
      pty_match[gsub("^Independent$|^Independent \\d+$|^Unknown$","",clea_sub$pty_n) %>% nchar() %>% sum() == 0] <- FALSE
      pty_match[is.na(pty_match)] <- FALSE
      npty_match <- agrepl(incumb_npty,clea_sub$pty_n,ignore.case = T)
      npty_match[gsub("^Independent$|^Independent \\d+$|^Unknown$","",clea_sub$pty_n) %>% nchar() %>% sum() == 0] <- FALSE
      npty_match[is.na(npty_match)] <- FALSE
      cvs1_incumb_ <- suppressMessages(
        c(clea_sub[can_match,cvs1],clea_sub[,"cvs1",with=F] %>% top_n(3) %>% unlist() %>% min(na.rm=T))[1]
        ) %>% na_if(Inf) %>% na_if(-Inf)
      pvs1_incumb_ <- suppressMessages(
        c(clea_sub[pty_match,pvs1] %>% unique() %>% sum(na.rm=T),clea_sub[,"pvs1",with=F] %>% top_n(3) %>% unlist() %>% min(na.rm=T))[1]
        ) %>% na_if(Inf) %>% na_if(-Inf)
      pvs1_nincumb_ <- suppressMessages(
        c(clea_sub[npty_match,pvs1] %>% unique() %>% sum(na.rm=T),clea_sub[,"pvs1",with=F] %>% top_n(3) %>% unlist() %>% min(na.rm=T))[1]
        ) %>% na_if(Inf) %>% na_if(-Inf)

      # Extract info
      round1 <- data.frame(
        election_id = (clea_sub %>% select(id) %>% unique())[1],
        ctr = (clea_sub %>% select(ctr) %>% unique())[1],
        ctr_n = (clea_sub %>% select(ctr_n) %>% unique())[1],
        cst = ctsz[c0],
        cst_n = (clea_sub %>% select(cst_n) %>% unique())[1],
        yrmo = paste0(clea_sub$yr*100 + clea_sub$mn)[1],
        to1 = (clea_sub %>% select(to1) %>% unique())[1],
        pev1 = (clea_sub %>% select(pev1) %>% unique())[1],
        vot1 = (clea_sub %>% select(vot1) %>% unique())[1],
        vv1 = (clea_sub %>% select(vv1) %>% unique())[1],
        cv1_margin = suppressMessages(
          ifelse(c1_yes, clea_sub %>% select(cv1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pv1_margin = suppressMessages(
          ifelse(p1_yes, clea_sub %>% select(pv1) %>% unique() %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        v1_margin = suppressMessages(
          ifelse(c1_yes,clea_sub %>% select(cv1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), 
                        clea_sub %>% select(pv1) %>% top_n(2) %>% unlist() %>% diff() %>% abs()) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        cvs1_margin = suppressMessages(
          ifelse(c1_yes, clea_sub %>% select(cvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pvs1_margin = suppressMessages(
          ifelse(p1_yes, clea_sub %>% select(pvs1) %>% unique() %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pvs1_margin_incumb = suppressMessages(
          ifelse(p1_yes, (c(clea_sub[pty_match,pvs1],0)[1] - clea_sub[!pty_match] %>% select(pvs1) %>% unlist() %>% mymax()),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pvs1_margin_nincumb = suppressMessages(
          ifelse(p1_yes, (pvs1_nincumb_ - clea_sub[!npty_match] %>% select(pvs1) %>% unlist() %>% mymax()),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        vs1_margin = suppressMessages(
          ifelse(c1_yes, clea_sub %>% select(cvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), 
                         clea_sub %>% select(pvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs()) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        nincumb_pty_n = incumb_npty,
        nincumb_pty = c(clea_sub_t1[pty_n%in%incumb_npty,pty],NA)[1],
        incumb_pty_n = c(clea_sub_t1[win1_t1_pty,pty_n],NA)[1],
        incumb_pty = c(clea_sub_t1[win1_t1_pty,pty],NA)[1],
        incumb_can = c(clea_sub_t1[win1_t1_can,can],NA)[1],
        win1_pty_n = suppressMessages(
          c(clea_sub[win1_pty,pty_n],NA)[1]
          ),
        win1_pty = suppressMessages(
          c(clea_sub[win1_pty,pty],NA)[1]
          ),
        win1_can = suppressMessages(
          c(clea_sub[win1_can,can],NA)[1]
          ),
        pvs1_nincumb = suppressMessages(
          ifelse(p1_yes, c(clea_sub[npty_match,pvs1],0)[1],NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        cvs1_incumb = suppressMessages(
          ifelse(c1_yes, c(clea_sub[can_match,cvs1],0)[1],NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pvs1_incumb = suppressMessages(
          ifelse(p1_yes, c(clea_sub[pty_match,pvs1] %>% unique() %>% sum(na.rm=T),0)[1],NA) %>% na_if(Inf) %>% na_if(-Inf)
          ), 
        win_pvs1 = suppressMessages(
          ifelse(p1_yes, clea_sub[,pvs1] %>% unlist() %>% max(na.rm=T),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        win_cvs1 = suppressMessages(
          ifelse(c1_yes, clea_sub[,cvs1] %>% unlist() %>% max(na.rm=T),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        contest_p1_nincumb = suppressMessages(
          ifelse(p1_yes, 1 - (pvs1_nincumb_ - c(clea_sub[!npty_match,pvs1],rep(NA,sum(!npty_match)))[1:sum(!npty_match)] %>% max(na.rm=T)),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        contest_p1 = suppressMessages(
          ifelse(p1_yes, 1 - (pvs1_incumb_ - c(clea_sub[!pty_match,pvs1],rep(NA,sum(!pty_match)))[1:sum(!pty_match)] %>% max(na.rm=T)),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        contest_c1 =  suppressMessages(
          ifelse(c1_yes, 1 - (cvs1_incumb_ - c(clea_sub[!can_match,cvs1],rep(NA,sum(!can_match)))[1:sum(!can_match)] %>% max(na.rm=T)),NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        comptop2_c1 = suppressMessages(
          ifelse(c1_yes, 1-clea_sub %>% select(cvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        comptop2_p1 = suppressMessages(
          ifelse(p1_yes, 1-clea_sub %>% select(pvs1) %>% unique() %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        comptop1_c1 = suppressMessages(
          ifelse(c1_yes, 1-clea_sub %>% select(cvs1) %>% max(na.rm=T) %>% unlist(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        comptop1_p1 = suppressMessages(
          ifelse(p1_yes, 1-clea_sub %>% select(pvs1) %>% max(na.rm=T) %>% unlist(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        stringsAsFactors = FALSE
      )
      round1
      round2 <- data.frame(
        to2 = NA,
        pev2 = NA,
        vot2 = NA,
        vv2 = NA,
        cv2_margin = NA,
        pv2_margin = NA,
        v2_margin = NA,
        cvs2_margin = NA,
        pvs2_margin = NA,
        vs2_margin = NA,
        win2_pty_n = NA,
        win2_pty = NA,
        win2_can = NA,
        stringsAsFactors = FALSE
      )
      if(length(win2)>0){
      round2 <- data.frame(
        to2 = (clea_sub %>% select(to2) %>% unique())[1],
        pev2 = (clea_sub %>% select(pev2) %>% unique())[1],
        vot2 = (clea_sub %>% select(vot2) %>% unique())[1],
        vv2 = (clea_sub %>% select(vv2) %>% unique())[1],
         cv2_margin = suppressMessages(
          ifelse(c2_yes, clea_sub %>% select(cv1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pv2_margin = suppressMessages(
          ifelse(p2_yes, clea_sub %>% select(pv1) %>% unique() %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        v2_margin = suppressMessages(
          ifelse(c2_yes,clea_sub %>% select(cv1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), 
            clea_sub %>% select(pv1) %>% top_n(2) %>% unlist() %>% diff() %>% abs()) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        cvs2_margin = suppressMessages(
          ifelse(c2_yes, clea_sub %>% select(cvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        pvs2_margin = suppressMessages(
          ifelse(p2_yes, clea_sub %>% select(pvs1) %>% unique() %>% top_n(2) %>% unlist() %>% diff() %>% abs(), NA) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        vs2_margin = suppressMessages(
          ifelse(c2_yes, clea_sub %>% select(cvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs(), 
            clea_sub %>% select(pvs1) %>% top_n(2) %>% unlist() %>% diff() %>% abs()) %>% na_if(Inf) %>% na_if(-Inf)
          ),
        win2_pty_n = suppressMessages(
          c(clea_sub[win2_pty,pty_n],NA)[1]
          ),
        win2_pty = suppressMessages(
          c(clea_sub[win2_pty,pty],NA)[1]
          ),
        win2_can = suppressMessages(
          c(clea_sub[win2_can,can],NA)[1]
          ),
        stringsAsFactors = FALSE
      )
      }
      clea_out <- cbind(round1,round2)
      clea_out
    } ) %>% bind_rows() %>% as.data.table()
    clea_mat[1:10,]
    # clea_mat[to1<0]
    # ,mc.cores = min(length(ctsz),detectCores()-1) 

    # Merge with long/lats
    geo_sub <- geo_mat[(ISO3==clea_cntz[k0]) & (cst_n %in% clea_i[,cst_n]),] %>% setnames("id","geo_id")
    clea_mat <- merge(clea_mat,geo_sub,by=c("ctr_n","cst_n"))
    # clea_mat

    # Select variables
    char_varz <- c("cst_n","cst","yrmo","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
    mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
    sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

    ###########################
    # Point-in-polygon method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GAUL_PointInPoly/"))){

      # Copy
      clea_mat_pip <- clea_mat

      # Load GAUL polygons
      load(paste0(temp,"/GAUL_",clea_cntz[k0],"_",yrz[y0],".RData"))
      # plot(map0_$geometry)
      map0_$o0 <- row.names(map0_)
      map1_$o1 <- row.names(map1_)
      map2_$o2 <- row.names(map2_)

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gaul)){
        t1_ <- Sys.time()
        map0_ <- ms_simplify(map0_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom()
        # map1_ <- ms_simplify(map1_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom()
        # map2_ <- ms_simplify(map2_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom()
        t2_ <- Sys.time()
        print(t2_-t1_)
      }

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )
      
      # Find matching polygons for each point
      o0_ <- suppressMessages(
        st_within(cst_sf,map0_) %>% as.data.table()
        )
      o1_ <- suppressMessages(
        st_within(cst_sf,map1_) %>% as.data.table()
        )
      o2_ <- suppressMessages(
        st_within(cst_sf,map2_) %>% as.data.table()
        )
      clea_mat_pip[o0_$row.id,o0 := o0_$col.id]
      clea_mat_pip[o1_$row.id,o1 := o1_$col.id]
      clea_mat_pip[o2_$row.id,o2 := o2_$col.id]


      # ADM0
      a0_char <- lapply(seq_along(char_varz),function(j0){
        int_0_ <- clea_mat_pip[,list(w = get(char_varz[j0]) %>% unique() %>% paste0(.,collapse="|")),by=o0] %>% setnames("w",paste0(char_varz[j0]))
        if(j0>1){int_0_ <- int_0_ %>% select(-o0)}
        int_0_
        }) %>% bind_cols()
      a0_sums <- lapply(seq_along(sum_varz),function(j0){
        int_0_ <- clea_mat_pip[,list(w = sum(get(sum_varz[j0]),na.rm=T)),by=o0] %>% setnames("w",paste0(sum_varz[j0]))
        if(j0>1){int_0_ <- int_0_ %>% select(-o0)}
        int_0_
        }) %>% bind_cols()
      a0_means <- lapply(seq_along(mean_varz),function(j0){
        int_0_ <- clea_mat_pip[,list(w = mean(get(mean_varz[j0]),na.rm=T)),by=o0] %>% setnames("w",paste0(mean_varz[j0]))
        if(j0>1){int_0_ <- int_0_ %>% select(-o0)}
        int_0_
        }) %>% bind_cols()
      a0 <- merge(map0_,a0_char,by="o0",all.x=T,all.y=F)
      a0 <- merge(a0,a0_means,by="o0",all.x=T,all.y=F)
      a0 <- merge(a0,a0_sums,by="o0",all.x=T,all.y=F)
      # ADM1
      a1_char <- lapply(seq_along(char_varz),function(j0){
        int_1_ <- clea_mat_pip[,list(w = get(char_varz[j0]) %>% unique() %>% paste0(.,collapse="|")),by=o1] %>% setnames("w",paste0(char_varz[j0]))
        if(j0>1){int_1_ <- int_1_ %>% select(-o1)}
        int_1_
        }) %>% bind_cols()
      a1_sums <- lapply(seq_along(sum_varz),function(j0){
        int_1_ <- clea_mat_pip[,list(w = sum(get(sum_varz[j0]),na.rm=T)),by=o1] %>% setnames("w",paste0(sum_varz[j0]))
        if(j0>1){int_1_ <- int_1_ %>% select(-o1)}
        int_1_
        }) %>% bind_cols()
      a1_means <- lapply(seq_along(mean_varz),function(j0){
        int_1_ <- clea_mat_pip[,list(w = mean(get(mean_varz[j0]),na.rm=T)),by=o1] %>% setnames("w",paste0(mean_varz[j0]))
        if(j0>1){int_1_ <- int_1_ %>% select(-o1)}
        int_1_
        }) %>% bind_cols()
      a1 <- merge(map1_,a1_char,by="o1",all.x=T,all.y=F)
      a1 <- merge(a1,a1_means,by="o1",all.x=T,all.y=F)
      a1 <- merge(a1,a1_sums,by="o1",all.x=T,all.y=F)
      # ADM2
      a2_char <- lapply(seq_along(char_varz),function(j0){
        int_2_ <- clea_mat_pip[,list(w = get(char_varz[j0]) %>% unique() %>% paste0(.,collapse="|")),by=o2] %>% setnames("w",paste0(char_varz[j0]))
        if(j0>1){int_2_ <- int_2_ %>% select(-o2)}
        int_2_
        }) %>% bind_cols()
      a2_sums <- lapply(seq_along(sum_varz),function(j0){
        int_2_ <- clea_mat_pip[,list(w = sum(get(sum_varz[j0]),na.rm=T)),by=o2] %>% setnames("w",paste0(sum_varz[j0]))
        if(j0>1){int_2_ <- int_2_ %>% select(-o2)}
        int_2_
        }) %>% bind_cols()
      a2_means <- lapply(seq_along(mean_varz),function(j0){
        int_2_ <- clea_mat_pip[,list(w = mean(get(mean_varz[j0]),na.rm=T)),by=o2] %>% setnames("w",paste0(mean_varz[j0]))
        if(j0>1){int_2_ <- int_2_ %>% select(-o2)}
        int_2_
        }) %>% bind_cols()
      a2 <- merge(map2_,a2_char,by="o2",all.x=T,all.y=F)
      a2 <- merge(a2,a2_means,by="o2",all.x=T,all.y=F)
      a2 <- merge(a2,a2_sums,by="o2",all.x=T,all.y=F)

      # Save 
      save(a0,a1,a2,file=paste0("Elections/CLEA/Processed/GAUL_PointInPoly/GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      png(paste0("Elections/CLEA/Maps/GAUL_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_cst.png"),width=4,height=4,units="in",res=150)
        plot(a0$geometry,lwd=.25)
        points(as(cst_sf,"Spatial"))
      dev.off()
      for(plot_var in c(mean_varz)){
        tryCatch({
          if(a1[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GAUL_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul1.png"),width=4,height=4,units="in",res=150)
            plot(a1[paste0(plot_var)],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(a2[paste0(plot_var)],lwd=.25)
            dev.off()
          }
        },error=function(e){})
      }

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished PointInPoly"))

    # Clean workspace
    rm(list=ls()[grep("a0|a1|a2|_char|_means|_sums|cst_sf|clea_mat_pip|^map",ls())])
    gc(reset=T)

    ###########################
    # Voronoi method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GAUL_Voronoi/"))){

      # Load GAUL polygons
      load(paste0(temp,"/GAUL_",clea_cntz[k0],"_",yrz[y0],".RData"))
      format(object.size(map0_),units="Mb")
      format(object.size(map1_),units="Mb")
      format(object.size(map2_),units="Mb")
      # if(clea_cntz[k0]%in%"USA"){
      #   map1_ <- map1_[!map1_$ADM1_NAME%in%c("Alaska","Hawaii"),]
      #   map2_ <- map2_[!map2_$ADM1_NAME%in%c("Alaska","Hawaii"),]
      # }

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gaul)){
        t1_ <- Sys.time()
        map0_ <- ms_simplify(map0_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        map1_ <- ms_simplify(map1_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        map2_ <- ms_simplify(map2_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        t2_ <- Sys.time()
        print(t2_-t1_)
      }

      format(object.size(map0_),units="Mb")
      format(object.size(map1_),units="Mb")
      format(object.size(map2_),units="Mb")
      
      # png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/000_test1.png"),width=30,height=10,units="in",res=150); par(mar=c(0,0,0,0),mfrow=c(1,3)); plot(map0_$geometry); plot(map1_$geometry); plot(map2_$geometry); dev.off()


      # Buffer 
      # map0_ne_ <- suppressMessages(
      #   map0_ne[map0_ne$ISO3%in%clea_cntz[k0],] %>% st_buffer(dist=1)
      #   )
      map0_buff_ <- suppressMessages(
        map0_[map0_$ISO3%in%clea_cntz[k0],] %>% st_buffer(dist=1)
        )
      # plot(map0_ne_)

      # Voronoi polygons
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf_crop <-  suppressMessages(
        st_crop(cst_sf %>% st_jitter(),st_bbox(map0_))
        )
      cst_geom_crop <- cst_sf_crop  %>% st_geometry() %>% st_union()
      geo_vor <- suppressMessages(
        st_voronoi(cst_geom_crop) %>% st_cast() %>% st_as_sf() %>% st_intersection(map0_buff_$geometry) %>% setnames("x","geometry")
      )
      if(nrow(cst_sf_crop)==1){
        geo_vor <- suppressMessages(
          map0_ %>% st_geometry() %>% st_as_sf() %>% st_intersection(map0_buff_$geometry) %>% setnames("x","geometry")
          )
      }
      st_geometry(geo_vor) <- "geometry"

      # cst_sf$geometry %>% unique() %>% length()
      # cst_sf$geometry %>% length()
      # cst_sf_crop$geometry %>% unique() %>% length()
      # cst_sf_crop$geometry %>% length()
      # geo_vor$geometry %>% length()
      # geo_vor$geometry %>% unique() %>% length()
      # geo_vor$geometry %>% length()

      # Combine with attributes
      int <- suppressMessages(
        st_intersects(geo_vor,cst_sf_crop) %>% as.data.frame()
        )
      geo_vor <- bind_cols(geo_vor  %>% slice(int$row.id),cst_sf_crop %>% as.data.frame() %>% select(-geometry) %>% slice(int$col.id)) 

      # Area and pop-weighted interpolation 
      geo_vor <-  suppressMessages(
        st_intersection(geo_vor,map0_)
        )
      geo_vor$POP_TOTAL <- raster::extract(r_crop,geo_vor %>% as("Spatial"),fun=sum,na.rm=T)
      geo_vor$AREA_TOTAL <- st_area(geo_vor) %>% as.numeric()
      # ADM1
      int_1 <- suppressMessages(
        st_intersection(geo_vor,map1_)
        )
      # Fix weird Canada geometry issue
      if(int_1 %>% st_geometry_type() %>% grepl("GEOMETRY",.) %>% sum() > 0){
        # int_1 <- int_1[(int_1 %>% st_geometry_type() %>% grepl("GEOMETRY",.)),] %>% st_collection_extract(type="POLYGON") %>% ms_simplify(.,keep=.005) %>% fix_geom(self_int=FALSE) %>% st_union(int_1[!(int_1 %>% st_geometry_type() %>% grepl("GEOMETRY",.)),]) 
        int_1 <- int_1[!(int_1 %>% st_geometry_type() %>% grepl("GEOMETRY",.)),] 
        format(object.size(int_1),units="Mb")
      }      

      # png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/000_test1.png"),width=30,height=10,units="in",res=150); par(mar=c(0,0,0,0),mfrow=c(1,3)); plot(geo_vor$geometry); plot(int_1$geometry); plot(map1_$geometry); dev.off()
      int_1$POP_INT <- raster::extract(r_crop,int_1 %>% as("Spatial"),fun=sum,na.rm=T)
      int_1$POP_W <- int_1$POP_INT/int_1$POP_TOTAL
      int_1$AREA_INT <- st_area(int_1) %>% as.numeric()
      int_1$AREA_W <- int_1$AREA_INT/int_1$AREA_TOTAL
      # Convert to dt
      int_1_dt <- data.table(int_1)
      # Interpolate missing values
      if(sum(is.na(int_1_dt$POP_INT))==1){
        w <- pointDistance(int_1 %>% st_centroid() %>% st_geometry() %>% unlist() %>% matrix(ncol=2,byrow=T) %>% as.data.frame() %>% setNames(c("lon","lat")),lonlat=T) %>% as.dist() %>% as.matrix()
        diag(w) <- NA
        int_1_dt[is.na(POP_INT),POP_INT := int_1_dt$POP_INT[t(apply(w, 1, order)[ 1:min(10,nrow(int_1)), is.na(int_1$POP_INT)])] %>% mean(na.rm=T)]
        int_1_dt[is.na(POP_W),POP_W := POP_INT/POP_TOTAL]
      }
      if(sum(is.na(int_1_dt$POP_INT))>1){
        w <- pointDistance(int_1 %>% st_centroid() %>% st_geometry() %>% unlist() %>% matrix(ncol=2,byrow=T) %>% as.data.frame() %>% setNames(c("lon","lat")),lonlat=T) %>% as.dist() %>% as.matrix()
        diag(w) <- NA
        int_1_dt[is.na(POP_INT),POP_INT := t(apply(w, 1, order)[ 1:min(10,nrow(int_1)), is.na(int_1$POP_INT)]) %>% apply(1,function(.){mean(int_1$POP_INT[.],na.rm=T)})]
        int_1_dt[is.na(POP_W),POP_W := POP_INT/POP_TOTAL]
      }
      # if(sum(is.na(int_1_dt$POP_W))>1){
      #   w <- st_intersects(int_1,int_1) %>% as.matrix()
      #   int_1_dt[is.na(POP_W),POP_W := w[is.na(int_1$POP_W),] %>% apply(1,which) %>% sapply(function(x){mean(int_1$POP_W[x],na.rm=T)})]
      # }
      # Aggregate
      j0 <- 1
      a1_char <- lapply(seq_along(char_varz),function(j0){
        int_1_pw <- int_1_dt[,list(w = get(char_varz[j0])[which.max(POP_W)]),by=ADM1_CODE] %>% setnames("w",paste0(char_varz[j0],"_pw"))
        int_1_aw <- int_1_dt[,list(w = get(char_varz[j0])[which.max(AREA_W)]),by=ADM1_CODE] %>% setnames("w",paste0(char_varz[j0],"_aw"))
        int_1_ <- int_1_pw %>% bind_cols(int_1_aw %>% select(-ADM1_CODE))
        if(j0>1){int_1_ <- int_1_ %>% select(-ADM1_CODE)}
        int_1_
        }) %>% bind_cols()
      a1_sums <- lapply(seq_along(sum_varz),function(j0){
        int_1_pw <- int_1_dt[,list(w = sum(get(sum_varz[j0])*POP_W,na.rm=T)),by=ADM1_CODE] %>% setnames("w",paste0(sum_varz[j0],"_pw"))
        int_1_aw <- int_1_dt[,list(w = sum(get(sum_varz[j0])*AREA_W,na.rm=T)),by=ADM1_CODE] %>% setnames("w",paste0(sum_varz[j0],"_aw"))
        int_1_ <- int_1_pw %>% bind_cols(int_1_aw %>% select(-ADM1_CODE))
        if(j0>1){int_1_ <- int_1_ %>% select(-ADM1_CODE)}
        int_1_
        }) %>% bind_cols()
      a1_means <- lapply(seq_along(mean_varz),function(j0){
        int_1_pw <- int_1_dt[,list(w = weighted.mean(get(mean_varz[j0]),POP_W,na.rm=T)),by=ADM1_CODE] %>% setnames("w",paste0(mean_varz[j0],"_pw"))
        int_1_aw <- int_1_dt[,list(w = weighted.mean(get(mean_varz[j0]),AREA_W,na.rm=T)),by=ADM1_CODE] %>% setnames("w",paste0(mean_varz[j0],"_aw"))
        int_1_ <- int_1_pw %>% bind_cols(int_1_aw %>% select(-ADM1_CODE))
        if(j0>1){int_1_ <- int_1_ %>% select(-ADM1_CODE)}
        int_1_
        }) %>% bind_cols()
      a1 <- merge(map1_,a1_char,by="ADM1_CODE")
      a1 <- merge(a1,a1_means,by="ADM1_CODE")
      a1 <- merge(a1,a1_sums,by="ADM1_CODE")
      # ADM2
      int_2 <- suppressMessages(
        st_intersection(geo_vor,map2_)
        )
      # Fix weird Canada geometry issue
      if(int_2 %>% st_geometry_type() %>% grepl("GEOMETRY|POINT|LINE",.) %>% sum() > 0){
        # int_2 <- int_2[(int_2 %>% st_geometry_type() %>% grepl("GEOMETRY",.)),] %>% st_collection_extract(type="POLYGON") %>% ms_simplify(.,keep=.005) %>% fix_geom(self_int=FALSE) %>% st_union(int_2[!(int_2 %>% st_geometry_type() %>% grepl("GEOMETRY",.)),]) 
        int_2 <- int_2[!(int_2 %>% st_geometry_type() %>% grepl("GEOMETRY|POINT|LINE",.)),] 
        format(object.size(int_2),units="Mb")
      }      

      # png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/000_test1.png"),width=30,height=10,units="in",res=150); par(mar=c(0,0,0,0),mfrow=c(1,3)); plot(geo_vor$geometry); plot(int_2$geometry); plot(map2_$geometry); dev.off()
      int_2$POP_INT <- raster::extract(r_crop,int_2 %>% as("Spatial"),fun=sum)
      int_2$POP_W <- int_2$POP_INT/int_2$POP_TOTAL
      int_2$AREA_INT <- st_area(int_2) %>% as.numeric()
      int_2$AREA_W <- int_2$AREA_INT/int_2$AREA_TOTAL
      # Convert to dt
      int_2_dt <- data.table(int_2)
       # Interpolate missing values
      if(sum(is.na(int_2_dt$POP_INT))==1){
        w <- pointDistance(int_2 %>% st_centroid() %>% st_geometry() %>% unlist() %>% matrix(ncol=2,byrow=T) %>% as.data.frame() %>% setNames(c("lon","lat")),lonlat=T) %>% as.dist() %>% as.matrix()
        diag(w) <- NA
        int_2_dt[is.na(POP_INT),POP_INT := int_2_dt$POP_INT[t(apply(w, 1, order)[ 1:min(10,nrow(int_2)), is.na(int_2$POP_INT)])] %>% mean(na.rm=T)]
        int_2_dt[is.na(POP_W),POP_W := POP_INT/POP_TOTAL]
      }
      if(sum(is.na(int_2_dt$POP_INT))>1){
        w <- pointDistance(int_2 %>% st_centroid() %>% st_geometry() %>% unlist() %>% matrix(ncol=2,byrow=T) %>% as.data.frame() %>% setNames(c("lon","lat")),lonlat=T) %>% as.dist() %>% as.matrix()
        diag(w) <- NA
        int_2_dt[is.na(POP_INT),POP_INT := t(apply(w, 1, order)[ 1:min(10,nrow(int_2)), is.na(int_2$POP_INT)]) %>% apply(1,function(.){mean(int_2$POP_INT[.],na.rm=T)})]
        int_2_dt[is.na(POP_W),POP_W := POP_INT/POP_TOTAL]

      }
      # if(sum(is.na(int_2_dt$POP_W))>1){
      #   w <- st_intersects(int_2,int_2) %>% as.matrix()
      #   int_2_dt[is.na(POP_W),POP_W := w[is.na(int_2$POP_W),] %>% apply(1,which) %>% sapply(function(x){mean(int_2$POP_W[x],na.rm=T)})]
      # }
      # Aggregate
      a2_char <- lapply(seq_along(char_varz),function(j0){
        int_2_pw <- int_2_dt[,list(w = get(char_varz[j0])[which.max(POP_W)]),by=ADM2_CODE] %>% setnames("w",paste0(char_varz[j0],"_pw"))
        int_2_aw <- int_2_dt[,list(w = get(char_varz[j0])[which.max(AREA_W)]),by=ADM2_CODE] %>% setnames("w",paste0(char_varz[j0],"_aw"))
        int_2_ <- int_2_pw %>% bind_cols(int_2_aw %>% select(-ADM2_CODE))
        if(j0>1){int_2_ <- int_2_ %>% select(-ADM2_CODE)}
        int_2_
        }) %>% bind_cols()
      a2_sums <- lapply(seq_along(sum_varz),function(j0){
        int_2_pw <- int_2_dt[,list(w = sum(get(sum_varz[j0])*POP_W,na.rm=T)),by=ADM2_CODE] %>% setnames("w",paste0(sum_varz[j0],"_pw"))
        int_2_aw <- int_2_dt[,list(w = sum(get(sum_varz[j0])*AREA_W,na.rm=T)),by=ADM2_CODE] %>% setnames("w",paste0(sum_varz[j0],"_aw"))
        int_2_ <- int_2_pw %>% bind_cols(int_2_aw %>% select(-ADM2_CODE))
        if(j0>1){int_2_ <- int_2_ %>% select(-ADM2_CODE)}
        int_2_
        }) %>% bind_cols()
      a2_means <- lapply(seq_along(mean_varz),function(j0){
        int_2_pw <- int_2_dt[,list(w = weighted.mean(get(mean_varz[j0]),POP_W,na.rm=T)),by=ADM2_CODE] %>% setnames("w",paste0(mean_varz[j0],"_pw"))
        int_2_aw <- int_2_dt[,list(w = weighted.mean(get(mean_varz[j0]),AREA_W,na.rm=T)),by=ADM2_CODE] %>% setnames("w",paste0(mean_varz[j0],"_aw"))
        int_2_ <- int_2_pw %>% bind_cols(int_2_aw %>% select(-ADM2_CODE))
        if(j0>1){int_2_ <- int_2_ %>% select(-ADM2_CODE)}
        int_2_
        }) %>% bind_cols()
      a2 <- merge(map2_,a2_char,by="ADM2_CODE")
      a2 <- merge(a2,a2_means,by="ADM2_CODE")
      a2 <- merge(a2,a2_sums,by="ADM2_CODE")

      # Save 
      save(geo_vor,a1,a2,file=paste0("Elections/CLEA/Processed/GAUL_Voronoi/GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      for(plot_var in c(mean_varz)){#print(plot_var)
        tryCatch({
          if(geo_vor[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_voronoi.png"),width=4,height=4,units="in",res=150)
            plot(geo_vor[plot_var],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_gaul1.png"),width=4,height=4,units="in",res=150)
            plot(a1[paste0(plot_var,"_pw")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_gaul1.png"),width=4,height=4,units="in",res=150)
            plot(a1[paste0(plot_var,"_aw")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(a2[paste0(plot_var,"_pw")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(a2[paste0(plot_var,"_aw")],lwd=.25)
            dev.off()
          }
         },error=function(e){})
      }

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished Voronoi"))

    ###########################
    # Kriging
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GAUL_Kriging/"))){

      # Load GAUL polygons
      load(paste0(temp,"/GAUL_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gaul)){
        t1_ <- Sys.time()
        map0_ <- ms_simplify(map0_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom()
        map1_ <- ms_simplify(map1_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom()
        map2_ <- ms_simplify(map2_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom()
        t2_ <- Sys.time()
        print(t2_-t1_)
      }

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf %>% st_jitter(),st_bbox(map0_) )
        )

      # Find optimal projection for map
      # epsg_mdb <- mdb.get("EPSG/EPSG_v9_8_6.mdb")
      # epsg_table <- epsg_mdb[[5]] %>% as.data.table()
      # save(epsg_table,file="EPSG/EPSG_mdb_5.RData")
      # load("EPSG/EPSG_mdb_5.RData")
      epsg_poly <- read_sf("EPSG","EPSG_Polygons")
      epsg_candidates <- suppressMessages(
        st_within(cst_sf,epsg_poly) %>% as.data.table()
        )
      epsg_candidates <- epsg_candidates[,col.id] %>% table() %>% sort(decreasing=F) %>% names() %>% as.numeric()
      epsg_mat <- lapply(seq_along(epsg_candidates),function(e0){
        area_1 <- map0_$geometry %>% st_area() %>% as.numeric()
        area_2 <- epsg_poly[epsg_candidates[e0],] %>% st_area() %>% as.numeric()
        area_ix <- suppressMessages(
          map0_$geometry %>% st_intersection(epsg_poly[epsg_candidates[e0],]) %>% st_area() %>% as.numeric()
          )
        data.frame(
          overlap_1 = area_ix/area_1,
          overlap_2 = area_ix/area_2,
          area_code = epsg_poly[epsg_candidates[e0],] %>% as.data.table() %>% select(AREA_CODE) %>% unlist(),
          area_name = epsg_poly[epsg_candidates[e0],] %>% as.data.table() %>% select(AREA_NAME) %>% unlist(),
          region = epsg_poly[epsg_candidates[e0],] %>% as.data.table() %>% select(REGION) %>% unlist(),
          stringsAsFactors = FALSE
        ) %>% as.data.table()
      }) %>% bind_rows()
      # ,mc.cores=detectCores()-1
      epsg_mat <- epsg_mat[overlap_1<=1&overlap_2<=1,]
      epsg_dist <- epsg_mat[,.(overlap_1,overlap_2)] %>% as.matrix() %>% rbind(c(1,1),.) %>% dist() %>% as.matrix() 
      epsg_best <- epsg_mat[epsg_dist[-1,1] %>% order(),area_code]
      # epsg_best <- epsg_mat[epsg_dist[-1,1] %>% which.min(),area_code]
      epsg_best <- lapply(epsg_best,function(x){st_crs(x)$epsg}) %>% unlist() %>% na.omit()
      epsg_best <- lapply(epsg_best,function(x){st_crs(x)$epsg}) %>% unlist() %>% na.omit()
      crs_bad <- TRUE; x0 <- 1
      while(crs_bad&x0<=length(epsg_best)){
        epsg_best_ <- epsg_best[x0]
        cst_sf_transform <- cst_sf %>% st_transform(crs=epsg_best[x0])
        crs_bad <- (cst_sf %>% st_transform(crs=epsg_best[x0]) %>% st_bbox() %>% as.numeric() %>% is.na() %>% mean())==1 | st_is_longlat(cst_sf_transform)
        x0 <- x0+1
      }
      cst_sf_transform
        
      # Create regular grid
      cst_grid <- st_make_grid(map0_ %>% st_transform(crs=epsg_best_),cellsize=25000,what="centers")
      if(length(cst_grid[map0_ %>% st_transform(crs=epsg_best_)])==1){cst_grid <- st_make_grid(map0_ %>% st_transform(crs=epsg_best_),n=25,what="centers")}
      # cst_grid <- cst_grid[map0_ %>% st_transform(crs=epsg_best_)]
      
      # plot(cst_grid)
      # plot(map0_ %>% st_transform(crs=epsg_best_) %>% st_geometry(),add=T)


      # Ordinary kriging
      krig_list <- lapply(seq_along(mean_varz),function(v0){
        # print(paste0(v0,"/",length(mean_varz)))
        yvar <- mean_varz[v0]
        a0 <- data.frame(x = NA, x_sd = NA, stringsAsFactors = F) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
        a1 <- data.frame(x = rep(NA,nrow(map1_)), x_sd = rep(NA,nrow(map1_)), stringsAsFactors = F) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
        a2 <- data.frame(x = rep(NA,nrow(map2_)), x_sd = rep(NA,nrow(map2_)), stringsAsFactors = F) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
        if(cst_sf_transform[yvar] %>% na.omit() %>% nrow() > 2 & cst_sf_transform %>% as.data.table() %>% select(yvar) %>% unlist() %>% unique() %>% length() > 2){
          krig_form <- as.formula(paste0(yvar,"~1"))
          krig_out <- suppressMessages(
            autoKrige(krig_form, as(cst_sf_transform[yvar] %>% na.omit(),"Spatial"), new_data=as(cst_grid,"Spatial"))
            )
          krig_pix <- SpatialPixelsDataFrame(krig_out$krige_output %>% coordinates(),data = krig_out$krige_output %>% as.data.frame())
          # krig_pix <- SpatialPixelsDataFrame(krig_out$krige_output %>% coordinates(),data = krig_out$krige_output %>% as.data.frame())          
          proj4string(krig_pix) <- st_crs(epsg_best_)$proj4string
          krig_fit <- krig_pix[,"var1.pred"] %>% raster() %>% projectRaster( crs = proj4string(as(map0_,"Spatial"))) 
          krig_sd <- krig_pix[,"var1.stdev"] %>% raster() %>% projectRaster( crs = proj4string(as(map0_,"Spatial"))) 
          # krig_fit <- krig_out$krige_output[,"var1.pred"] %>% raster() %>% projectRaster( crs = proj4string(as(map0_,"Spatial")))        
          # krig_sd <- krig_out$krige_output[,"var1.stdev"] %>% raster() %>% projectRaster( crs = proj4string(as(map0_,"Spatial")))        
          a0 <- data.frame(
            x = raster::extract(krig_fit,map0_ %>% as("Spatial"),fun=mean,na.rm=T),
            x_sd = raster::extract(krig_sd,map0_ %>% as("Spatial"),fun=mean,na.rm=T),
            stringsAsFactors = FALSE
            ) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
          a1 <- data.frame(
            x = raster::extract(krig_fit,map1_ %>% as("Spatial"),fun=mean,na.rm=T),
            x_sd = raster::extract(krig_sd,map1_ %>% as("Spatial"),fun=mean,na.rm=T),
            stringsAsFactors = FALSE
            ) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
          a2 <- data.frame(
            x = raster::extract(krig_fit,map2_ %>% as("Spatial"),fun=mean,na.rm=T),
            x_sd = raster::extract(krig_sd,map2_ %>% as("Spatial"),fun=mean,na.rm=T),
            stringsAsFactors = FALSE
            ) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
          }
        list(a0,a1,a2)
        })
      # ,mc.cores=detectCores()/2

      a0 <- krig_list %>% sapply(.,function(x){x[[1]]}) %>% bind_cols() %>% as.data.table() %>% setnames(1:(2*length(mean_varz)),paste0(rep(mean_varz,each=2),c("_kr","_kr_sd")))
      a1 <- krig_list %>% sapply(.,function(x){x[[2]]}) %>% bind_cols() %>% as.data.table() %>% setnames(1:(2*length(mean_varz)),paste0(rep(mean_varz,each=2),c("_kr","_kr_sd")))
      a2 <- krig_list %>% sapply(.,function(x){x[[3]]}) %>% bind_cols() %>% as.data.table() %>% setnames(1:(2*length(mean_varz)),paste0(rep(mean_varz,each=2),c("_kr","_kr_sd")))
      a0 <- map0_ %>% bind_cols(a0)
      a1 <- map1_ %>% bind_cols(a1)
      a2 <- map2_ %>% bind_cols(a2)

      # Save 
      save(a0,a1,a2,file=paste0("Elections/CLEA/Processed/GAUL_Kriging/GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      for(plot_var in c(mean_varz)){#print(plot_var)
        tryCatch({
          if(a1[paste0(plot_var,"_kr")] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GAUL_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul1.png"),width=4,height=4,units="in",res=150)
            plot(a1[paste0(plot_var,"_kr")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(a2[paste0(plot_var,"_kr")],lwd=.25)
            dev.off()
          }
        },error=function(e){})
      }

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished Kriging"))


  ################################
  # Close country loop
  ################################

  },error=function(e){print(paste0(clea_cntz[k0]," ",yrz[y0]," ERROR"))})

  t2 <- Sys.time()
  print(t2-t1)

  },mc.cores = min(length(clea_cntz),detectCores()/2))
# ,mc.cores = min(length(clea_cntz),detectCores()-1)

################################
# Close year loop
################################

# Delete temporary GAUL files
for(f0 in dir(temp)[grep(paste0(yrz[y0],".RData$"),dir(temp))]){
  file.remove(paste0(temp,"/",f0))
}

}
