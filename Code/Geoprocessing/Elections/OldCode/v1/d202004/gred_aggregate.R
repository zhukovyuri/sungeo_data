# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/gred_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/gred_aggregate.R")' &
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
                      "Rfast",
                      "automap")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)



###############################################
###############################################
###############################################
## GRED
###############################################
###############################################
###############################################

# List of countries with broken GAUL geometries
fix_gred <- c("ZZZ")

# List of countries to skip
skip_gred <- c("ZZZ")

# List of countries to simplify polygons
simp_gred <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

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
load("Elections/CLEA/GeoCode/cst_best_2.RData")
# Add country codes
geo_mat[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat

# Load Natural Earth boundaries
con_ne <- unzip(zipfile = "Admin/NE/ne_10m_admin_0_countries.zip", exdir = temp)

# Loop over years
y0 <- 15
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
  save_list <- mclapply(seq_along(clea_cntz),function(k0){
    # cat(round(k0/length(clea_cntz),2),"\r")
    clea_ <- clea_full[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]
    save(clea_,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
  },mc.cores=min(length(clea_cntz),detectCores()-1))
  dir(temp)
  rm(save_list,clea_full)
  gc(reset = TRUE)


  ###########################
  # Loop over countries
  ###########################
  k0 <- 2

  cntz_list <- mclapply(seq_along(clea_cntz),function(k0){

    t1 <- Sys.time()
    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)))

    tryCatch({

    ###########################
    # Extract variables
    ###########################

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
    geo_sub <- geo_mat[(ISO3==clea_cntz[k0]) & (cst_n %in% clea_i[,cst_n]) & (yr %in% yrz[y0]),]
    clea_mat <- merge(clea_mat,geo_sub %>% select(-cst_n),by=c("ctr_n","cst"))
    # clea_mat

    # Select variables
    char_varz <- c("cst_n","cst","yr","yrmo","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
    mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
    sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")

    ###########################
    # Direct merge
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GRED/"))){

      # Load GRED polygons
      gred <- read_sf(paste0(temp,"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f2],"/",yrz[y0],"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f4],".shp"))
      gred

      # png(paste0("Elections/CLEA/Maps/GRED/000_test1.png"),width=10,height=10,units="in",res=150); par(mar=c(0,0,0,0),mfrow=c(1,1)); plot(gred$geometry);dev.off()

      # Merge
      gred_mat <- merge(gred,clea_mat %>% select(-ctr_n,-ctr,-cst_n),by=c("cst"),all.x=T,all.y=F) %>% st_transform(4326)

      gred_mat
      nrow(gred)
      nrow(gred_mat)
      nrow(clea_mat)

      # Save 
      save(gred_mat,file=paste0("Elections/CLEA/Processed/GRED/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      for(plot_var in c(mean_varz)){
        tryCatch({
          if(gred_mat[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gred.png"),width=4,height=4,units="in",res=150)
            plot(gred_mat[paste0(plot_var)],lwd=.25)
            dev.off()
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
      gred$o0 <- row.names(gred)

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gred)){
        t1_ <- Sys.time()
        gred <- ms_simplify(gred,keep=simp_gred[clea_cntz[k0]]) %>% fix_geom()
        t2_ <- Sys.time()
        print(t2_-t1_)
      }

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(gred))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(gred))
        )
      
      # Find matching polygons for each point
      o0_ <- suppressMessages(
        st_within(cst_sf,gred) %>% as.data.table()
        )
      clea_mat_pip[o0_$row.id,o0 := o0_$col.id]

      # Aggregate
      gred_char <- lapply(seq_along(char_varz),function(j0){
        int_1_ <- clea_mat_pip[,list(w = get(char_varz[j0]) %>% unique() %>% paste0(.,collapse="|")),by=o0] %>% setnames("w",paste0(char_varz[j0]))
        if(j0>1){int_1_ <- int_1_ %>% select(-o0)}
        int_1_
        }) %>% bind_cols()
      gred_sums <- lapply(seq_along(sum_varz),function(j0){
        int_1_ <- clea_mat_pip[,list(w = sum(get(sum_varz[j0]),na.rm=T)),by=o0] %>% setnames("w",paste0(sum_varz[j0]))
        if(j0>1){int_1_ <- int_1_ %>% select(-o0)}
        int_1_
        }) %>% bind_cols()
      gred_means <- lapply(seq_along(mean_varz),function(j0){
        int_1_ <- clea_mat_pip[,list(w = mean(get(mean_varz[j0]),na.rm=T)),by=o0] %>% setnames("w",paste0(mean_varz[j0]))
        if(j0>1){int_1_ <- int_1_ %>% select(-o0)}
        int_1_
        }) %>% bind_cols()
      gred_mat <- merge(gred,gred_char,by="o0",all.x=T,all.y=F)
      gred_mat <- merge(gred_mat,gred_means,by="o0",all.x=T,all.y=F)
      gred_mat <- merge(gred_mat,gred_sums,by="o0",all.x=T,all.y=F)
      
      # Save 
      save(gred_mat,file=paste0("Elections/CLEA/Processed/GRED_PointInPoly/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      png(paste0("Elections/CLEA/Maps/GRED_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_cst.png"),width=4,height=4,units="in",res=150)
        plot(gred$geometry,lwd=.25)
        points(as(cst_sf,"Spatial"))
      dev.off()
      for(plot_var in c(mean_varz)){
        tryCatch({
          if(gred_mat[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gred1.png"),width=4,height=4,units="in",res=150)
            plot(gred_mat[paste0(plot_var)],lwd=.25)
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
      format(object.size(gred),units="Mb")

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gred)){
        t1_ <- Sys.time()
        gred <- ms_simplify(gred,keep=simp_gred[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        t2_ <- Sys.time()
        print(t2_-t1_)
      }
      format(object.size(gred),units="Mb")
 
      # Buffer 
      map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")
      map0_ne_ <- suppressMessages(
        map0_ne[map0_ne$ISO3%in%clea_cntz[k0],] %>% st_buffer(dist=1)
        )

      # Voronoi polygons
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(gred))
      cst_sf_crop <-  suppressMessages(
        st_crop(cst_sf %>% st_jitter(),st_bbox(map0_ne_))
        )
      cst_geom_crop <- cst_sf_crop  %>% st_geometry() %>% st_union()
      geo_vor <- suppressMessages(
        st_voronoi(cst_geom_crop) %>% st_cast() %>% st_as_sf() %>% st_intersection(map0_ne_$geometry) %>% setnames("x","geometry")
      )
      if(nrow(cst_sf_crop)==1){
        geo_vor <- suppressMessages(
          map0_ne_ %>% st_geometry() %>% st_as_sf() %>% st_intersection(map0_ne_$geometry) %>% setnames("x","geometry")
          )
      }
      st_geometry(geo_vor) <- "geometry"

      # Combine with attributes
      int <- suppressMessages(
        st_intersects(geo_vor,cst_sf_crop) %>% as.data.frame()
        )
      geo_vor <- bind_cols(geo_vor  %>% slice(int$row.id),cst_sf_crop %>% as.data.frame() %>% select(-geometry) %>% slice(int$col.id)) 

      # png(paste0("Elections/CLEA/Maps/Voronoi_GRED/000_test1.png"),width=20,height=10,units="in",res=150); par(mar=c(0,0,0,0),mfrow=c(1,2)); plot(geo_vor$geometry); plot(gred$geometry); dev.off()

      # Crop raster by country extent
      b <- as(extent(st_bbox(map0_ne_)[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
      crs(b) <- crs(r)
      b <- st_as_sf(b) 
      r_crop <- crop(r, b)

      # Area and pop-weighted interpolation 
      geo_vor <-  suppressMessages(
        st_intersection(geo_vor,map0_ne[map0_ne$ISO3%in%clea_cntz[k0],])
        )
      geo_vor$POP_TOTAL <- raster::extract(r_crop,geo_vor %>% as("Spatial"),fun=sum,na.rm=T)
      geo_vor$AREA_TOTAL <- st_area(geo_vor) %>% as.numeric()
      int_1 <- suppressMessages(
        st_intersection(geo_vor,gred)
        )
      # Fix weird geometry issue
      if(int_1 %>% st_geometry_type() %>% grepl("GEOMETRY",.) %>% sum() > 0){
        int_1 <- int_1[!(int_1 %>% st_geometry_type() %>% grepl("GEOMETRY",.)),] 
        format(object.size(int_1),units="Mb")
      }      

      png(paste0("Elections/CLEA/Maps/GRED_Voronoi/000_test1.png"),width=30,height=10,units="in",res=150); par(mar=c(0,0,0,0),mfrow=c(1,3)); plot(geo_vor$geometry); plot(int_1$geometry); plot(gred$geometry); dev.off()

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
      gred_char <- lapply(seq_along(char_varz),function(j0){
        int_1_pw <- int_1_dt[,list(w = get(char_varz[j0])[which.max(POP_W)]),by=cst] %>% setnames("w",paste0(char_varz[j0],"_pw"))
        int_1_aw <- int_1_dt[,list(w = get(char_varz[j0])[which.max(AREA_W)]),by=cst] %>% setnames("w",paste0(char_varz[j0],"_aw"))
        int_1_ <- int_1_pw %>% bind_cols(int_1_aw %>% select(-cst))
        if(j0>1){int_1_ <- int_1_ %>% select(-cst)}
        int_1_
        }) %>% bind_cols()
      gred_sums <- lapply(seq_along(sum_varz),function(j0){
        int_1_pw <- int_1_dt[,list(w = sum(get(sum_varz[j0])*POP_W,na.rm=T)),by=cst] %>% setnames("w",paste0(sum_varz[j0],"_pw"))
        int_1_aw <- int_1_dt[,list(w = sum(get(sum_varz[j0])*AREA_W,na.rm=T)),by=cst] %>% setnames("w",paste0(sum_varz[j0],"_aw"))
        int_1_ <- int_1_pw %>% bind_cols(int_1_aw %>% select(-cst))
        if(j0>1){int_1_ <- int_1_ %>% select(-cst)}
        int_1_
        }) %>% bind_cols()
      gred_means <- lapply(seq_along(mean_varz),function(j0){
        int_1_pw <- int_1_dt[,list(w = weighted.mean(get(mean_varz[j0]),POP_W,na.rm=T)),by=cst] %>% setnames("w",paste0(mean_varz[j0],"_pw"))
        int_1_aw <- int_1_dt[,list(w = weighted.mean(get(mean_varz[j0]),AREA_W,na.rm=T)),by=cst] %>% setnames("w",paste0(mean_varz[j0],"_aw"))
        int_1_ <- int_1_pw %>% bind_cols(int_1_aw %>% select(-cst))
        if(j0>1){int_1_ <- int_1_ %>% select(-cst)}
        int_1_
        }) %>% bind_cols()
      gred_mat <- merge(gred,gred_char,by="cst")
      gred_mat <- merge(gred_mat,gred_means,by="cst")
      gred_mat <- merge(gred_mat,gred_sums,by="cst")

     
      # Save 
      save(geo_vor,gred_mat,file=paste0("Elections/CLEA/Processed/GRED_Voronoi/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      for(plot_var in c(mean_varz)){#print(plot_var)
        tryCatch({
          if(geo_vor[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_voronoi.png"),width=4,height=4,units="in",res=150)
            plot(geo_vor[plot_var],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_gred.png"),width=4,height=4,units="in",res=150)
            plot(gred_mat[paste0(plot_var,"_pw")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GRED_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_gred.png"),width=4,height=4,units="in",res=150)
            plot(gred_mat[paste0(plot_var,"_aw")],lwd=.25)
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

    if(!skip_existing|(skip_existing&!paste0("GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GRED_Kriging/"))){

      # Load GRED polygons
      gred <- read_sf(paste0(temp,"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f2],"/",yrz[y0],"/",sub_dirz[iso3%in%clea_cntz[k0]&yr%in%yrz[y0],f4],".shp")) %>% st_transform(4326)
      format(object.size(gred),units="Mb")

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gred)){
        t1_ <- Sys.time()
        gred <- ms_simplify(gred,keep=simp_gred[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        t2_ <- Sys.time()
        print(t2_-t1_)
      }
      format(object.size(gred),units="Mb")

      # Country borders
      map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")
      map0_ <- map0_ne[map0_ne$ISO3%in%clea_cntz[k0],] %>% st_transform(4326)

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
      v0 <- 1
      krig_mat <- lapply(seq_along(mean_varz),function(v0){
        # print(paste0(v0,"/",length(mean_varz)))
        yvar <- mean_varz[v0]
        a1 <- data.frame(x = rep(NA,nrow(gred)), x_sd = rep(NA,nrow(gred)), stringsAsFactors = F) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
        if(cst_sf_transform[yvar] %>% na.omit() %>% nrow() > 2 & cst_sf_transform %>% as.data.table() %>% select(yvar) %>% unlist() %>% unique() %>% length() > 2){
          print(paste0("Krige ",clea_cntz[k0]," ",v0,"/",length(mean_varz)))
          krig_form <- as.formula(paste0(yvar,"~1"))
          krig_out <- suppressMessages(
            autoKrige(krig_form, as(cst_sf_transform[yvar] %>% na.omit(),"Spatial"), new_data=as(cst_grid,"Spatial"))
            )
          krig_pix <- SpatialPixelsDataFrame(krig_out$krige_output %>% coordinates(),data = krig_out$krige_output %>% as.data.frame())
          proj4string(krig_pix) <- st_crs(epsg_best_)$proj4string
          krig_fit <- krig_pix[,"var1.pred"] %>% raster() %>% projectRaster( crs = proj4string(as(map0_,"Spatial"))) 
          krig_sd <- krig_pix[,"var1.stdev"] %>% raster() %>% projectRaster( crs = proj4string(as(map0_,"Spatial"))) 
          a1 <- data.frame(
            x = raster::extract(krig_fit,gred %>% as("Spatial"),fun=mean,na.rm=T),
            x_sd = raster::extract(krig_sd,gred %>% as("Spatial"),fun=mean,na.rm=T),
            stringsAsFactors = FALSE
            ) %>% as.data.table() %>% setnames(old=c("x","x_sd"),new=c(mean_varz[v0],paste0(mean_varz[v0],"_sd")))
          }
          a1
        }) %>% bind_cols() %>% as.data.table() %>% setnames(1:(2*length(mean_varz)),paste0(rep(mean_varz,each=2),c("_kr","_kr_sd")))
      # ,mc.cores=detectCores()/2
      krig_mat
      gred_mat <- gred %>% bind_cols(krig_mat)
      
      # Save 
      save(gred_mat,file=paste0("Elections/CLEA/Processed/GRED_Kriging/GRED_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      for(plot_var in c(mean_varz)){#print(plot_var)
        tryCatch({
          if(gred_mat[paste0(plot_var,"_kr")] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GRED_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gred.png"),width=4,height=4,units="in",res=150)
            plot(gred_mat[paste0(plot_var,"_kr")],lwd=.25)
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





