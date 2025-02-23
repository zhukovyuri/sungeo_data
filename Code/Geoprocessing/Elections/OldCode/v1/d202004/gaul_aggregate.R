# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/gaul_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/gaul_aggregate.R")' &
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
load("Elections/CLEA/GeoCode/cst_best_2.RData")
# Add country codes
geo_mat[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat

# Load Natural Earth boundaries
con_ne <- unzip(zipfile = "Admin/NE/ne_10m_admin_0_countries.zip", exdir = temp)
map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")

# Loop over years
y0 <- 1
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
  k0 <- 1
  cntz_list <- mclapply(seq_along(clea_cntz),function(k0){

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
    clea_mat <- merge(clea_mat,geo_sub %>% select(-cst_n),by=c("ctr_n","cst"))

    # Select variables
    char_varz <- c("cst_n","cst","yr","yrmo","noncontested","nincumb_pty_n","nincumb_pty","incumb_pty_n","incumb_pty","incumb_can","win1_pty_n","win1_pty","win1_can","win2_pty_n","win2_pty","win2_can")
    mean_varz <- c("to1","cvs1_margin","pvs1_margin","pvs1_margin_incumb","pvs1_margin_nincumb","vs1_margin","to2","cvs2_margin","pvs2_margin","vs2_margin","win_pvs1","win_cvs1","pvs1_nincumb","cvs1_incumb","pvs1_incumb","contest_p1_nincumb","contest_p1","contest_c1","comptop2_c1","comptop2_p1","comptop1_c1","comptop1_p1")
    sum_varz <- c("pev1","vot1","vv1","cv1_margin","pv1_margin","v1_margin","pev2","vot2","vv2","cv2_margin","pv2_margin","v2_margin")


    ###########################
    # Point-in-polygon method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GAUL_PointInPoly/"))){

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
        st_crop(cst_sf,st_bbox(map0_))
        )

      # Aggregate over polygons
      clea_chars_0 <- pinp_fun(polyz=map0_,pointz=cst_sf,varz=char_varz,funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_0 <- pinp_fun(polyz=map0_,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_0 <- pinp_fun(polyz=map0_,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)
      clea_chars_1 <- pinp_fun(polyz=map1_,pointz=cst_sf,varz=char_varz,funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_1 <- pinp_fun(polyz=map1_,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_1 <- pinp_fun(polyz=map1_,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)
      clea_chars_2 <- pinp_fun(polyz=map2_,pointz=cst_sf,varz=char_varz,funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_2 <- pinp_fun(polyz=map2_,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_2 <- pinp_fun(polyz=map2_,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)

      # Merge
      idvar <- "ADM0_CODE"
      clea_gaul0 <- map0_ %>% 
        merge(clea_chars_0 %>% as.data.table() %>% select(c(idvar,char_varz)),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_0 %>% as.data.table() %>% select(c(idvar,mean_varz)),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_0 %>% as.data.table() %>% select(c(idvar,sum_varz)),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      idvar <- "ADM1_CODE"
      clea_gaul1 <- map1_ %>% 
        merge(clea_chars_1 %>% as.data.table() %>% select(c(idvar,char_varz)),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_1 %>% as.data.table() %>% select(c(idvar,mean_varz)),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_1 %>% as.data.table() %>% select(c(idvar,sum_varz)),by=idvar,all.x=T,all.y=F) %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      idvar <- "ADM2_CODE"
      clea_gaul2 <- map2_ %>% 
        merge(clea_chars_2 %>% as.data.table() %>% select(c(idvar,char_varz)),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_2 %>% as.data.table() %>% select(c(idvar,mean_varz)),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_2 %>% as.data.table() %>% select(c(idvar,sum_varz)),by=idvar,all.x=T,all.y=F) %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      summary(clea_gaul1)

      # Save 
      save(clea_gaul0,clea_gaul1,clea_gaul2,file=paste0("Elections/CLEA/Processed/GAUL_PointInPoly/GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/GAUL_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_000.png"),width=4,height=4,units="in",res=150)
          plot(clea_gaul0$geometry,lwd=.25)
          points(as(cst_sf,"Spatial"))
        dev.off()
        for(plot_var in toupper(mean_varz)){
          if(clea_gaul1[plot_var] %>% na.omit() %>% nrow() > 0){
            clea_sf <- clea_gaul1 %>% st_as_sf()
            png(paste0("Elections/CLEA/Maps/GAUL_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul1.png"),width=4,height=4,units="in",res=150)
            plot(clea_sf[paste0(plot_var)],lwd=.25)
            dev.off()
            clea_sf <- clea_gaul2 %>% st_as_sf()
            png(paste0("Elections/CLEA/Maps/GAUL_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(clea_sf[paste0(plot_var)],lwd=.25)
            dev.off()
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished PointInPoly"))

    # Clean workspace
    rm(list=ls()[grep("clea_gaul|_char|_means|_sums|cst_sf|^map",ls())])
    gc(reset=T)


    ###########################
    # Voronoi method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/GAUL_Voronoi/"))){

      # Load GAUL polygons
      load(paste0(temp,"/GAUL_",clea_cntz[k0],"_",yrz[y0],".RData"))
      # list(map0_,map1_,map2_) %>% sapply(function(.){format(object.size(.),units="Mb")})

      # Simplify polygons
      if(clea_cntz[k0]%in%names(simp_gaul)){
        t1_ <- Sys.time()
        map0_ <- ms_simplify(map0_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        map1_ <- ms_simplify(map1_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        map2_ <- ms_simplify(map2_,keep=simp_gaul[clea_cntz[k0]]) %>% fix_geom(self_int=FALSE)
        t2_ <- Sys.time()
        print(t2_-t1_)
      }
      # list(map0_,map1_,map2_) %>% sapply(function(.){format(object.size(.),units="Mb")})

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )
      
      # Tesselation
      clea_gaul0 <- tess_fun(
        pointz=cst_sf,
        polyz=map0_,
        poly_id="ADM0_CODE",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        varz=list(sum_varz,mean_varz),
        char_varz=char_varz,
        funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
        return_tess=TRUE
        )
      clea_gaul1 <- tess_fun(
        pointz=cst_sf,
        polyz=map1_,
        poly_id="ADM1_CODE",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        varz=list(sum_varz,mean_varz),
        char_varz=char_varz,
        funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
        return_tess=TRUE
        )
      clea_gaul2 <- tess_fun(
        pointz=cst_sf,
        polyz=map2_,
        poly_id="ADM2_CODE",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        varz=list(sum_varz,mean_varz),
        char_varz=char_varz,
        funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
        return_tess=TRUE
        )
      clea_gaul2

      # Rename variables
      geo_vor <- clea_gaul0[["tess"]] %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_gaul0 <- clea_gaul0[["result"]] %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME",paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2",paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng","yr_pw","yrmo_pw","noncontested_pw")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_gaul1 <- clea_gaul1[["result"]] %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME",paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2",paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng","yr_pw","yrmo_pw","noncontested_pw")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())
      clea_gaul2 <- clea_gaul2[["result"]] %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME",paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2",paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng","yr_pw","yrmo_pw","noncontested_pw")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper())

      # Save 
      save(geo_vor,clea_gaul0,clea_gaul1,clea_gaul2,file=paste0("Elections/CLEA/Processed/GAUL_Voronoi/GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_000_voronoi.png"),width=4,height=4,units="in",res=150)
        plot(geo_vor$geometry,lwd=.25)
        dev.off()
        for(plot_var in toupper(mean_varz)){#print(plot_var)
          if(geo_vor[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_voronoi.png"),width=4,height=4,units="in",res=150)
            plot(geo_vor[plot_var],lwd=.25)
            dev.off()
            # png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_gaul1.png"),width=4,height=4,units="in",res=150)
            # plot(clea_gaul1[paste0(plot_var,"_pw")],lwd=.25)
            # dev.off()
            # png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_gaul1.png"),width=4,height=4,units="in",res=150)
            # plot(clea_gaul1[paste0(plot_var,"_aw")],lwd=.25)
            # dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(clea_gaul2[paste0(plot_var,"_PW")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(clea_gaul2[paste0(plot_var,"_AW")],lwd=.25)
            dev.off()
          }
        }
      },error=function(e){})
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

      # Ordinary kriging
      clea_gaul0 <- krig_fun(
        pointz=cst_sf,
        polyz=map0_,
        varz=mean_varz,
        epsg=NULL,
        epsg_poly=read_sf("EPSG","EPSG_Polygons"),
        messagez=clea_cntz[k0]) %>% as.data.table()
      clea_gaul1 <- krig_fun(
        pointz=cst_sf,
        polyz=map1_,
        varz=mean_varz,
        epsg=NULL,
        epsg_poly=read_sf("EPSG","EPSG_Polygons"),
        messagez=clea_cntz[k0]) %>% as.data.table()
      clea_gaul2 <- krig_fun(
        pointz=cst_sf,
        polyz=map2_,
        varz=mean_varz,
        epsg=NULL,
        epsg_poly=read_sf("EPSG","EPSG_Polygons"),
        messagez=clea_cntz[k0]) %>% as.data.table()

      # Rename variables
      clea_gaul0[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_gaul0 <- clea_gaul0 %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      clea_gaul1[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_gaul1 <- clea_gaul1 %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      clea_gaul2[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_gaul2 <- clea_gaul2 %>%
        setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME","cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2","CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("STR0_YEAR","EXP0_YEAR","STR1_YEAR","EXP1_YEAR","STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()

      # Save 
      save(clea_gaul0,clea_gaul1,clea_gaul2,file=paste0("Elections/CLEA/Processed/GAUL_Kriging/GAUL_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        for(plot_var in toupper(mean_varz)){#print(plot_var)
          if(clea_gaul2[paste0(plot_var,"_KR")] %>% na.omit() %>% nrow() > 0){
            # png(paste0("Elections/CLEA/Maps/GAUL_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul1.png"),width=4,height=4,units="in",res=150)
            # plot(clea_gaul1_kr[paste0(plot_var,"_KR")],lwd=.25)
            # dev.off()
            png(paste0("Elections/CLEA/Maps/GAUL_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_gaul2.png"),width=4,height=4,units="in",res=150)
            plot(clea_gaul2[paste0(plot_var,"_KR")],lwd=.25)
            dev.off()
          }
        }
      },error=function(e){})

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

# # Delete temporary GAUL files
# for(f0 in dir(temp)[grep(paste0(yrz[y0],".RData$"),dir(temp))]){
#   file.remove(paste0(temp,"/",f0))
# }

}
