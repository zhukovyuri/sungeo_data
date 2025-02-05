# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/fews_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/fews_aggregate.R")' &
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



###################################################
## Functions & parameters
###################################################

# SUNGEO functions
source("../Code/functions.R")

# List of countries to simplify polygons
simp_poly <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

# Skip existing files?
skip_existing <- TRUE

# Custom functions
source("../Code/functions.R")

# Set tempdir
temp <- tempdir()

###################################################
## Load data
###################################################

# Load FEWS Livelihood zones
fews <- read_sf("Economic/FEWS/RawData/FEWS_NET_LH_World.shp") %>% as.data.table() %>% st_as_sf()
format(object.size(fews),"Mb")

# Country codes
fews$ISO3 <- fews$COUNTRY %>% countrycode("iso2c","iso3c")


###################################################
## GRED
###################################################

# Unzip GRED polygons
filez <- dir("Admin/GRED/")[grep(".zip$",dir("Admin/GRED/"))]
vz <- filez %>% str_extract("(\\d)+")
con <- unzip(zipfile = paste0("Admin/GRED/",filez[1]), exdir = temp)
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
# Subset post-1990
sub_dirz <- sub_dirz[yr>=1990,]
sub_filez <- sub_dirz$f4

# Loop over countries
cntz <- fews$ISO3 %>% unique() %>% sort() %>% intersect(sub_dirz$iso3)
cntz
k0 <- 2
cntz_list <- mclapply(cntz %>% seq_along(),function(k0){

  # Loop over election cycles
  sub_dirz_k <- sub_dirz[iso3%in%cntz[k0],]
  ky_list <- lapply(1:nrow(sub_dirz_k),function(ky_){

    # Skip if existing
    if(!skip_existing|(skip_existing&!paste0("GRED_FEWS_",cntz[k0],"_",sub_dirz_k[ky_,f3],".RData")%in%dir("Economic/FEWS/Processed/GRED/"))){
    t1 <- Sys.time()

    print(paste0("k0 = ",k0,"/",length(cntz)," ",cntz[k0]," ",sub_dirz_k[ky_,f3]))

    # Load GRED country polygons
    poly_dest <- read_sf(paste0(sub_dirz_k[ky_,f1],"/",sub_dirz_k[ky_,f2],"/",sub_dirz_k[ky_,f3],"/",sub_dirz_k[ky_,f4],".shp")) %>% as.data.table() %>% st_as_sf()

    # Unzip & load population raster
    r_crop <- popcrop(yrz=sub_dirz_k[ky_,f3],poly=poly_dest,temp=temp)

    # Overlay
    fews_out <- ap_fun(
      poly_from=fews %>% fix_geom(),
      poly_to=poly_dest %>% fix_geom(),
      poly_to_id="cst",
      methodz=c("aw","pw"),
      pop_raster=r_crop,
      # varz="EFF_YEAR"
      char_varz=c("LZCODE","LZNAMEEN","CLASS","LZNUM"),
      char_assign=c("biggest_overlap","all_overlap")
    )

    # Save 
    save(fews_out,file=paste0("Economic/FEWS/Processed/GRED/GRED_FEWS_",cntz[k0],"_",sub_dirz_k[ky_,f3],".RData"))

    # Visualize
    colz <- data.frame(
      LZCODE=fews$LZCODE %>% unique() %>% sort(),
      COL=fews$LZCODE %>% unique() %>% sort() %>% as.factor() %>% as.numeric(),
      stringsAsFactors = FALSE
      ) %>% as.data.table()
    fews$COL <- colz[match(fews$LZCODE,colz$LZCODE),COL]
    fews_out$COL_aw <- colz[match(fews_out$LZCODE_aw,colz$LZCODE),COL]
    fews_out$COL_pw <- colz[match(fews_out$LZCODE_pw,colz$LZCODE),COL]
    png(paste0("Economic/FEWS/Maps/GRED/",cntz[k0],"_",sub_dirz_k[ky_,f3],"_fews.png"),width=4*1.618,height=4,units="in",res=100)
    par(mar=c(0,0,0,0))
    plot(fews[fews$ISO3%in%cntz[k0],] %>% st_geometry(),col=fews[fews$ISO3%in%cntz[k0],] %>% as.data.table() %>% select(COL) %>% unlist(),lwd=.25)
    dev.off()
    png(paste0("Economic/FEWS/Maps/GRED/",cntz[k0],"_",sub_dirz_k[ky_,f3],"_gred_aw.png"),width=4*1.618,height=4,units="in",res=100)
    par(mar=c(0,0,0,0))
    plot(fews_out %>% st_geometry(),col=fews_out %>% as.data.table() %>% select(COL_aw) %>% unlist(),lwd=.25)
    dev.off()
    png(paste0("Economic/FEWS/Maps/GRED/",cntz[k0],"_",sub_dirz_k[ky_,f3],"_gred_pw.png"),width=4*1.618,height=4,units="in",res=100)
    par(mar=c(0,0,0,0))
    plot(fews_out %>% st_geometry(),col=fews_out %>% as.data.table() %>% select(COL_pw) %>% unlist(),lwd=.25)
    dev.off()

    t2 <- Sys.time()
    print(t2-t1)
  # Close skip existing exception
  }

  # Close ky_ (election cycle) loop
  })

# Close k0 (country) loop
},mc.cores=min(length(cntz),detectCores()/2))

# plot(fews[fews$ISO3%in%cntz[k0],"LZCODE"])
# dev.off()
# plot(fews_out["LZCODE_pw"])
# dev.off()


###################################################
## GAUL
###################################################

# List of countries with broken GAUL geometries
fix_gaul <- c("JPN","NIC","PAK","BRA","ITA")

# List of countries to skip
skip_gaul <- c("USA","KEN","MLT","BEN")

# List of countries to simplify polygons
simp_gaul <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

# Loop over years
yrz <- 1990:2014
y0 <- 1
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

  # Subset GAUL
  map0 <- map0[map0$ISO3 %in% fews$ISO3,]
  map1 <- map1[map1$ISO3 %in% fews$ISO3,]
  map2 <- map2[map2$ISO3 %in% fews$ISO3,]
  object.size(map2) %>% format("Mb")

  # Unique countries
  cntz <- fews$ISO3 %>% unique() %>% sort() %>% intersect(map0$ISO3)
  cntz
  
  # Save country maps & rasters to temp folder
  save_list <- mclapply(seq_along(cntz),function(k0){

    # Subset by country, check/fix geometries
    map0_ <- map0[map0$ISO3%in%cntz[k0],] 
    map1_ <- map1[map1$ISO3%in%cntz[k0],] 
    map2_ <- map2[map2$ISO3%in%cntz[k0],] 
    if(cntz[k0]%in%fix_gaul){
      map0_ <- map0_ %>% fix_geom()
      map1_ <- map1_ %>% fix_geom()
      map2_ <- map2_ %>% fix_geom()  
    }
    if(cntz[k0]%in%"IND"){
      map0_ <- map0_[map0_$ADM0_NAME%in%"India",]
      map1_ <- map1_[map1_$ADM0_NAME%in%"India",]
      map2_ <- map2_[map2_$ADM0_NAME%in%"India",]
    }
    # Save to temp folder
    save(map0_,map1_,map2_,file=paste0(temp,"/GAUL_",cntz[k0],"_",yrz[y0],".RData"))
  },mc.cores=min(length(cntz),detectCores()-1))
  rm(map0,map1,map2,save_list)

  # Loop over countries
  cntz_list <- lapply(cntz %>% seq_along(),function(k0){

    # Skip if existing
    if(!skip_existing|(skip_existing&!paste0("GAUL_FEWS_",cntz[k0],"_",yrz[y0],".RData")%in%dir("Economic/FEWS/Processed/GAUL/"))){

      t1 <- Sys.time()
      print(paste0("k0 = ",k0,"/",length(cntz)," ",cntz[k0]," ",yrz[y0]))

      # Load GAUL country polygons
      load(paste0(temp,"/GAUL_",cntz[k0],"_",yrz[y0],".RData"))

      # Simplify polygons
      if(cntz[k0]%in%names(simp_gaul)){
        t1_ <- Sys.time()
        map0_ <- ms_simplify(map0_,keep=simp_gaul[cntz[k0]]) %>% fix_geom()
        map1_ <- ms_simplify(map1_,keep=simp_gaul[cntz[k0]]) %>% fix_geom()
        map2_ <- ms_simplify(map2_,keep=simp_gaul[cntz[k0]]) %>% fix_geom()
        t2_ <- Sys.time()
        print(t2_-t1_)
      }

      # Unzip & load population raster
      r_crop <- popcrop(yrz=yrz[y0],poly=map0_,temp=temp)

      # Overlay
      fews0_out <- ap_fun(
        poly_from=fews %>% fix_geom(),
        poly_to=map0_ %>% fix_geom(),
        poly_to_id="ADM0_CODE",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        # varz="EFF_YEAR"
        char_varz=c("LZCODE","LZNAMEEN","CLASS","LZNUM"),
        char_assign=c("biggest_overlap","all_overlap")
      )
      fews1_out <- ap_fun(
        poly_from=fews %>% fix_geom(),
        poly_to=map1_ %>% fix_geom(),
        poly_to_id="ADM1_CODE",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        # varz="EFF_YEAR"
        char_varz=c("LZCODE","LZNAMEEN","CLASS","LZNUM"),
        char_assign=c("biggest_overlap","all_overlap")
      )
      fews2_out <- ap_fun(
        poly_from=fews %>% fix_geom(),
        poly_to=map2_ %>% fix_geom(),
        poly_to_id="ADM2_CODE",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        # varz="EFF_YEAR"
        char_varz=c("LZCODE","LZNAMEEN","CLASS","LZNUM"),
        char_assign=c("biggest_overlap","all_overlap")
      )

      # Save 
      save(fews0_out,fews1_out,fews2_out,file=paste0("Economic/FEWS/Processed/GAUL/GAUL_FEWS_",cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      colz <- data.frame(
        LZCODE=fews$LZCODE %>% unique() %>% sort(),
        COL=fews$LZCODE %>% unique() %>% sort() %>% as.factor() %>% as.numeric(),
        stringsAsFactors = FALSE
        ) %>% as.data.table()
      fews$COL <- colz[match(fews$LZCODE,colz$LZCODE),COL]
      fews2_out$COL_aw <- colz[match(fews2_out$LZCODE_aw,colz$LZCODE),COL]
      fews2_out$COL_pw <- colz[match(fews2_out$LZCODE_pw,colz$LZCODE),COL]
      png(paste0("Economic/FEWS/Maps/GAUL/",cntz[k0],"_",yrz[y0],"_fews.png"),width=4*1.618,height=4,units="in",res=100)
      par(mar=c(0,0,0,0))
      plot(fews[fews$ISO3%in%cntz[k0],] %>% st_geometry(),col=fews[fews$ISO3%in%cntz[k0],] %>% as.data.table() %>% select(COL) %>% unlist(),lwd=.25)
      dev.off()
      png(paste0("Economic/FEWS/Maps/GAUL/",cntz[k0],"_",yrz[y0],"_gaul2_aw.png"),width=4*1.618,height=4,units="in",res=100)
      par(mar=c(0,0,0,0))
      plot(fews2_out %>% st_geometry(),col=fews2_out %>% as.data.table() %>% select(COL_aw) %>% unlist(),lwd=.25)
      dev.off()
      png(paste0("Economic/FEWS/Maps/GAUL/",cntz[k0],"_",yrz[y0],"_gaul2_pw.png"),width=4*1.618,height=4,units="in",res=100)
      par(mar=c(0,0,0,0))
      plot(fews2_out %>% st_geometry(),col=fews2_out %>% as.data.table() %>% select(COL_pw) %>% unlist(),lwd=.25)
      dev.off()

      t2 <- Sys.time()
      print(t2-t1)
    # Close skip existing exception
    }

  # Close k0 (country) loop
  })
  # ,mc.cores=min(length(cntz),detectCores()/2)


# Close y0 (year) loop
}


# plot(fews[fews$ISO3%in%cntz[k0],"LZCODE"])
# dev.off()
# plot(fews_out["LZCODE_pw"])
# dev.off()

