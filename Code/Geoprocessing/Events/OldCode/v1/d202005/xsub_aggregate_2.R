# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/EventData/Geoprocessing/xsub_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/EventData/Geoprocessing/xsub_aggregate.R")' &
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
#                       "rmapshaper")
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

# List of countries with broken GAUL geometries
fix_poly <- c("JPN","NIC","PAK","BRA","ITA")

# List of countries to skip
skip_poly <- c("USA","KEN","MLT","BEN")

# List of countries to simplify polygons
simp_poly <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

# Skip existing files?
skip_existing <- FALSE

# # Source custom functions
# source("../Code/functions.R")


####################################
# xSub-GRED merge: election level
####################################

# Set tempdir
temp <- tempdir()

# List of xSub data files
xsub_dir <- "../../XSub/Data/Upload/data_rdata_event/"
xsub_filez <- dir(xsub_dir) %>% (function(.){.[!grepl("^MELTT|^MIPT|^GTD|^ITERATE",.)]})
xsub_iso <- xsub_filez %>% strsplit("_") %>% sapply("[",2)

# Dates
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors = FALSE) %>% as.data.table()
# ticker <- ticker[as.character(ticker$DATE)>=range(as.character(events$DATE),na.rm=T)[1]&as.character(ticker$DATE)<=range(as.character(events$DATE),na.rm=T)[2],]

# Load CLEA data
load("Elections/CLEA/clea_lc_20190617/clea_lc_20190617.rdata")
clea <- clea_lc_20190617 %>% as.data.table(); rm(clea_lc_20190617)
# Fix character encoding issue (convert all to ASCII)
clea <- clea %>% dplyr::mutate_if(sapply(clea,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% as.data.table()
# Add country codes
clea[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
clea[ctr_n%in%"Micronesia",ISO3 := "FSM"]
clea[ctr_n%in%"Kosovo",ISO3 := "XKX"]
# Subset >1990
# clea_full <- clea
# save(clea_full,file=paste0(temp,"/clea_full.RData"))
# rm(clea_full)
# clea <- clea[yr>=1990,]

# Load GRED polygons
filez <- dir("Admin/GRED/")[grep(".zip$",dir("Admin/GRED/"))]
vz <- filez %>% str_extract("(\\d)+")
temp <- tempdir()
f_ <- 1
con <- unzip(zipfile = paste0("Admin/GRED/",filez[f_]), exdir = temp)
gred_dirz <- data.frame(
  f1 = temp,
  f2 = sapply(strsplit(con,"\\.|/"),"[",4),
  f3 = sapply(strsplit(con,"\\.|/"),"[",5),
  f4 = sapply(strsplit(con,"\\.|/"),"[",6),
  stringsAsFactors = FALSE
) %>% distinct(f4,.keep_all = TRUE) %>% drop_na(f4) %>% as.data.table()
gred_dirz[,ctr_n := f4 %>% strsplit("_") %>% sapply("[",3)]
gred_dirz[,iso3 := ctr_n %>% countrycode("country.name","iso3c")]
gred_dirz[,yr := f4 %>% strsplit("_") %>% sapply("[",4)]

# Keep only CLEA-GRED overlap
clea <- clea[paste0(ISO3,"_",yr) %in% paste0(gred_dirz$iso3,"_",gred_dirz$yr),]

# Keep only xSub-GRED overlap
xsub_filez <- xsub_filez[xsub_iso%in%gred_dirz$iso3]
xsub_iso <- xsub_iso[xsub_iso%in%gred_dirz$iso3]

######################
# Loop over files
######################

f0 <- 36

# PSOCK
ncores <- min(length(xsub_filez),detectCores()/2)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("xsub_dir","xsub_filez","xsub_iso","clea","gred_dirz","temp","datez","ticker","fix_poly","skip_poly","simp_poly","skip_existing"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(tidyverse))
parallel::clusterEvalQ(NULL, expr=library(sf))
parallel::clusterEvalQ(NULL, expr=library(rmapshaper))
parallel::clusterEvalQ(NULL, expr=library(stringi))
parallel::clusterEvalQ(NULL, expr=library(raster))
parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
file_list <- parLapply(NULL,1:length(xsub_filez),function(f0){

# # Start fork (mclapply)
# file_list <- mclapply(1:length(xsub_filez),function(f0){

  t1 <- Sys.time()
  print(paste0(f0,"/",length(xsub_filez)," ",xsub_filez[f0]))

  # Source custom functions
  source("../Code/functions.R")

  # Find matching GRED polygons
  gred_dirz_sub <- gred_dirz[iso3%in%xsub_iso[f0]]
  gred_yrz <- gred_dirz_sub$yr

  ######################
  # Loop over years
  ######################

  y0 <- 1
  year_list <- lapply(1:nrow(gred_dirz_sub),function(y0){

    # Load GRED polygon 
    gred_sf <- read_sf(paste0(temp,"/",gred_dirz_sub[y0,f2],"/",gred_dirz_sub[y0,f3],"/",gred_dirz_sub[y0,f4],".shp"))  %>% data.table::setnames(names(.),tolower(names(.))) %>% st_transform(st_crs("EPSG:4326"))
    if(gred_dirz_sub[y0,iso3]%in%fix_poly){gred_sf <- gred_sf %>% fix_geom()}
    gred_sf$o0 <- row.names(gred_sf)

    # Fix encoding issue
    gred_sf <- gred_sf %>% as.data.table() %>% mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% st_as_sf()

    # Find month of election
    yrmoz <- clea[ISO3 %in% xsub_iso[f0] & yr %in% gred_dirz_sub[y0,yr],(yr*100+mn)] %>% unique() 

    # Load xSub, convert to dt, filter by precision
    load(paste0(xsub_dir,xsub_filez[f0]))
    xsub_data <- indata %>% dplyr::filter((!is.na(LONG))&(!is.na(LAT))&(TIMEPRECISION%in%c("day"))) %>% as.data.table(); rm(indata)

    ######################
    # Run if at least one event
    ######################    
    if(nrow(xsub_data)>0){

      # Convert to sf
      xsub_data <- xsub_data %>% dplyr::mutate_if(is.factor, list( ~ as.character(.))) %>% as.data.table()
      xsub_sf <- st_as_sf(xsub_data,coords = c("LONG", "LAT"),crs=st_crs(gred_sf))

      # Variable selection
      sum_varz <- names(xsub_sf) %>% (function(.){.[grep("^INITIATOR|^TARGET|^DYAD|^SIDE|^ACTION_ANY",.)]})

      # Window selection
      windowz <- c(30,60,90)

      ######################
      # Loop over months
      ######################

      ym0 <- 1
      yrmo_list <- lapply(1:length(yrmoz),function(ym0){

        # Vector of dates on month of election
        wind_t <- ticker[YRMO%in%yrmoz[ym0],DATE]

        ######################
        # Run if non-empty set
        ######################
        if(sum(xsub_sf$DATE %in% c(wind_t))>0){

          ######################
          # Skip existing?
          ######################
          if(!skip_existing|(skip_existing&!paste0("GRED_",  xsub_filez[f0] %>% gsub("event",yrmoz[ym0],.)) %in% dir("EventData/xSub/Processed/GRED_simp/"))){

            ####
            # Event counts during month of election
            ####

            # Add point counts to polygons (month of election)
            t_mat_0 <- point2poly_simp(
              pointz = xsub_sf[xsub_sf$DATE%in%wind_t,],
              polyz = gred_sf,
              varz = sum_varz,
              na_val = 0
              )            

            # # Add point counts to polygons (month of election)
            # t_mat_0 <- pinp_fun(polyz=gred_sf,
            #     pointz=xsub_sf[xsub_sf$DATE%in%wind_t,],
            #     varz=sum_varz, na_val=0)
            
            # Loop over windows
            wind_list <- lapply(windowz,function(w0){
              # Extract dates
              wind_post <- ticker[YRMO%in%yrmoz[ym0],((max(TID)+1):(max(TID)+w0))] %>% (function(.){ticker[TID%in%.,DATE]})
              wind_pre <- ticker[YRMO%in%yrmoz,((min(TID)-1):(min(TID)-w0))] %>% (function(.){ticker[TID%in%.,DATE]})
              # Point-in-poly
              pre_mat_0 <- point2poly_simp(polyz=gred_sf,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_pre,],
                  varz=sum_varz, na_val=0)
              post_mat_0 <- point2poly_simp(polyz=gred_sf,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_post,],
                  varz=sum_varz, na_val=0)
              list(pre=pre_mat_0,post=post_mat_0)
            })

            # Combine results
            # ADM0
            idvar <- "cst"
            wind_mat_0 <- merge(t_mat_0,wind_list[[1]]$pre %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[1])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[1]]$post %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[1])),by=idvar,all.x=T,all.y=F) #%>% (function(.){.[order(. %>% select(idvar) %>% as.numeric()),]}) 
            if(length(windowz)>1){
              for(w0_ in 2:length(windowz)){
              wind_mat_0 <- merge(wind_mat_0,wind_list[[w0_]]$pre %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[w0_])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[w0_]]$post %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[w0_])),by=idvar,all.x=T,all.y=F)
               }
            }

          # Re-order, re-name columns
          xsub_dt <- wind_mat_0 %>% as.data.table()
          xsub_dt <- xsub_dt[,c("SOURCE","ISO3","YRMO") := list(xsub_data$SOURCE[1], xsub_iso[f0], yrmoz[ym0])] %>% data.table::setnames(old=c("cst","cst_n","ctr","ctr_n","yr"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR")) %>% dplyr::select(SOURCE,ISO3,YRMO,YEAR,everything()) #%>% select(-o0) 
          xsub_sf <- xsub_dt %>% st_as_sf()

          ####
          # Export
          ####

          # Visualize
          tryCatch({
            plot_var <- "ACTION_ANY"
            # png(paste0("EventData/xSub/Maps/GRED/",xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_t.png"),width=4,height=4,units="in",res=150)
            # plot(xsub_sf[paste0(plot_var)],lwd=.25,main=paste0(yrmoz[ym0]," ",plot_var))
            # dev.off()
            png(paste0("EventData/xSub/Maps/GRED_simp/",xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_pre.png"),width=4*1.618,height=4,units="in",res=150)
            plot_yz(xsub_sf,paste0(plot_var,"_pre",max(windowz)),plot_title=paste0(yrmoz[ym0]," ",plot_var,"_pre",max(windowz)),breaks_style="pretty")
            dev.off()
          },error=function(e){})

          # Save to file
          tryCatch({
            save(xsub_dt,file=paste0("EventData/xSub/Processed/GRED_simp/GRED_",  xsub_filez[f0] %>% gsub("event",yrmoz[ym0],.)))
            # st_write(xsub_sf,dsn=paste0("EventData/xSub/Processed/GRED/GRED_",  xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_sf.csv"),layer_options="GEOMETRY=AS_WKT",delete_dsn=TRUE)
          },error=function(e){})

          #####################    
          # Close if skip existing
          #####################
          }

        #####################    
        # Close if statement
        #####################
        }

      #####################
      # Close month loop
      #####################
      })

    #####################    
    # Close if statement
    #####################
    }

  #####################
  # Close year loop
  #####################
  })

#####################
# Close file loop
#####################
t2 <- Sys.time()
print(t2-t1)


# Close parlapply
})
parallel::stopCluster(cl)
gc()

# # Close mclappy
# },mc.cores=min(length(xsub_filez),detectCores()-1))




###############################################
###############################################
###############################################
## GAUL
###############################################
###############################################
###############################################

rm(list=ls())

# List of countries with broken GAUL geometries
fix_poly <- c("JPN","NIC","PAK","BRA","ITA")

# List of countries to skip
skip_poly <- c("USA","KEN","MLT","BEN")

# List of countries to simplify polygons
simp_poly <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01,ZAF=.001)

# Skip existing files?
skip_existing <- FALSE

# Source custom functions
# source("../Code/functions.R")


####################################
# xSub-GAUL merge: election level
####################################

# Clear tempdir
# tempdir() %>% normalizePath() %>% paste0(., "/", dir(tempdir())) %>% unlink(recursive = TRUE)
# dir(tempdir())

# Set tempdir
temp <- tempdir()

# List of xSub data files
xsub_dir <- "../../XSub/Data/Upload/data_rdata_event/"
xsub_filez <- dir(xsub_dir) %>% (function(.){.[!grepl("^MELTT|^MIPT|^GTD|^ITERATE",.)]})
xsub_iso <- xsub_filez %>% strsplit("_") %>% sapply("[",2)

# Dates
datez <- seq(as.Date("1900-01-01"), as.Date(Sys.Date()), by="days")
ticker <- data.frame(DATE=gsub("-","",datez),TID=1:length(datez),WID=rep(1:length(datez),each=7)[1:length(datez)],YRMO=substr(gsub("-","",datez),1,6),YEAR=substr(gsub("-","",datez),1,4),stringsAsFactors = FALSE) %>% as.data.table()
# ticker <- ticker[as.character(ticker$DATE)>=range(as.character(events$DATE),na.rm=T)[1]&as.character(ticker$DATE)<=range(as.character(events$DATE),na.rm=T)[2],]

# Load CLEA data
load("Elections/CLEA/clea_lc_20190617/clea_lc_20190617.rdata")
clea <- clea_lc_20190617 %>% as.data.table(); rm(clea_lc_20190617)
# Fix character encoding issue (convert all to ASCII)
clea <- clea %>% dplyr::mutate_if(sapply(clea,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% as.data.table()
# Add country codes
clea[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
clea[ctr_n%in%"Micronesia",ISO3 := "FSM"]
clea[ctr_n%in%"Kosovo",ISO3 := "XKX"]
# Subset >1990
clea_full <- clea
save(clea_full,file=paste0(temp,"/clea_full.RData"))
rm(clea_full)
clea <- clea[yr>=1990&yr<=2014,]

# Keep only xSub-CLEA overlap
xsub_filez <- xsub_filez[xsub_iso%in%clea$ISO3]
xsub_iso <- xsub_iso[xsub_iso%in%clea$ISO3]

y0 <- 1990
# Save temporary copies of GAUL layers for these countries
save_list_1 <- lapply(1990:2014,function(y0){
  t1_ <- Sys.time()
  print(y0)

  # Load GAUL
  con <- unzip(zipfile = paste0("Admin/GAUL/g2015_",y0,"_0.zip"), exdir = temp)
  map0 <- read_sf(paste0(temp,"/g2015_",y0,"_0"))
  map0$ISO3 <- countrycode(map0$ADM0_NAME,"country.name","iso3c")
  con <- unzip(zipfile = paste0("Admin/GAUL/g2015_",y0,"_1.zip"), exdir = temp)
  map1 <- read_sf(paste0(temp,"/g2015_",y0,"_1"))
  map1$ISO3 <- countrycode(map1$ADM0_NAME,"country.name","iso3c")
  con <- unzip(zipfile = paste0("Admin/GAUL/g2015_",y0,"_2.zip"), exdir = temp)
  map2 <- read_sf(paste0(temp,"/g2015_",y0,"_2"))
  map2$ISO3 <- countrycode(map2$ADM0_NAME,"country.name","iso3c")
  # Subset by country, check/fix geometries
  save_list_2 <- mclapply(xsub_iso %>% intersect(map0$ISO3) %>% intersect(clea[yr%in%y0,ISO3]) %>% unique(),function(k0){
    map0_ <- map0[map0$ISO3%in%k0,] 
    map1_ <- map1[map1$ISO3%in%k0,] 
    map2_ <- map2[map2$ISO3%in%k0,] 
    if(k0%in%fix_poly){
      map0_ <- map0_ %>% fix_geom()
      map1_ <- map1_ %>% fix_geom()
      map2_ <- map2_ %>% fix_geom()  
    }
    if(k0%in%"IND"){
      map0_ <- map0_[map0_$ADM0_NAME%in%"India",]
      map1_ <- map1_[map1_$ADM0_NAME%in%"India",]
      map2_ <- map2_[map2_$ADM0_NAME%in%"India",]
    }
   save(map0_,map1_,map2_,file=paste0(temp,"/GAUL_",k0,"_",y0,".RData"))
  },mc.cores=detectCores()-1)
  t2_ <- Sys.time()
  print(t2_-t1_)
})
dir(temp)

# # Load Natural Earth boundaries
# con_ne <- unzip(zipfile = "Admin/NE/ne_10m_admin_0_countries.zip", exdir = temp)
# map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("ISO_A3","ISO3")

######################
# Loop over files
######################

f0 <- 39
xsub_filez[f0]

# PSOCK
ncores <- min(length(xsub_filez),detectCores()/2)
cl <- parallel::makePSOCKcluster(ncores, outfile="")
parallel::setDefaultCluster(cl)
parallel::clusterExport(NULL,c("xsub_dir","xsub_filez","xsub_iso","clea","temp","datez","ticker","fix_poly","skip_poly","simp_poly","skip_existing"),envir = environment())
parallel::clusterEvalQ(NULL, expr=library(data.table))
parallel::clusterEvalQ(NULL, expr=library(tidyverse))
parallel::clusterEvalQ(NULL, expr=library(sf))
parallel::clusterEvalQ(NULL, expr=library(rmapshaper))
parallel::clusterEvalQ(NULL, expr=library(stringi))
parallel::clusterEvalQ(NULL, expr=library(raster))
parallel::clusterEvalQ(NULL, expr=library(SUNGEO))
file_list <- parLapply(NULL,1:length(xsub_filez),function(f0){

# # Start fork (mclapply)
# file_list <- mclapply(1:length(xsub_filez),function(f0){

  t1 <- Sys.time()
  print(paste0(f0,"/",length(xsub_filez)," ",xsub_filez[f0]))

  # Custom functions
  source("../Code/functions.R")

  # Load xSub, convert to dt, filter by precision
  load(paste0(xsub_dir,xsub_filez[f0]))
  xsub_data <- indata %>% filter((!is.na(LONG))&(!is.na(LAT))&(TIMEPRECISION%in%c("day"))) %>% as.data.table(); rm(indata)

  # Extract years (keep only overlap with GAUL & CLEA)
  yrz <- xsub_data$DATE %>% substr(1,4) %>% unique() %>% sort() %>% intersect(1990:2014) %>% intersect(clea[ISO3 %in% xsub_iso[f0] ,yr] %>% unique())

  # # Load xSub, convert to dt, filter by precision
  # load(paste0(xsub_dir,xsub_filez[f0]))
  # xsub_data <- indata %>% filter((!is.na(LONG))&(!is.na(LAT))&(TIMEPRECISION%in%c("day"))) %>% as.data.table(); rm(indata)


  ######################
  # Run if not empty set
  ######################

  if(length(yrz)>0){

    ######################
    # Loop over years
    ######################

    y0 <- 1
    year_list <- lapply(1:length(yrz),function(y0){

      # Load GAUL polygons
      load(paste0(temp,"/GAUL_",xsub_iso[f0],"_",yrz[y0],".RData"))
      format(object.size(map0_),units="Mb")
      format(object.size(map1_),units="Mb")
      format(object.size(map2_),units="Mb")

      # Simplify polygons
      if(xsub_iso[f0]%in%names(simp_poly)){
        t1_ <- Sys.time()
        map0_ <- rmapshaper::ms_simplify(map0_,keep=simp_poly[xsub_iso[f0]]) %>% fix_geom(self_int=FALSE)
        map1_ <- rmapshaper::ms_simplify(map1_,keep=simp_poly[xsub_iso[f0]]) %>% fix_geom(self_int=FALSE)
        map2_ <- rmapshaper::ms_simplify(map2_,keep=simp_poly[xsub_iso[f0]]) %>% fix_geom(self_int=FALSE)
        t2_ <- Sys.time()
        print(t2_-t1_)
      }
      format(object.size(map0_),units="Mb")
      format(object.size(map1_),units="Mb")
      format(object.size(map2_),units="Mb")
     
      # Find months of election
      yrmoz <- clea[ISO3 %in% xsub_iso[f0] & yr %in% yrz[y0],(yr*100+mn)] %>% unique() 

      # ######################
      # # Run if at least one event
      # ######################    
      # if(nrow(xsub_data)>0){

      # Convert xSub to sf
      xsub_data <- xsub_data %>% dplyr::mutate_if(is.factor, list(~ as.character(.))) %>% as.data.table()
      xsub_sf <- st_as_sf(xsub_data,coords = c("LONG", "LAT"),crs=st_crs(map0_))

      # Variable selection
      sum_varz <- names(xsub_sf) %>% (function(.){.[grep("^INITIATOR|^TARGET|^DYAD|^SIDE|^ACTION_ANY",.)]})

      # Window selection
      windowz <- c(30,60,90)

      ######################
      # Loop over months
      ######################

      ym0 <- 1
      yrmo_list <- lapply(1:length(yrmoz),function(ym0){

        # Vector of dates on month of election
        wind_t <- ticker[YRMO%in%yrmoz[ym0],DATE]

        ######################
        # Run if non-empty set
        ######################
        if(sum(xsub_sf$DATE %in% c(wind_t))>0){

          ######################
          # Skip existing?
          ######################
          if(!skip_existing|(skip_existing&!paste0("GAUL_",  xsub_filez[f0] %>% gsub("event",yrmoz[ym0],.)) %in% dir("EventData/xSub/Processed/GAUL_simp/"))){

            ####
            # Event counts during month of election
            ####

            # Add point counts to polygons (month of election)
            t_mat_0 <- point2poly_simp(polyz=map0_,
                pointz=xsub_sf[xsub_sf$DATE%in%wind_t,],
                varz=sum_varz, na_val=0)
            t_mat_1 <- point2poly_simp(polyz=map1_,
                pointz=xsub_sf[xsub_sf$DATE%in%wind_t,],
                varz=sum_varz, na_val=0)
            t_mat_2 <- point2poly_simp(polyz=map2_,
                pointz=xsub_sf[xsub_sf$DATE%in%wind_t,],
                varz=sum_varz, na_val=0)

            # Loop over windows
            wind_list <- lapply(windowz,function(w0){
              # Extract dates
              wind_post <- ticker[YRMO%in%yrmoz[ym0],((max(TID)+1):(max(TID)+w0))] %>% (function(.){ticker[TID%in%.,DATE]})
              wind_pre <- ticker[YRMO%in%yrmoz,((min(TID)-1):(min(TID)-w0))] %>% (function(.){ticker[TID%in%.,DATE]})
              # Point-in-poly
              pre_mat_0 <- point2poly_simp(polyz=map0_,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_pre,],
                  varz=sum_varz, na_val=0)
              pre_mat_1 <- point2poly_simp(polyz=map1_,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_pre,],
                  varz=sum_varz, na_val=0)
              pre_mat_2 <- point2poly_simp(polyz=map2_,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_pre,],
                  varz=sum_varz, na_val=0)
              post_mat_0 <- point2poly_simp(polyz=map0_,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_post,],
                  varz=sum_varz, na_val=0)
              post_mat_1 <- point2poly_simp(polyz=map1_,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_post,],
                  varz=sum_varz, na_val=0)
              post_mat_2 <- point2poly_simp(polyz=map2_,
                  pointz=xsub_sf[xsub_sf$DATE%in%wind_post,],
                  varz=sum_varz, na_val=0) 
              list(pre0=pre_mat_0,post0=post_mat_0,pre1=pre_mat_1,post1=post_mat_1,pre2=pre_mat_2,post2=post_mat_2)
            })

            # Combine results
            # ADM0
            idvar <- "ADM0_CODE"
            wind_mat_0 <- merge(t_mat_0,wind_list[[1]]$pre0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[1])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[1]]$post0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[1])),by=idvar,all.x=T,all.y=F) %>% (function(.){.[order(. %>% dplyr::select(idvar %>% all_of()) %>% as.numeric()),]}) 
            if(length(windowz)>1){
              for(w0_ in 2:length(windowz)){
              wind_mat_0 <- merge(wind_mat_0,wind_list[[w0_]]$pre0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[w0_])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[w0_]]$post0 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[w0_])),by=idvar,all.x=T,all.y=F)
               }
            }
            # ADM1
            idvar <- "ADM1_CODE"
            wind_mat_1 <- merge(t_mat_1,wind_list[[1]]$pre1 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[1])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[1]]$post1 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[1])),by=idvar,all.x=T,all.y=F) 
            if(length(windowz)>1){
              for(w0_ in 2:length(windowz)){
              wind_mat_1 <- merge(wind_mat_1,wind_list[[w0_]]$pre1 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[w0_])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[w0_]]$post1 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[w0_])),by=idvar,all.x=T,all.y=F)
              }
            }
            # ADM2
            idvar <- "ADM2_CODE"
            wind_mat_2 <- merge(t_mat_2,wind_list[[1]]$pre2 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[1])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[1]]$post2 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[1])),by=idvar,all.x=T,all.y=F) 
            if(length(windowz)>1){
              for(w0_ in 2:length(windowz)){
              wind_mat_2 <- merge(wind_mat_2,wind_list[[w0_]]$pre2 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_pre",windowz[w0_])),by=idvar,all.x=T,all.y=F) %>% merge(.,wind_list[[w0_]]$post2 %>% as.data.table() %>% dplyr::select(c(idvar,sum_varz) %>% all_of()) %>% data.table::setnames(old=sum_varz,new=paste0(sum_varz,"_post",windowz[w0_])),by=idvar,all.x=T,all.y=F)
              }
            }


          # Re-order, re-name columns
          xsub_dt_0 <- wind_mat_0 %>% as.data.table()
          xsub_dt_0 <- xsub_dt_0[,c("SOURCE","ISO3","YEAR","YRMO") := list(xsub_data$SOURCE[1], xsub_iso[f0], yrz[y0],yrmoz[ym0])] %>% data.table::setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2"),skip_absent=TRUE) %>% dplyr::select(SOURCE,ISO3,YRMO,YEAR,everything()) %>% dplyr::select(-one_of("STR0_YEAR","EXP0_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) 
          xsub_dt_0
          xsub_dt_1 <- wind_mat_1 %>% as.data.table()
          xsub_dt_1 <- xsub_dt_1[,c("SOURCE","ISO3","YEAR","YRMO") := list(xsub_data$SOURCE[1], xsub_iso[f0], yrz[y0],yrmoz[ym0])] %>% data.table::setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2"),skip_absent=TRUE) %>% dplyr::select(SOURCE,ISO3,YRMO,YEAR,everything()) %>% dplyr::select(-one_of("STR1_YEAR","EXP1_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) 
          xsub_dt_1
          xsub_dt_2 <- wind_mat_2 %>% as.data.table()
          xsub_dt_2 <- xsub_dt_2[,c("SOURCE","ISO3","YEAR","YRMO") := list(xsub_data$SOURCE[1], xsub_iso[f0], yrz[y0],yrmoz[ym0])] %>% data.table::setnames(old=c("ADM0_CODE","ADM0_NAME","ADM1_CODE","ADM1_NAME","ADM2_CODE","ADM2_NAME"),new=c("GAUL_CODE_0","GAUL_NAME_0","GAUL_CODE_1","GAUL_NAME_1","GAUL_CODE_2","GAUL_NAME_2"),skip_absent=TRUE) %>% dplyr::select(SOURCE,ISO3,YRMO,YEAR,everything()) %>% dplyr::select(-one_of("STR2_YEAR","EXP2_YEAR","STATUS","DISP_AREA","Shape_Area","Shape_Leng")) 
          xsub_dt_2


          # Visualize
          tryCatch({
            plot_var <- "ACTION_ANY"
            xsub_sf <- xsub_dt_1 %>% st_as_sf()
            png(paste0("EventData/xSub/Maps/GAUL_simp/",xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_pre_adm1.png"),width=4*1.618,height=4,units="in",res=150)
            plot_yz(xsub_sf,paste0(plot_var,"_pre",max(windowz)),plot_title=paste0(yrmoz[ym0]," ",plot_var,"_pre",max(windowz)),breaks_style="pretty")
            dev.off()
            xsub_sf <- xsub_dt_2 %>% st_as_sf()
            png(paste0("EventData/xSub/Maps/GAUL_simp/",xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_pre_adm2.png"),width=4*1.618,height=4,units="in",res=150)
            plot_yz(xsub_sf,paste0(plot_var,"_pre",max(windowz)),plot_title=paste0(yrmoz[ym0]," ",plot_var,"_pre",max(windowz)),breaks_style="pretty")
            dev.off()
          },error=function(e){})

          # Save to file
          tryCatch({
            save(xsub_dt_0,xsub_dt_1,xsub_dt_2,file=paste0("EventData/xSub/Processed/GAUL_simp/GAUL_",  xsub_filez[f0] %>% gsub("event",yrmoz[ym0],.)))
            # st_write(xsub_dt_0 %>% st_as_sf(),dsn=paste0("EventData/xSub/Processed/GAUL_simp/GAUL_",  xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_sf_0.csv"),layer_options="GEOMETRY=AS_WKT",delete_dsn=TRUE)
            # st_write(xsub_dt_1 %>% st_as_sf(),dsn=paste0("EventData/xSub/Processed/GAUL_simp/GAUL_",  xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_sf_1.csv"),layer_options="GEOMETRY=AS_WKT",delete_dsn=TRUE)
            # st_write(xsub_dt_2 %>% st_as_sf(),dsn=paste0("EventData/xSub/Processed/GAUL_simp/GAUL_",  xsub_filez[f0] %>% gsub("event.RData",yrmoz[ym0],.),"_sf_2.csv"),layer_options="GEOMETRY=AS_WKT",delete_dsn=TRUE)
          },error=function(e){})

          print(paste0("finished ",xsub_filez[f0]," ",yrmoz[ym0]))

          #####################    
          # Close if skip existing
          #####################
          }

        #####################    
        # Close if statement
        #####################
        }

      #####################
      # Close month loop
      #####################
      })

      # #####################    
      # # Close if statement
      # #####################
      # }

    #####################
    # Close year loop
    #####################
    })

  #####################
  # Close if statement
  #####################
  }

#####################
# Close file loop
#####################

print(paste0("finished ",xsub_filez[f0]))

t2 <- Sys.time()
print(t2-t1)


# Close parLapply
})
parallel::stopCluster(cl)
gc()

# # Close mclapply
# },mc.cores=min(length(xsub_filez),detectCores()-1))

