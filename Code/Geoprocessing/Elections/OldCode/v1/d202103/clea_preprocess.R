# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_preprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/clea_preprocess.R")' &
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
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","SUNGEO")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# install.packages("devtools")
# library(devtools)
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# devtools::install_git("git://github.com/zhukovyuri/SUNGEO.git")
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
# library(SUNGEO)

###############################################
###############################################
###############################################
## Preprocess
###############################################
###############################################
###############################################

# # List of countries with broken GAUL geometries
# fix_poly <- c("JPN","NIC","PAK","BRA","ITA","AUS")

# List of countries to skip
skip_poly <- NULL

# # Skip existing files?
# skip_existing <- FALSE

# Source custom functions
source("../Code/functions.R")

# Set tempdir
temp <- tempdir()

# Load CLEA data
load("Elections/CLEA/clea_lc_20201216/clea_lc_20201216.rdata")
clea <- clea_lc_20201216; rm(clea_lc_20201216) 
# Fix character encoding issue (convert all to ASCII)
clea <- clea %>% dplyr::mutate_if(sapply(clea,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% as.data.table()
# Add country codes
clea[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
clea[ctr_n%in%"Micronesia",ISO3 := "FSM"]
clea[ctr_n%in%"Kosovo",ISO3 := "XKX"]
clea[ctr_n%in%"Somaliland",ISO3 := "SOM"]
# Subset >1990
# clea_full <- clea
# save(clea_full,file=paste0(temp,"/clea_full.RData"))
# rm(clea_full)
object.size(clea) %>% format(units="Mb")
# clea <- clea[yr>=1990,]

# List of country-years in CLEA
sub_dirz <- clea[,.(ISO3,yr)] %>% unique() %>% data.table::setnames(names(.),tolower(names(.))) %>% arrange(yr,iso3) %>% dplyr::filter(!is.na(iso3))

# Load coordinates, Fix character encoding issue (convert all to ASCII)
geo_mat <- readRDS("Elections/CLEA/GeoCode/cst_best_v5.RDS") %>% dplyr::mutate_if(sapply(.,is.character),function(.){stringi::stri_encode(., "", "UTF-8") %>% stringi::stri_trans_general(., "Latin-ASCII")}) %>% dplyr::mutate(ISO3 = countrycode(ctr_n,"country.name","iso3c")) %>% as.data.table()
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat[ctr_n%in%"Somaliland",ISO3 := "SOM"]
geo_mat[is.na(long),]

# Fix Israel
geo_mat[iso3c=="ISR"&is.na(long),c("long","lat"):=.(34.97892,32.12208)]
# clea <- clea[ISO3=="ISR"]

# Prepare country-year level processed CLEA files & other objects
# Loop over years
print("Prepping temp files")
y0 <- 144
yrz <- unique(clea$yr) %>% sort()
yr_list <- mclapply(1:length(yrz),function(y0){
  print(yrz[y0])

  # Subset CLEA by year
  clea_yr <- clea[yr%in%yrz[y0],]
  clea_cntz <- clea_yr[,ISO3] %>% unique() %>% sort()

  # Exceptions
  clea_cntz <- clea_cntz[!clea_cntz%in%skip_poly]

  # Loop over countries
  k0 <- 2
  save_list <- lapply(seq_along(clea_cntz),function(k0){
    # # cat(round(k0/length(clea_cntz),2),"\r")
 
    # Past elections for country k0
    clea_ <- clea[(ISO3%in%clea_cntz[k0])&(yr<yrz[y0]),]

    # Pre-process CLEA
    clea_mat <- clea_prep(
      clea_i = clea_yr[ISO3%in%clea_cntz[k0],],
      clea_i_t1 = clea_[yr==max(yr),] %>% as.data.table())
    clea_mat

    # Merge with long/lats
    geo_sub_ <- geo_mat[(ISO3==clea_cntz[k0]) & (cst_n %in% clea_yr[ISO3%in%clea_cntz[k0],cst_n]) & (yr %in% yrz[y0]),]
    clea_mat <- merge(clea_mat,geo_sub_ %>% dplyr::select(-cst_n),by=c("ctr_n","cst"))

    # save(clea_mat,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
    saveRDS(clea_mat,file=paste0("Elections/CLEA/Preprocessed/CLEA_",clea_cntz[k0],"_",yrz[y0],".RDS"))
  })

  dir(temp)
  rm(save_list,clea_full)
  gc(reset = TRUE, full = TRUE)

},mc.cores=min(length(yrz),detectCores()-1))

