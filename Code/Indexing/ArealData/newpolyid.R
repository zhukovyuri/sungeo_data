# Connect to boba
# ssh 10.0.0.18
# ssh 2601:189:8000:b3a0:d774:600f:ebca:d6b2

# ssh zhukov@sungeo.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/Emissions/Geoprocessing/edgar_geoprocess.R")'  > sungeo_edgar.log 2>&1 &  
# nohup R -e 'source("/media/zhukov/dropbox2022/Dropbox/SUNGEO/Code/Emissions/Geoprocessing/edgar_geoprocess.R")'  > sungeo_edgar.log 2>&1 & 
# tail -f sungeo_edgar.log
# R


rm(list=ls())


## Detect system and set directory
setwd("~/")
if(grepl("boba",Sys.info()[["nodename"]])){setwd("/media/zhukov/dropbox2022/Dropbox/SUNGEO/")}
if(grepl("sungeo",Sys.info()[["nodename"]])){setwd("/data/Dropbox/SUNGEO/")}
if(grepl("^ubu|^zhu",Sys.info()[["nodename"]])){setwd("~/Dropbox/SUNGEO/")}


## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("data.table","stringr","parallel","dplyr","sf","SUNGEO","countrycode")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)


######################################
# Loop over ADMZ
######################################

# # Print progress for each file?
# extra_verbose <- TRUE

# # Skip existing files?
# skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","HEXGRID","PRIOGRID")

adm_dirz <- lapply(seq_along(admz),function(a0){dir(paste0("Data/Admin/",admz[a0],"/Simplified")) %>% stringr::str_split("_",simplify=TRUE) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character() %>% gsub("GB-EAW","GBR",.), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Data/Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% dplyr::arrange(iso3,adm_yr) %>% data.table::as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,dplyr::everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[file.size(adm_f0)>1000] %>% .[,fnn_out := gsub("Simplified","Reindexed",adm_f0)] %>% .[,exists := fnn_out %>% file.exists()]

# k0 <- 30000
pb <- txtProgressBar(min = 0, max = adm_dirz[,.N], style = 3); 
mapmat <- lapply(adm_dirz[,.I],function(k0){
  # print(k0)
  setTxtProgressBar(pb, k0)
  tryCatch({
    map <- sf::read_sf(adm_dirz[k0,adm_f0])
    map$SG_POLYID <- paste0(adm_dirz[k0,paste0(iso3,"_",geoset,adm_yr,"_",adm,"_")],sprintf(paste0("%0",nchar(nrow(map)),"d"), 1:nrow(map)))
    map <- map %>% dplyr::select(SG_POLYID,dplyr::everything())
    sf::write_sf(map,dsn=adm_dirz[k0,fnn_out], delete_dsn=TRUE)
  },error=function(e){
    cat("\n");
    print(paste0("ERROR! k0=",k0, "; ",adm_dirz[k0,fnn_out],"... ",e));
    cat("\n")
  })
})

dir("Data/Admin/PRIOGRID/Reindexed")