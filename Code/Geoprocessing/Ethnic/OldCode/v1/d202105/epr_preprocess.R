# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/Ethnic/Geoprocessing/epr_geoprocess.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("zhubu")){setwd("/media/zhukov/sg1/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhukov","zhuk")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("^node|^likert",Sys.info()[["nodename"]])){setwd("SUNGEO/Data/")}
if(grepl("arc-ts",Sys.info()[["nodename"]])){setwd("/scratch/zhukov_root/zhukov1/zhukov/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","dplyr","stringr","data.table","countrycode","parallel","maptools","stringi","SUNGEO")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
loaded.packages <- lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=TRUE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
# library(SUNGEO)

# sessionInfo()

# Log file
log_file <- "../r_log_errors_epr.txt"
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)

#############################
## Prepare environment
#############################

# # Temp dir
# tempd <- tempdir()
# unlink(tempd,recursive=TRUE)

# Load polygons and fix character encoding issue (convert all to ASCII)
dir("Ethnic/EPR/Raw")
epr <- list.files("Ethnic/EPR/Raw",pattern="2019\\.geojson",full.names=TRUE) %>% sf::st_read() %>% sf::st_set_crs("+init=epsg:4326") %>% dplyr::mutate_if(sapply(.,is.factor),function(x){as.character(x) %>% iconv("latin1","UTF-8", "byte")  %>% stringi::stri_trans_general("Latin-ASCII")})

# Break up by year
yearz <- 1946:2017
y0 <- 2017
epr_yearz <- mclapply(yearz,function(y0){print(y0)
	epr_year <- epr %>% as.data.table() %>% .[from <= y0 & to >= y0] %>% sf::st_as_sf()
	epr_year <- epr_year[!(epr_year %>% sf::st_is_empty()),]
	saveRDS(epr_year,file=paste0("Ethnic/EPR/Raw/EPR_Yearly/EPR_",y0,".RDS"))
	},mc.cores=8)
