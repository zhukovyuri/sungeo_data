# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO_Flu/Code/Preprocessing/flu_preprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO_Flu/Code/Preprocessing/flu_preprocess.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhukov","zhuk")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}
detachAllPackages()
list.of.packages <- c("sf","raster","tidyverse","data.table","countrycode","parallel","maptools","stringi","readxl")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)

# Install SUNGEO
# devtools::install_github("zhukovyuri/SUNGEO", dependencies=FALSE)
# install.packages("/data/Dropbox/SUNGEO/R_package/SUNGEO_0.1.0.tar.gz", repo = NULL, type="source")
sungeo_lib <- grep("R\\/x86",.libPaths(),value=T)
library(SUNGEO,lib.loc=sungeo_lib)

# Log file
log_file <- paste0("../r_log_errors_covid_",Sys.time() %>% gsub("-| |\\:|EDT","",.),".txt") 
write(paste("Session: ",Sys.time(),Sys.info()["nodename"],paste(Sys.info(),collapse=" "),sessionInfo()[[1]]$version.string), file = log_file,append=FALSE)
file.exists(log_file)



###############################################
###############################################
###############################################
## Preprocess: JHU + Barcelo panel data
###############################################
###############################################
###############################################

# Load JHU global
jhu_data <- list.files("Covid19/JHU/Raw/",pattern="jhu_data", full.names=T) %>% rev() %>% .[1] %>% readRDS() %>% .[,GEO_ID := paste0(LONGITUDE,"_",LATITUDE) %>% as.factor() %>% as.numeric()] %>% .[order(GEO_ID,DATE_ALT)] %>% .[,CASES_NEW := c(NA_real_,diff(CONFIRMED) %>% abs()),by=GEO_ID] %>% .[,DEATHS_NEW := c(NA_real_,diff(DEATHS) %>% abs()),by=GEO_ID] %>% .[,RECOVERED_NEW := c(NA_real_,diff(RECOVERED) %>% abs()),by=GEO_ID] %>% data.table::setnames(c("CONFIRMED","DEATHS","RECOVERED"),c("CASES_CUM","DEATHS_CUM","RECOVERED_CUM")) %>% .[,.SD,.SDcols=c("GEO_ID","ADM0_NAME","ADM1_NAME","ADM2_NAME","LONGITUDE","LATITUDE","GEO_PRECISION","DATE_ALT","CASES_CUM","DEATHS_CUM","RECOVERED_CUM","CASES_NEW","DEATHS_NEW","RECOVERED_NEW")] %>% data.table::setnames("DATE_ALT","DATE") %>% .[is.na(ADM1_NAME),ADM1_NAME := ADM0_NAME %>% str_split(", |,",simplify=T) %>% apply(1,function(x){rev(x[!is.na(x)&nchar(x)>0])[2]})] %>% .[is.na(ADM2_NAME),ADM2_NAME := ADM0_NAME %>% str_split(", |,",simplify=T) %>% apply(1,function(x){rev(x[!is.na(x)&nchar(x)>0])[3]})] %>% .[,ADM0_NAME := ADM0_NAME %>% str_split(", |,",simplify=T) %>% apply(1,function(x){rev(x[!is.na(x)&nchar(x)>0])[1]})] %>% .[,GEO_PRECISION := apply(.SD,1,function(x){paste0("ADM",length(x[!is.na(x)&nchar(x)>0])-1)}),.SDcols=c("ADM0_NAME","ADM1_NAME","ADM2_NAME")] %>% .[,ISO_A3 := ADM0_NAME %>% countrycode("country.name","iso3c")] %>% dplyr::select(GEO_ID,ISO_A3,everything())
jhu_data[ADM0_NAME%in%c("Saint Martin","St. Martin"),ISO_A3 := "MAF"]
jhu_data[ADM0_NAME%in%c("Channel Islands"),ISO_A3 := "CHI"]
jhu_data[ADM0_NAME%in%c("Kosovo"),ISO_A3 := "XKX"]
jhu_data[ADM0_NAME%in%c("Micronesia"),ISO_A3 := "FSM"]
jhu_data[ADM0_NAME%in%c("South"),ISO_A3 := "KOR"]
jhu_data[ADM0_NAME%in%c("South"),GEO_PRECISION := "ADM0"]
jhu_data[ADM0_NAME%in%c("South"),c("ADM0_NAME","ADM1_NAME") := .("South Korea",NA_character_)]
# Save country files
disag <- sort(unique(jhu_data$ISO_A3))
parallel::mclapply(disag,function(d0){
	saveRDS(jhu_data[ISO_A3%in%d0,],file=paste0("Covid19/JHU/Preprocessed/JHU_REPORTS_",d0,".RDS"))
	},mc.cores=detectCores()-1)


# Load JHU USA, convert to long, merge
jhu_deaths <- list.files("Covid19/JHU/Raw/",pattern="time_series_covid19_deaths_US", full.names=T) %>% rev() %>% .[1] %>% readRDS()
jhu_deaths_long <- jhu_deaths %>% melt(., id.vars = c("FIPS"),measure.vars = grep("^D_",names(.),value=T)) %>% .[order(FIPS,variable)] %>% .[!is.na(FIPS)&nchar(FIPS)>0,] %>% .[,delta := c(value[1],diff(value) %>% abs()),by=FIPS]  %>% data.table::setnames(c("variable","value","delta"),c("DATE","DEATHS_CUM","DEATHS_NEW"))
jhu_cases <- list.files("Covid19/JHU/Raw/",pattern="time_series_covid19_confirmed_US", full.names=T) %>% rev() %>% .[1] %>% readRDS()
jhu_cases_long <- jhu_cases %>% melt(., id.vars = c("FIPS"),measure.vars = grep("^D_",names(.),value=T)) %>% .[order(FIPS,variable)] %>% .[!is.na(FIPS)&nchar(FIPS)>0,] %>% .[,delta := c(value[1],diff(value) %>% abs()),by=FIPS]  %>% data.table::setnames(c("variable","value","delta"),c("DATE","CASES_CUM","CASES_NEW"))
jhu_long <- jhu_cases[,.SD,.SDcols=names(jhu_cases)[!grepl("^D",names(jhu_cases))]] %>% merge(jhu_cases_long,by="FIPS") %>% merge(jhu_deaths_long,by=c("FIPS","DATE")) %>% .[,DATE := gsub("^D_","",DATE)]
rm(jhu_cases,jhu_deaths,jhu_cases_long,jhu_deaths_long)
# Save country files
disag <- sort(unique(jhu_long$ISO3))
parallel::mclapply(disag,function(d0){
	saveRDS(jhu_long[ISO3%in%d0,],file=paste0("Covid19/JHU/Preprocessed/JHU_TS_",d0,".RDS"))
	},mc.cores=detectCores()-1)

# Load Barcelo, convert to long
dir("Covid19/CoronaNet/Raw")
cnet <- list.files("Covid19/CoronaNet/Raw/",pattern="geo\\.RDS",full.names=T) %>% readRDS() %>% as.data.table() %>% .[,geometry:=NULL] %>% .[,GEO_ID := paste0(longitude,"_",latitude) %>% as.factor() %>% as.numeric()] %>% .[, CNET_NPI := 1*grepl("Emergency|Monitoring|Resources|Distancing|Closure|Regulation|Lockdown|Quarantine|Restriction|Curfew|Border|Gatherings|Awareness|Anti-Disinformation|Other Policy|Hygiene",type)] %>% .[, CNET_PI := 1*grepl("Testing|Vaccine",type)] %>% .[, CNET_NPI_CLOSE := 1*grepl("Closure|Regulation|Lockdown|Quarantine|Restriction|Curfew|Border|Gatherings",type)] %>% .[, CNET_NPI_PINFO := 1*grepl("Awareness|Anti-Disinformation",type)] %>% .[, CNET_NPI_ENFOR := 1*grepl("Fines|Penalties|Jail",compliance)] %>% .[,CNET_NEW := 1*grepl("new",entry_type)] %>% .[,CNET_CHANGE := 1*grepl("Change",update_type)] %>% .[,CNET_STRENGTHEN := 1*grepl("Change",update_type)*grepl("Strength",update_level)] %>% .[,CNET_END := 1*grepl("End",update_type)] %>% data.table::setnames(c("country","province","city"),c("ADM0_NAME","ADM1_NAME","ADM2_NAME")) %>% .[,.SD,.SDcols=c("GEO_ID","ISO_A3","ADM0_NAME","ADM1_NAME","ADM2_NAME","longitude","latitude","geo_precision","date_start","date_end",grep("^CNET_",names(.),value=T))] %>% data.table::setnames(toupper(names(.))) %>% .[is.na(DATE_END)|nchar(DATE_END)==0,DATE_END:=Sys.Date() %>% as.character()] %>% .[DATE_END<DATE_START,c("DATE_START","DATE_END") := .(DATE_END,DATE_START)]
cnet <- mclapply(1:nrow(cnet),function(o0){
	# cat(o0,"\r")
	# print(o0)
	cnet_sub <- cnet[o0,] %>% .[,seq(as.Date(DATE_START),as.Date(DATE_END),by="day")] %>% data.table(DATE = .) %>% bind_cols(cnet[rep(o0,nrow(.)),]) %>% .[,c("CNET_NEW","CNET_CHANGE","CNET_STRENGTHEN","CNET_END") := lapply(.SD,function(x){c(x[1],rep(0,nrow(.)-1))}),.SDcols=c("CNET_NEW","CNET_CHANGE","CNET_STRENGTHEN","CNET_END")]
	cnet_sub
},mc.cores=detectCores()-1) %>% dplyr::bind_rows() %>% .[,c("DATE_START","DATE_END") := NULL] %>% .[,DATE:=gsub("-","",DATE)] %>% .[DATE <= Sys.Date() %>% as.character() %>% gsub("-","",.),]
# Save country files
disag <- sort(unique(cnet$ISO_A3)) %>% .[nchar(.)==3]
mclapply(disag,function(d0){
	saveRDS(cnet[ISO_A3%in%d0,],file=paste0("Covid19/CoronaNet/Preprocessed/CNET_",d0,".RDS"))
	},mc.cores=detectCores()-1)

# # Clear memory
# rm(list=ls())
# ls()
