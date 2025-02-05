# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhuk","zhukov")){setwd("~/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["user"]]%in%c("cjfariss")){setwd("/Users/cjfariss/Dropbox/SUNGEO/Data/")}

## Install & load packages 
detachAllPackages <- function() {
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
}; detachAllPackages()
list.of.packages <- c("dplyr","data.table","parallel","sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}; loaded.packages <- lapply(list.of.packages, require, character.only = TRUE); rm(list.of.packages,new.packages,loaded.packages,detachAllPackages)


#######################
# Create connectivity matrices
#######################

skip_existing <- TRUE

# List of boundary sets
admz <- c("GADM","GAUL","GRED","geoBoundaries","MPIDR","NHGIS","SHGIS","PRIOGRID","HEXGRID")

# Full list of files to be processed
suppressWarnings({
filez <- lapply(seq_along(admz),function(a0){dir(paste0("Admin/",admz[a0],"/Simplified")) %>% stringr::str_split("_",simplify=T) %>% .[,2:4] %>% as.data.frame(stringsAsFactors=T) %>% data.table::setnames(c("iso3","adm_yr","adm")) %>% dplyr::mutate(iso3 = iso3 %>% as.character(), adm_yr = adm_yr %>% as.character() %>% as.numeric(), adm = adm %>% as.character(), adm_f0 = list.files(paste0("Admin/",admz[a0],"/Simplified"),full.names=T) %>% as.character()) %>% dplyr::arrange(iso3,adm_yr) %>% as.data.table() %>% .[,geoset := admz[a0]] %>% dplyr::select(geoset,everything()) %>% .[order(file.size(adm_f0)),]}) %>% dplyr::bind_rows() %>% .[,w_ix_f0 := paste0("Admin/",geoset,"/Connectivity/",iso3,"_",geoset,adm_yr,"_",adm,"_W_IX.RDS")] %>% .[,w_id_f0 := paste0("Admin/",geoset,"/Connectivity/",iso3,"_",geoset,adm_yr,"_",adm,"_W_ID.RDS")] %>% .[,w_pid_f0 := paste0("Admin/",geoset,"/Connectivity/",iso3,"_",geoset,adm_yr,"_",adm,"_W.RDS")]
 #%>% merge(readRDS("../Code/Copy/all_umap.RDS"),by="adm_f0",all.x=T,all.y=F)
}); filez

# Unique (subnational) maps
mapz <- filez[adm != "ADM0",unique(adm_f0)]
mapz <- filez[,unique(adm_f0)]

# Examples
m0 <- grep("GADM_UGA_2018_ADM1",mapz,value=TRUE); m0
m0 <- grep("NHGIS_USA_2010_ADM0",mapz,value=TRUE); m0
m0 <- mapz[239]

filez[adm_f0%in%m0 ,]

# Loop
map_list <- parallel::mclapply(mapz,function(m0){

  # Skip existing
  if(!skip_existing|(skip_existing&filez[adm_f0%in%m0 ,(!file.exists(w_ix_f0)|!file.exists(w_id_f0)|!file.exists(w_pid_f0))])){

  print(paste0(which(mapz%in%m0),"/",length(mapz)," ... ",m0))
  
  suppressWarnings({
    suppressMessages({
      # Load
      map0 <- sf::st_read(m0,quiet=TRUE)
      # Proceed if non-empty
      if(nrow(map0)>0){
        svar <- filez[adm_f0%in%m0,unique(adm) %>% paste0(.,"_CODE")]
        # Create ID if missing
        if(!svar%in%names(map0)){map0$TEMPID <- map0 %>% as.data.table() %>% .[,filez[adm_f0%in%m0,paste0(adm,"_NAME")]   %>% get() %>% as.factor() %>% as.numeric() ]; names(map0)[names(map0)=="TEMPID"] <- svar}
        # Find neighbors
        if(filez[adm_f0%in%m0 ,(!file.exists(w_ix_f0))]){
          w <- sf::st_intersects( sf::st_as_sfc( lapply(sf::st_geometry(map0), function(x){sf::st_as_sfc(sf::st_bbox(x))[[1]]} ) ) ) %>% as.data.table() %>% data.table::setnames(c("row_id","col_id")) #%>% .[row_id!=col_id] 
          saveRDS(w,file=filez[adm_f0%in%m0,w_ix_f0])
        }
        if(filez[adm_f0%in%m0 ,(!file.exists(w_id_f0))]){
          w_id <- map0 %>% as.data.table() %>% .[,get(svar)] %>% data.table(temp = .,svar_i= .[w$row_id], svar_j = .[w$col_id]) %>% .[,temp:=NULL]      
          saveRDS(w_id,file=filez[adm_f0%in%m0,w_id_f0])
        }
        if(filez[adm_f0%in%m0 ,(!file.exists(w_pid_f0))]){
          w_polyid <- data.table(POLYGON_ID_i = filez[adm_f0%in%m0,paste0(iso3,"_",geoset,adm_yr,"_",adm,"_") %>% unique()] %>% paste0(.,w_id$svar_i), POLYGON_ID_j = filez[adm_f0%in%m0,paste0(iso3,"_",geoset,adm_yr,"_",adm,"_") %>% unique()] %>% paste0(.,w_id$svar_j))
          saveRDS(w_polyid,file=filez[adm_f0%in%m0,w_pid_f0])
        }
      # end nrow>0
      }

      })
    })

  # end skip
  }

},mc.cores=parallel::detectCores()/2)


