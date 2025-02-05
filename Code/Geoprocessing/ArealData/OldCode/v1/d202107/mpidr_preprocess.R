# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/mpidr_preprocess.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ArealData/Geoprocessing/mpidr_preprocess.R")' &
# tail -f nohup.out
# R

rm(list=ls())

## Set directory
setwd("~/")
if(Sys.info()[["nodename"]]%in%"sungeo"){setwd("/data/Dropbox/SUNGEO/Data/")}
if(Sys.info()[["nodename"]]%in%c("ubu","zhuk","zhukov")){setwd("~/Dropbox/SUNGEO/Data/")}
if(grepl("w64",sessionInfo()[[1]][[1]])){setwd("K:/Dropbox/SUNGEO/Data/")}

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
                      "data.table",
                      "deldir",
                      "tidyverse",
                      "V8",
                      "rmapshaper")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}
lapply(list.of.packages, require, character.only = TRUE)
rm(list.of.packages,new.packages,detachAllPackages)
library(SUNGEO)


################################
# Break up Europe shapefile
################################

dir("Admin/MPIDR")
temp <- tempdir()
con <- unzip("Admin/MPIDR/Zips/EUR/europe_1900-2003.zip", exdir = temp)
filez <- con[grep("Europe_",con)] %>% gsub(".dbf|.prj|.sbn|.sbx|.shp|.shp.xml|.shx","",.) %>% unique()
yearz <- filez %>% str_extract("\\d{4}")

f0 <- 1
year_list <- lapply(seq_along(yearz),function(f0){
  print(yearz[f0])

  eur <- st_read(paste0(filez[f0],".shp"),quiet=TRUE) %>% fix_geom()
  eur %>% as.data.frame() %>% dplyr::select(COUNTRY,NAME) %>% dplyr::arrange(COUNTRY,NAME)

  # Country names
  if(yearz[f0]==1900){
    adm0 <- c("10_Austria-Hungary","20_Luxembourg","30_Belgium","31_Belgium","40_Denmark","50_United Kingdom","51_United Kingdom","60_Russia","70_France","80_Germany","90_Greece","91_Greece","100_United Kingdom","110_Italy","120_Netherlands","130_Norway","140_Russia","150_Portugal","160_Russia","170_United Kingdom","180_Spain","181_Spain","190_Sweden","210_Albania","200_Switzerland","220_Bulgaria","230_Czechoslovakia","231_Czechoslovakia","240_Austria-Hungary","250_Romania","260_Serbia","270_Iceland","300_Turkey","310_Liechtenstein","320_San Marino","350_Andorra","380_Danzig","390_Saar")
    adm1 <- c("10_Austria","50_England and Wales","51_England and Wales","60_Grand Duchy of Finland","100_Ireland","140_Kingdom of Poland","160_Russia","170_Scotland","240_Hungary")
  }
  if(yearz[f0]%in%c(1930,1960)){
    adm0 <- c("10_Austria","20_Luxembourg","30_Belgium","31_Belgium","40_Denmark","50_United Kingdom","51_United Kingdom","60_Finland","70_France","80_Germany","90_Greece","91_Greece","100_Ireland","110_Italy","120_Netherlands","130_Norway","140_Poland","150_Portugal","160_Russia","170_United Kingdom","180_Spain","181_Spain","190_Sweden","210_Albania","200_Switzerland","220_Bulgaria","230_Czechoslovakia","231_Czechoslovakia","240_Hungary","250_Romania","260_Serbia","270_Iceland","300_Turkey","310_Liechtenstein","320_San Marino","350_Andorra","380_Danzig","390_Saar")
    adm1 <- c("50_England and Wales","51_England and Wales","170_Scotland")
  }
  if(yearz[f0]<1990){
    eur$ADM0_NAME <- adm0[eur$COUNTRY %>% match(adm0 %>% strsplit("_") %>% sapply(function(x){x[1]}))] %>% strsplit("_") %>% sapply(function(x){x[2]})
    eur$ADM0_NAME[eur$COUNTRY==0] <- eur$NAME[eur$COUNTRY==0] %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub(".-Maz.-Tracia","ania",.) %>% gsub("Lichten","Liechten",.) %>% gsub("Surrey","United Kingdom",.)
    eur$ADM0_CODE <- eur$ADM0_NAME %>% as.factor() %>% as.numeric()
    eur$ADM0_ISO3 <- eur$ADM0_NAME %>% countrycode(origin="country.name","iso3c")
    eur$ADM0_ISO3[eur$ADM0_NAME%in%"Austria-Hungary"] <- "AUH"
    eur$ADM0_ISO3[eur$ADM0_NAME%in%"Czechoslovakia"] <- "CSK"
    eur$ADM0_ISO3[eur$ADM0_NAME%in%c("Danzig","Saar")] <- "DEU"
    eur$ADM0_ISO3[eur$ADM0_NAME%in%"Komi-Permjaken"] <- "RUS"
    eur$ADM1_NAME <- adm1[eur$COUNTRY %>% match(adm1 %>% strsplit("_") %>% sapply(function(x){x[1]}))] %>% strsplit("_") %>% sapply(function(x){x[2]})
    eur$ADM1_NAME[is.na(eur$ADM1_NAME)] <- eur$NAME[is.na(eur$ADM1_NAME)] %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
    eur$ADM1_NAME[eur$NAME=="SURREY"] <- "England and Wales"
    eur$ADM1_CODE <- eur$ADM1_NAME %>% as.factor() %>% as.numeric()
    eur$ADM2_NAME <- NA_character_
    eur$ADM2_NAME[adm1[eur$COUNTRY %>% match(adm1 %>% strsplit("_") %>% sapply(function(x){x[1]}))] %>% strsplit("_") %>% sapply(function(x){x[2]}) %>% (function(.){which(!is.na(.))})] <- eur$NAME[adm1[eur$COUNTRY %>% match(adm1 %>% strsplit("_") %>% sapply(function(x){x[1]}))] %>% strsplit("_") %>% sapply(function(x){x[2]}) %>% (function(.){which(!is.na(.))})] %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish() %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
    eur$ADM2_CODE <- NA_real_
    eur$ADM2_CODE[!is.na(eur$ADM2_NAME)] <- eur$ADM2_NAME[!is.na(eur$ADM2_NAME)] %>% as.factor() %>% as.numeric()
    eur$MPIDR_YEAR <- yearz[f0]
    eur <- eur %>% dplyr::select(AUTONUMBER,ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,MPIDR_YEAR) %>% data.table::setnames("AUTONUMBER","MPIDR_ID")
  }
  if(yearz[f0]>=1990){
    eur$ADM0_NAME <- eur$COUNTRY %>% countrycode("iso2c","country.name")
    eur$ADM0_NAME[eur$COUNTRY%in%"UK"] <- "United Kingdom"
    eur$ADM0_NAME[eur$COUNTRY%in%"SAM"] <- "San Marino"
    eur$ADM0_NAME[eur$COUNTRY%in%"CS"] <- "Czechoslovakia"
    eur$ADM0_NAME[eur$COUNTRY%in%"EI"] <- "Ireland"
    eur$ADM0_NAME[eur$COUNTRY%in%"GC"] <- "German Democratic Republic"
    eur$ADM0_NAME[eur$COUNTRY%in%"PO"] <- "Portugal"
    eur$ADM0_NAME[eur$COUNTRY%in%"SW"] <- "Sweden"
    eur$ADM0_NAME[eur$COUNTRY%in%"YO"] <- "Yugoslavia"
    eur$ADM0_CODE <- eur$ADM0_NAME %>% as.factor() %>% as.numeric()
    eur$ADM0_ISO3 <- eur$ADM0_NAME %>% countrycode(origin="country.name","iso3c")
    eur$ADM0_ISO3[eur$ADM0_NAME%in%"Czechoslovakia"] <- "CSK"
    eur$ADM0_ISO3[eur$ADM0_NAME%in%"Yugoslavia"] <- "YUG"
    eur$ADM0_ISO3[eur$ADM0_NAME%in%"German Democratic Republic"] <- "DDR"
    eur$ADM1_NAME <- eur$NAME %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
    eur$ADM1_CODE <- eur$ADM1_NAME %>% as.factor() %>% as.numeric()
    eur$ADM2_NAME <- NA_character_
    eur$ADM2_CODE <- NA_real_
    eur$MPIDR_YEAR <- yearz[f0]
    eur <- eur %>% dplyr::select(AUTONUMBER,ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,MPIDR_YEAR) %>% data.table::setnames("AUTONUMBER","MPIDR_ID")
  }

  # Subset and save by country
  disag <- eur$ADM0_ISO3 %>% unique() %>% sort()
  k0 <- 25

  eur_list <- mclapply(seq_along(disag),function(k0){
    print(paste0(k0,"/",length(disag)," ",disag[k0]," ",yearz[f0]))

    # Error catching
    tryCatch({

    # Subset
    map <- eur[eur$ADM0_ISO3%in%disag[k0],]

    # Simplify polygons
    if(object.size(map) %>% format(units="Mb")>2){
      t1_ <- Sys.time()
      map <- rmapshaper::ms_simplify(map,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
      object.size(map) %>% format(units="Mb") %>% paste0(disag[k0]," ",yearz[f0] ," ",.) %>% print()
      t2_ <- Sys.time()
      print(t2_-t1_)
    }

    # Dissolve
    if(mean(is.na(map$ADM2_CODE))<1){
      map2 <- map
      map1 <- rmapshaper::ms_dissolve(map2,field="ADM1_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),sys=T)
      map0 <- rmapshaper::ms_dissolve(map1,field="ADM0_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE"),sys=T)
      # plot(map0["ADM0_NAME"]); dev.off()
      map0 <- map0 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
      map1 <- map1 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
      map2 <- map2 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
      write_sf(map0,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_",disag[k0],"_",yearz[f0],"_ADM0_wgs.geojson"),delete_dsn=TRUE)
      write_sf(map1,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_",disag[k0],"_",yearz[f0],"_ADM1_wgs.geojson"),delete_dsn=TRUE)
      write_sf(map2,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_",disag[k0],"_",yearz[f0],"_ADM2_wgs.geojson"),delete_dsn=TRUE)
    }
    if(mean(is.na(map$ADM2_CODE))==1){
      map1 <- map %>% dplyr::select(-c("ADM2_NAME","ADM2_CODE"))
      map0 <- rmapshaper::ms_dissolve(map1,field="ADM0_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE"),sys=T)
      # plot(map0["ADM0_NAME"]); dev.off()
      map0 <- map0 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
      map1 <- map1 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
      write_sf(map0,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_",disag[k0],"_",yearz[f0],"_ADM0_wgs.geojson"),delete_dsn=TRUE)
      write_sf(map1,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_",disag[k0],"_",yearz[f0],"_ADM1_wgs.geojson"),delete_dsn=TRUE)
    }

    # Save in native projection
    # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_",disag[k0],"_",yearz[f0],"_laea.csv"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
    # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_",disag[k0],"_",yearz[f0],"_laea.geojson"),delete_dsn=TRUE)

    # Reproject to EPSG:4326
    # map <- map %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
    # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_",disag[k0],"_",yearz[f0],"_wgs.csv"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
    # write_sf(map,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_",disag[k0],"_",yearz[f0],"_ADM1_wgs.geojson"),delete_dsn=TRUE)

    # Error catching
    },error=function(e){print(paste0("ERROR: ",k0," ",disag[k0]," ",yearz[f0]))})

  },mc.cores=min(length(disag),detectCores()-1))

})




################################
# Process other shapefiles
################################


# dir("Admin/MPIDR/Zips/SRB")

############
# DEU
############

yrz_0 <- dir("Admin/MPIDR/Zips/DEU") %>% str_extract("\\d{4}-\\d{4}") %>% sort()
y0 <- 5

zip_list <- lapply(seq_along(yrz_0),function(y0){
  print(yrz_0[y0])

  # Unzip
  temp <- tempdir()
  con <- unzip(paste0("Admin/MPIDR/Zips/DEU/",dir("Admin/MPIDR/Zips/DEU")[grep(yrz_0[y0],dir("Admin/MPIDR/Zips/DEU"))]), exdir = temp)
  filez <- con[grep("German_Union|German_Empire|Germany_",con)] %>% gsub(".dbf|.prj|.sbn|.sbx|.shp|.shp.xml|.shx|.xml","",.) %>% unique()
  yearz <- filez %>% str_extract("_\\d{4}_") %>% gsub("_","",.)
  filez <- filez[!duplicated(yearz,fromLast=FALSE)]
  yearz <- yearz[!duplicated(yearz,fromLast=FALSE)]

  f0 <- 2

  year_list <- mclapply(seq_along(yearz),function(f0){
    print(paste0(f0,"/",length(yearz)," ",yearz[f0]))

    # Error catching
    tryCatch({

    # Load
    deu <- st_read(paste0(filez[f0],".shp"),quiet=TRUE) %>% fix_geom()
    names(deu)[!grepl("geometry",names(deu))] <- names(deu)[!grepl("geometry",names(deu))] %>% toupper()

    # Clean up variables
    deu$ADM0_NAME <- c("German Union","German Empire","Germany")[c(y0==1,y0%in%c(2,3),y0%in%c(4,5))]
    deu$ADM0_ISO3 <- "DEU"
    deu$ADM0_CODE <- countrycode("DEU","iso3c","iso3n")

  
    if(y0%in%c(1:3)){
      deu$ADM1_NAME <- deu$LAND %>% as.character()
      deu$ADM1_CODE <- deu$LAND
      deu$ADM2_NAME <- deu$NAME %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
      deu$ADM2_CODE <- deu$ID
      deu$AUTONUMBER <- deu$ID
    }
    if(y0%in%c(4,5)){
      deu$ADM1_NAME <- deu$KREIS_KENN %>% as.character() %>% substr(1,2)
      deu$ADM1_CODE <- deu$ADM1_NAME %>% as.factor() %>% as.numeric()
      deu$ADM2_NAME <- deu$GEN %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
      deu$ADM2_CODE <- deu$KREIS_KENN
      deu$AUTONUMBER <- deu$KREIS_KENN
    }
    deu$MPIDR_YEAR <- yearz[f0]
    deu <- deu %>% dplyr::select(AUTONUMBER,ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,MPIDR_YEAR) %>% data.table::setnames("AUTONUMBER","MPIDR_ID")

    # plot(deu["ADM1_NAME"]); dev.off()

    # Save in native projection
    map <- deu

    # Simplify polygons
    if(object.size(map) %>% format(units="Mb")>2){
      t1_ <- Sys.time()
      map <- rmapshaper::ms_simplify(map,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
      object.size(map) %>% format(units="Mb") %>% paste0("DEU ",yearz[f0] ," ",.) %>% print()
      t2_ <- Sys.time()
      print(t2_-t1_)
    }

    # Dissolve
    map2 <- map
    map1 <- rmapshaper::ms_dissolve(map2,field="ADM1_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),sys=T)
    map0 <- rmapshaper::ms_dissolve(map1,field="ADM0_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE"),sys=T)
    # plot(map2["ADM1_NAME"]); dev.off()
    map0 <- map0 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
    map1 <- map1 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
    map2 <- map2 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
    write_sf(map0,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_DEU_",yearz[f0],"_ADM0_wgs.geojson"),delete_dsn=TRUE)
    write_sf(map1,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_DEU_",yearz[f0],"_ADM1_wgs.geojson"),delete_dsn=TRUE)
    write_sf(map2,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_DEU_",yearz[f0],"_ADM2_wgs.geojson"),delete_dsn=TRUE)
    

    # if(!dir.exists(paste0("Admin/MPIDR/Processed"))){dir.create(paste0("Admin/MPIDR/Processed"))}
    # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_DEU_",yearz[f0],"_utm.csv"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
    # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_DEU_",yearz[f0],"_utm.geojson"),delete_dsn=TRUE)

    # Reproject to EPSG:4326
    # map <- map %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
    # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_DEU_",yearz[f0],"_wgs.csv"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
    # write_sf(map,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_DEU_",yearz[f0],"_ADM2_wgs.geojson"),delete_dsn=TRUE)

    # Error catching
    },error=function(e){print(paste0("ERROR: ",f0," ",yearz[f0]))})

  },mc.cores=min(length(yearz),detectCores()-1))


})





############
# AUH
############

yrz_0 <- dir("Admin/MPIDR/Zips/AUH") %>% str_extract("\\d{4}") %>% sort()
y0 <- 1
print(yrz_0[y0])

# Unzip
temp <- tempdir()
con <- unzip(paste0("Admin/MPIDR/Zips/AUH/",dir("Admin/MPIDR/Zips/AUH")[grep(yrz_0[y0],dir("Admin/MPIDR/Zips/AUH"))]), exdir = temp)
filez <- con[grep("Austro_Hungarian",con)] %>% gsub(".dbf|.prj|.sbn|.sbx|.shp|.shp.xml|.shx|.xml","",.) %>% unique()
yearz <- filez %>% str_extract("_\\d{4}_") %>% gsub("_","",.)
filez <- filez[!duplicated(yearz,fromLast=FALSE)]
yearz <- yearz[!duplicated(yearz,fromLast=FALSE)]

f0 <- 1
{
  print(paste0(f0,"/",length(yearz)," ",yearz[f0]))

  # Load
  auh <- st_read(paste0(filez[f0],".shp"),quiet=TRUE) %>% fix_geom()

  # Clean up variables
  auh$ADM0_NAME <- "Austro-Hungarian Empire"
  auh$ADM0_ISO3 <- "AUH"
  auh$ADM0_CODE <- countrycode("AUT","iso3c","iso3n")
  auh$ADM1_NAME <- auh$LAND %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
  auh$ADM1_CODE <- auh$REGION
  auh$ADM2_NAME <- auh$NAME %>% as.character() %>% tolower() %>% tools::toTitleCase() %>% gsub("\\/","-",.) %>% gsub("\\\n","",.) %>% gsub("[^[:alnum:] ]", " ", .) %>% str_trim() %>% str_squish()
  auh$ADM2_CODE <- auh$ID_1
  auh$MPIDR_YEAR <- yearz[f0]
  auh <- auh %>% dplyr::select(AUTONUMBER,ADM0_ISO3,ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE,ADM2_NAME,ADM2_CODE,MPIDR_YEAR) %>% data.table::setnames("AUTONUMBER","MPIDR_ID")

  # Save in native projection
  map <- auh

  # Simplify polygons
  if(object.size(map) %>% format(units="Mb")>2){
    t1_ <- Sys.time()
    map <- rmapshaper::ms_simplify(map,keep=.01,keep_shapes=T,sys=T) %>% fix_geom()
    object.size(map) %>% format(units="Mb") %>% paste0("AUH ",yearz[f0] ," ",.) %>% print()
    t2_ <- Sys.time()
    print(t2_-t1_)
  }

  # Dissolve
  map2 <- map
  map1 <- rmapshaper::ms_dissolve(map2,field="ADM1_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE","ADM1_NAME","ADM1_CODE"),sys=T)
  map0 <- rmapshaper::ms_dissolve(map1,field="ADM0_NAME",copy_fields=c("MPIDR_ID","ADM0_ISO3","ADM0_NAME","ADM0_CODE"),sys=T)
  # plot(map2["ADM1_NAME"]); dev.off()
  map0 <- map0 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
  map1 <- map1 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
  map2 <- map2 %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
  write_sf(map0,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_AUH_",yearz[f0],"_ADM0_wgs.geojson"),delete_dsn=TRUE)
  write_sf(map1,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_AUH_",yearz[f0],"_ADM1_wgs.geojson"),delete_dsn=TRUE)
  write_sf(map2,dsn=paste0("Admin/MPIDR/Simplified/MPIDR_AUH_",yearz[f0],"_ADM2_wgs.geojson"),delete_dsn=TRUE)

  # plot(map2["ADM1_NAME"]); dev.off()

  # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_AUH_",yearz[f0],"_laea.csv"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
  # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_AUH_",yearz[f0],"_laea.geojson"),delete_dsn=TRUE)

  # Reproject to EPSG:4326
  # map <- map %>% st_transform(st_crs("EPSG:4326")) %>% fix_geom()
  # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_AUH_",yearz[f0],"_wgs.csv"),layer_options = "GEOMETRY=AS_WKT", delete_dsn=TRUE)
  # write_sf(map,dsn=paste0("Admin/MPIDR/Processed/MPIDR_AUH_",yearz[f0],"_ADM2_wgs.geojson"),delete_dsn=TRUE)
}