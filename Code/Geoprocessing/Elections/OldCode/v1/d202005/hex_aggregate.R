# ssh zhukov@sungeo.isr.umich.edu
# ssh zhukov@zhukov.isr.umich.edu
# nohup R -e 'source("/data/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/hex_aggregate.R")' &
# nohup R -e 'source("~/Dropbox/SUNGEO/Code/ElectionData/Geoprocessing/hex_aggregate.R")' &
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
## Hex-GRID
###############################################
###############################################
###############################################

# List of countries with broken GAUL geometries
fix_gred <- c("ZZZ")

# List of countries to skip
skip_hex <- c("ZZZ")

# List of countries to simplify polygons
simp_gred <- list(USA=.01,AUS=.01,BRA=.01,RUS=.01,CAN=.005,CHN=.01,MEX=.01,CHL=.01,EST=.01,ECU=.01,JPN=.01,PAK=.01,ISL=.01)

# Skip existing files?
skip_existing <- TRUE

# Custom functions
source("../Code/functions.R")

# Set tempdir
temp <- tempdir()

# Load NE crop layer
dir("Admin/NE")
con_ne <- unzip(zipfile = "Admin/NE/ne_10m_admin_0_sovereignty.zip", exdir = temp)
# map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% setnames("iso_a3","ISO3") %>% as.data.table()
map0_ne <- read_sf(con_ne[grep("shp$",con_ne)]) %>% as.data.table() %>% setnames(names(.),toupper(names(.))) %>% setnames("GEOMETRY","geometry")  %>% filter(POP_EST>0) %>% as.data.table()
map0_ne[,ISO3:=countrycode(ISO_A2,"iso2c","iso3c")]
map0_ne[ISO_A2=="XK",ISO3:="XKX"]
map0_ne[is.na(ISO3)|ISO_A2==-99,ISO3:=countrycode(ADMIN,"country.name","iso3c")]
map0_ne <- map0_ne[!is.na(ISO3),] %>% st_as_sf() %>% select(NE_ID,ISO3,ADMIN) %>% setnames("ADMIN","NE_NAME")
sort(unique(map0_ne$ISO3))

# Load Simple crop layer
data(wrld_simpl)
wrld_simpl <- wrld_simpl %>% st_as_sf()
map0_ws <- wrld_simpl %>% filter(POP2005>10000) %>% st_union() %>% ms_simplify(keep=.33)
format(object.size(map0_ws),"Mb")
# plot(map0_ws)
# dev.off()

# # Global Hex grid (cropped)
# t1 <- Sys.time()
# hex_05 <- make_hex_grid(map0_ws %>% as("Spatial"), cell_diameter = .5, clip = FALSE) %>% st_as_sf()
# hex_05$HEX_ID <- 1:nrow(hex_05) %>% sprintf("%05d", .)
# hex_05 <- hex_05 %>% bind_cols(hex_05 %>% st_centroid() %>% st_coordinates() %>% as.data.table() %>% setnames(c("X","Y"),c("HEX_X","HEX_Y")))
# save(hex_05,file="Admin/HEXGRID/hex_05.RData")
# t2 <- Sys.time()
# print(t2-t1)

# # Global Hex grid (full)
# t1 <- Sys.time()
# hex_05_full <- make_hex_grid(wrld_simpl %>% st_as_sf() %>% st_bbox() %>% st_as_sfc() %>% as("Spatial"), cell_diameter = .5, clip = FALSE) %>% st_as_sf()
# hex_05_full$HEX_ID <- 1:nrow(hex_05_full) %>% sprintf("%05d", .)
# hex_05_full <- hex_05_full %>% bind_cols(hex_05_full %>% st_centroid() %>% st_coordinates() %>% as.data.table() %>% setnames(c("X","Y"),c("HEX_X","HEX_Y")))
# save(hex_05_full,file="Admin/HEXGRID/hex_05_full.RData")
# t2 <- Sys.time()
# print(t2-t1)

# Load Hex grid
load("Admin/HEXGRID/hex_05_full.RData")

# # Preview
# plot(hex_05_full$geometry,lwd=.01)
# # plot(hex_05$geometry,lwd=.01,add=T,col="blue")
# plot(map0_ne$geometry,add=T,border="red",lwd=.1)
# # plot(map0_ws,add=T,border="blue",lwd=.1)
# dev.off()
# format(object.size(hex_05_full),"Mb")


####
# Reduce memory overhead
####

wrld_simpl$ISO3
ws_cntz <- sort(unique(wrld_simpl$ISO3))
k0 <- ws_cntz[11]
hex_list <- mclapply(ws_cntz,function(k0){
  if(!file.exists(paste0(temp,"/HEX_",k0,".RData"))){
    tryCatch({
      t1 <- Sys.time()
      print(paste0(k0," starting"))
      map0_sub <- wrld_simpl[wrld_simpl$ISO3%in%k0,] 
      map0_sub <- map0_sub %>% st_transform(st_crs(hex_05_full))
      o0 <- suppressMessages(
        hex_05_full %>% st_centroid() %>% st_within(map0_sub %>% st_buffer(dist=.5)) 
        )
      hex_crop <- hex_05_full[o0 %>% lengths() > 0,]
      hex_crop <- suppressMessages(
        hex_crop[hex_crop %>% st_intersects(map0_sub) %>% lengths() > 0,] %>% mutate(ISO3 = k0) %>% select(ISO3,everything())
        )
      save(hex_crop,file=paste0(temp,"/HEX_",k0,".RData"))
      # png(paste0("Admin/HEXGRID/Maps/000_",k0,".png"),width=10,height=10,units="in",res=150)
      # plot(hex_crop$geometry)
      # plot(map0_sub$geometry,add=T)
      # dev.off()
      t2 <- Sys.time()
      print(paste0(k0," complete"))
      print(t2-t1)
    },error=function(e){print(paste0(k0," ERROR"))})
  }
},mc.cores=detectCores()-2)

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
clea <- clea[ISO3 %in% ws_cntz,]
clea[,.(ISO3,yr)] %>% unique()

# Load coordinates
load("Elections/CLEA/GeoCode/cst_best_2.RData")
# Add country codes
geo_mat[,ISO3 := countrycode(ctr_n,"country.name","iso3c")]
geo_mat[ctr_n%in%"Micronesia",ISO3 := "FSM"]
geo_mat[ctr_n%in%"Kosovo",ISO3 := "XKX"]
geo_mat

# Loop over years
y0 <- 9
yrz <- unique(clea$yr) %>% sort() 
for(y0 in 20:length(yrz)){
  print(yrz[y0])

  # Subset CLEA by year
  clea_yr <- clea[yr%in%yrz[y0],]
  clea_cntz <- clea_yr[,ISO3] %>% unique() %>% sort()

  # Exceptions
  clea_cntz <- clea_cntz[!clea_cntz%in%skip_hex]

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

    # Crop raster by country extent
    b <- as(extent(st_bbox(wrld_simpl[wrld_simpl$ISO3%in%clea_cntz[k0],] )[c(1,3,2,4)] %>% as.numeric()), 'SpatialPolygons')
    crs(b) <- crs(r)
    b <- st_as_sf(b) 
    r_crop <- crop(r, b)

    save(clea_,r_crop,file=paste0(temp,"/CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))
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

    if(!skip_existing|(skip_existing&!paste0("HEX_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/HEX_PointInPoly/"))){

      # Load PG polygons
      load(paste0(temp,"/HEX_",clea_cntz[k0],".RData"))

      # Load country borders
      map0_ <- wrld_simpl[wrld_simpl$ISO3%in%clea_cntz[k0],] %>% st_transform(st_crs(hex_crop))

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )

      # Aggregate over polygons
      clea_chars_0 <- pinp_fun(polyz=hex_crop,pointz=cst_sf,varz=char_varz,funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)
      clea_means_0 <- pinp_fun(polyz=hex_crop,pointz=cst_sf,varz=mean_varz,funz=function(x){mean(x,na.rm=T)},na_val=NA)
      clea_sums_0 <- pinp_fun(polyz=hex_crop,pointz=cst_sf,varz=sum_varz,funz=function(x){sum(x,na.rm=T)},na_val=NA)
      
      # Merge
      idvar <- "HEX_ID"
      clea_hex <- hex_crop %>% 
        merge(clea_chars_0 %>% as.data.table() %>% select(c(idvar,char_varz)),by=idvar,all.x=T,all.y=F) %>% 
        merge(clea_means_0 %>% as.data.table() %>% select(c(idvar,mean_varz)),by=idvar,all.x=T,all.y=F) %>%
        merge(clea_sums_0 %>% as.data.table() %>% select(c(idvar,sum_varz)),by=idvar,all.x=T,all.y=F) %>% as.data.table() %>%
        setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        mutate(YEAR = yrz[y0]) %>% mutate(YRMO = cst_sf$yrmo[1] %>% as.numeric()) %>%
        select(ISO3,YRMO,YEAR,everything()) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
        # summary(clea_hex)

      # Save 
      save(clea_hex,file=paste0("Elections/CLEA/Processed/HEX_PointInPoly/HEX_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/HEX_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_000.png"),width=4,height=4,units="in",res=150)
          plot(clea_hex$geometry,lwd=.25)
          points(as(cst_sf,"Spatial"))
        dev.off()
        for(plot_var in toupper(mean_varz)){
          if(clea_hex[plot_var] %>% na.omit() %>% nrow() > 0){
            clea_sf <- clea_hex %>% st_as_sf()
            png(paste0("Elections/CLEA/Maps/HEX_PointInPoly/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_hex.png"),width=4,height=4,units="in",res=150)
            plot(clea_sf[paste0(plot_var)],lwd=.25)
            dev.off()
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished PointInPoly"))

    # Clean workspace
    rm(list=ls()[grep("clea_hex|_char|_means|_sums|cst_sf",ls())])
    gc(reset=T)

    ###########################
    # Voronoi method
    ###########################

    if(!skip_existing|(skip_existing&!paste0("HEX_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/HEX_Voronoi/"))){


      dir(temp)
      # Load PG polygons
      load(paste0(temp,"/HEX_",clea_cntz[k0],".RData"))

      # Load country borders
      map0_ <- wrld_simpl[wrld_simpl$ISO3%in%clea_cntz[k0],] %>% st_transform(st_crs(hex_crop))

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf,st_bbox(map0_))
        )
      
      # Tesselation
      clea_hex_list <- tess_fun(
        pointz=cst_sf,
        polyz=hex_crop,
        poly_id="HEX_ID",
        methodz=c("aw","pw"),
        pop_raster=r_crop,
        varz=list(sum_varz,mean_varz),
        char_varz=char_varz,
        funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)}),
        return_tess=TRUE
        )
      clea_hex_list

      # Rename variables
      geo_vor <- clea_hex_list[["tess"]] %>%
        setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      clea_hex <- clea_hex_list[["result"]] %>%
        setnames(old=c(paste0(rep(c("cst","cst_n","ctr","ctr_n"),each=2),c("_aw","_pw")),"yr_aw","yrmo_aw","noncontested_aw"),new=c(paste0(rep(c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N"),each=2),c("_AW","_PW")),"YEAR","YRMO","NONCONTESTED"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        select(-one_of("yr_pw","yrmo_pw","noncontested_pw")) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      clea_hex

      # Save 
      save(geo_vor,clea_hex,file=paste0("Elections/CLEA/Processed/HEX_Voronoi/HEX_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        png(paste0("Elections/CLEA/Maps/HEX_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_000_voronoi.png"),width=4,height=4,units="in",res=150)
        plot(geo_vor$geometry,lwd=.25)
        dev.off()
        for(plot_var in toupper(mean_varz)){#print(plot_var)
          if(geo_vor[plot_var] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/HEX_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_voronoi.png"),width=4,height=4,units="in",res=150)
            plot(geo_vor[plot_var],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/HEX_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_pw_hex.png"),width=4,height=4,units="in",res=150)
            plot(clea_hex[paste0(plot_var,"_PW")],lwd=.25)
            dev.off()
            png(paste0("Elections/CLEA/Maps/HEX_Voronoi/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_aw_hex.png"),width=4,height=4,units="in",res=150)
            plot(clea_hex[paste0(plot_var,"_AW")],lwd=.25)
            dev.off()
          }
        }
      },error=function(e){})
    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished Voronoi"))

    # Clean workspace
    rm(list=ls()[grep("clea_hex|_char|_means|_sums|cst_sf",ls())])
    gc(reset=T)

    ###########################
    # Kriging
    ###########################

    if(!skip_existing|(skip_existing&!paste0("HEX_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData")%in%dir("Elections/CLEA/Processed/HEX_Kriging/"))){

       # Load PG polygons
      load(paste0(temp,"/HEX_",clea_cntz[k0],".RData"))

      # Load country borders
      map0_ <- wrld_simpl[wrld_simpl$ISO3%in%clea_cntz[k0],] %>% fix_geom() %>% st_transform(st_crs(hex_crop))

      # Create point layer
      cst_sf <- st_as_sf(clea_mat,coords = c("long", "lat"),crs=st_crs(map0_))
      cst_sf <-  suppressMessages(
        st_crop(cst_sf %>% st_jitter(),st_bbox(map0_) )
        )

      # Ordinary kriging
      clea_hex <- krig_fun(
        pointz=cst_sf,
        polyz=hex_crop,
        varz=mean_varz,
        epsg=NULL,
        epsg_poly=read_sf("EPSG","EPSG_Polygons"),
        messagez=clea_cntz[k0]) %>% as.data.table()

      # Rename variables
      clea_hex[,c("yr","yrmo"):=list(cst_sf$yr[1],cst_sf$yrmo[1])]
      clea_hex <- clea_hex %>%
        setnames(old=c("cst","cst_n","ctr","ctr_n","yr","yrmo"),new=c("CLEA_CST","CLEA_CST_N","CLEA_CTR","CLEA_CTR_N","YEAR","YRMO"),skip_absent=TRUE) %>% 
        select(ISO3,YRMO,YEAR,everything()) %>% 
        setnames(old=names(.)[names(.)!="geometry"],new=names(.)[names(.)!="geometry"] %>% toupper()) %>% st_as_sf()
      
      # Save 
      save(clea_hex,file=paste0("Elections/CLEA/Processed/HEX_Kriging/HEX_CLEA_",clea_cntz[k0],"_",yrz[y0],".RData"))

      # Visualize
      tryCatch({
        for(plot_var in toupper(mean_varz)){#print(plot_var)
          if(clea_hex[paste0(plot_var,"_KR")] %>% na.omit() %>% nrow() > 0){
            png(paste0("Elections/CLEA/Maps/HEX_Kriging/",clea_cntz[k0],"_",yrz[y0],"_",plot_var,"_hex.png"),width=4,height=4,units="in",res=150)
            plot(clea_hex[paste0(plot_var,"_KR")],lwd=.25)
            dev.off()
          }
        }
      },error=function(e){})

    # Close "skip_existing" if statement
    }

    print(paste0(clea_cntz[k0],", ",k0,"/",length(clea_cntz)," -- finished Kriging"))

    # Clean workspace
    rm(list=ls()[grep("clea_hex|_char|_means|_sums|cst_sf",ls())])
    gc(reset=T)

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
# for(f0 in dir(temp)[grep(paste0(yrz[y0],".RData$"),dir(temp))]){
  # file.remove(paste0(temp,"/",f0))
# }

}





