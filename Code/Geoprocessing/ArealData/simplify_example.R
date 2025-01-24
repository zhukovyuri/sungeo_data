# Load packages (install if missing)
list.of.packages <- c("sf","dplyr","V8","rmapshaper","nngeo"); new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]; if(length(new.packages)){install.packages(new.packages,dependencies=TRUE)}; loaded.packages <- lapply(list.of.packages, require, character.only = TRUE); rm(list.of.packages,new.packages,loaded.packages)

# Is mapshaper installed on node?
rmapshaper::check_sys_mapshaper()

# Unzip & load polygons
file.dirz <- "~/Dropbox/SUNGEO/Data/Admin/geoBoundaries/Zips/"
file.zipz <- "geoBoundaries-2_0_1-NPL-ADM2-shp.zip"
map <- paste0(file.dirz,file.zipz) %>% unzip(exdir=tempdir()) %>% grep(".shp$",.,value=T) %>% sf::st_read(quiet=TRUE) %>% sf::st_transform(sf::st_crs("EPSG:4326")) 
# Delete temp files
list.files(tempdir(),pattern=file.zipz %>% gsub("\\.zip$|-shp","",.),full.names=TRUE) %>% file.remove()

##
# Simplify
##

# Vector of proportions of vertices to keep (.01 to 1)
keepz <- 1:100/100

# Simplify at smallest threshold
k0 <- 1
map_ <- rmapshaper::ms_simplify(map,keep=keepz[k0],keep_shapes=TRUE,sys=TRUE)

##
# Fix missing units
##

# map_[3,"geometry"] <- NULL # Remove one geometry set for illustration

# Plan A: increase retention threshold until no missing shapes
while(sum(sf::st_is_empty(map_))>0&k0<length(keepz)){
	k0 <- k0 + 1
	map_ <- rmapshaper::ms_simplify(map,keep=keepz[k0],keep_shapes=TRUE,sys=TRUE)
}
# Plan B: keep lowest threshold, replace missing units with original polygons
if(sum(sf::st_is_empty(map_))>0){
	map_ <- rmapshaper::ms_simplify(map,keep=keepz[1],keep_shapes=TRUE,sys=TRUE)
	map_[sf::st_is_empty(map_),] <- sf::st_is_empty(map_) %>% map[.,] 
}

##
# Fix self-intersections, remove holes
## 

if(mean(sf::st_is_valid(map_))<1){
	suppressWarnings({
		map_ <- sf::st_buffer(map_,0)
		map_ <- nngeo::st_remove_holes(map_)
	})
}
sf::st_is_valid(map_,reason=TRUE)



