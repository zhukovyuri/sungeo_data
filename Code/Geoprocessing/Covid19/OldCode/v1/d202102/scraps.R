
polyz=map
pointz=jhu_data_t %>% .[get(tvar) %in% t0_,] %>% st_as_sf(coords=c("LONGITUDE","LATITUDE"),na.fail=F,crs=st_crs(map))
poly_id=idvar
varz=c(sum_jhu_data,max_jhu_data)
funz=function(x,w){sum(x*w,na.rm=T)}
# Defaults
char_methodz = "aw"
methodz = "aw"
pop_raster = NULL
char_varz = NULL
char_assign = "biggest_overlap"
return_tess = FALSE
seed = 1


# temporary patch
point2poly_tess_ <- function (pointz, polyz, poly_id, char_methodz = "aw", methodz = "aw", 
    pop_raster = NULL, varz = NULL, char_varz = NULL, char_assign = "biggest_overlap", 
    funz = function(x, w) {
        stats::weighted.mean(x, w, na.rm = TRUE)
    }, return_tess = FALSE, seed = 1){

    set.seed(seed)
    if (class(varz) == "character") {
        varz <- list(varz)
    }
    if (class(funz) == "function") {
        funz <- list(funz)
    }
    if ("pw" %in% methodz & length(pop_raster) == 0) {
        stop("No population raster provided.")
    }
  suppressWarnings({
    suppressMessages({
      # polyz_u <- dplyr::select(fix_geom(rmapshaper::ms_dissolve(polyz)),-1)
      polyz_u <- fix_geom(sf::st_union(polyz))
    })
  })
# st_geometry(polyz_u)  

    suppressWarnings({
        suppressMessages(pointz_crop <- sf::st_crop(sf::st_jitter(pointz), 
            sf::st_bbox(polyz_u)))
    })
    pointz_crop$Unique_ID <- 1:nrow(pointz_crop)
    suppressWarnings({
        suppressMessages(pointz_geom <- sf::st_union(sf::st_geometry(pointz_crop)))
    })
    suppressWarnings({
        geo_vor <- suppressMessages(sf::st_as_sf(data.table::setnames(data.table::as.data.table(sf::st_intersection(sf::st_as_sf(sf::st_cast(sf::st_voronoi(pointz_geom))), 
            polyz_u)), old = "x", new = "geometry")))
    })
    if (nrow(pointz_crop) == 1) {
        suppressWarnings({
        suppressMessages({
            geo_vor <- sf::st_as_sf(data.frame(x=NA,geometry=sf::st_geometry(polyz_u)))
            # geo_vor <-sf::st_intersection(sf::st_as_sf(sf::st_geometry(polyz_u)), sf::st_geometry(polyz_u))
            # geo_vor <- data.table::setnames(geo_vor, "x", "geometry")
        })
        })
    }
    # sf::st_geometry(geo_vor) <- "geometry"
    suppressWarnings({
        suppressMessages(int <- sf::st_intersects(geo_vor, pointz_crop))
    })
    Errors <- sapply(int, function(x) length(x))
    geo_vor$Unique_ID <- NA
    geo_vor$Unique_ID[Errors == 1] <- pointz_crop$Unique_ID[unlist(int[Errors == 
        1])]
    suppressWarnings({
        geo_vor <- suppressMessages(fix_geom(sf::st_intersection(geo_vor, 
            polyz_u)))
    })
    pointz_crop_dt <- pointz_crop
    sf::st_geometry(pointz_crop_dt) <- NULL
    suppressWarnings({
        geo_vor <- suppressMessages(dplyr::left_join(geo_vor, 
            pointz_crop_dt, by = "Unique_ID"))
    })
    polyz_ <- poly2poly_ap(poly_from = geo_vor, poly_to = polyz, 
        poly_to_id = poly_id, geo_vor = geo_vor, methodz = methodz, 
        pop_raster = pop_raster, varz = varz, char_methodz = char_methodz, 
        char_varz = char_varz, char_assign = char_assign, funz = funz, 
        seed = seed)
    polyz_$Unique_ID <- NULL
    geo_vor$Unique_ID <- NULL
    if (!return_tess) {
        return(polyz_)
    }
    if (return_tess) {
        return(list(result = polyz_, tess = geo_vor))
    }
}
