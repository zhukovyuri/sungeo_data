pointz=cst_sf
polyz=map %>% sf::st_buffer(0)
poly_id=idvar
methodz=c("pw")
pop_raster=r_crop
varz=list(sum_varz,mean_varz)
char_varz=char_varz
funz=list(function(x,w){sum(x*w,na.rm=T)},function(x,w){weighted.mean(x,w,na.rm=T)})
return_tess=TRUE
char_methodz = "aw"
char_assign = "biggest_overlap"
seed = 1

point2poly_tess_ <- function (pointz, polyz, poly_id, char_methodz = "aw", methodz = "aw", 
    pop_raster = NULL, varz = NULL, char_varz = NULL, char_assign = "biggest_overlap", 
    funz = function(x, w) {
        stats::weighted.mean(x, w, na.rm = TRUE)
    }, return_tess = FALSE, seed = 1) {
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
            polyz_u <- fix_geom(sf::st_union(polyz))
        })
    })
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
    if (nrow(pointz_crop) == 1 | length(unique(st_geometry(pointz_crop))) == 
        1) {
        suppressWarnings({
            suppressMessages({
                geo_vor <- sf::st_as_sf(data.frame(x = NA, geometry = sf::st_geometry(polyz_u)))
            })
        })
    }
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
    sum(sf::st_is_empty(polyz))
    polyz_ <- poly2poly_ap_(poly_from = geo_vor, poly_to = polyz, 
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









poly_from = geo_vor
poly_to = polyz
poly_to_id = poly_id
geo_vor = geo_vor
methodz = methodz
pop_raster = pop_raster
varz = varz
char_methodz = char_methodz
char_varz = char_varz
char_assign = char_assign
funz = funz
seed = seed

raster::plot(r_crop)

poly2poly_ap_ <- function (poly_from, poly_to, poly_to_id, geo_vor = NULL, methodz = "aw", 
    char_methodz = "aw", pop_raster = NULL, varz = NULL, char_varz = NULL, 
    char_assign = "biggest_overlap", funz = function(x, w) {
        stats::weighted.mean(x, w, na.rm = TRUE)
    }, seed = 1) {
    set.seed(seed)
    if (length(varz) != length(funz) && is.list(varz) && is.list(funz)) {
        stop("ERROR: Length of variable list does not equal length of function list. Pleasse ensure that each set of variables correspsonds to a specific function argument.")
    }
    if (class(varz) == "character") {
        varz <- list(varz)
    }
    if (class(funz) != "list") {
        funz <- list(funz)
    }
    funz <- lapply(funz, function(sub_iter) {
        if (class(sub_iter) %in% "function" == FALSE && class(sub_iter) %in% 
            "character") {
            funzInt <- get(sub_iter, mode = "function")
            return(funzInt)
        }
        else {
            return(sub_iter)
        }
    })
    if ("pw" %in% methodz & length(pop_raster) == 0) {
        stop("No population raster provided.")
    }
    if ("aw" %in% methodz | "aw" %in% char_methodz) {
        poly_from$AREA_TOTAL <- as.numeric(sf::st_area(poly_from))
    }
    if ("pw" %in% methodz) {
        poly_from$POP_TOTAL <- raster::extract(pop_raster, methods::as(poly_from, 
            "Spatial"), fun = sum, na.rm = TRUE)
    }
    poly_to$Return_ID <- 1:nrow(poly_to)
    suppressWarnings({
        int_1 <- suppressMessages(sf::st_buffer(sf::st_intersection(sf::st_buffer(poly_from, 
            dist = 0), sf::st_buffer(poly_to, dist = 0)), dist = 0))
    })
    # Drop empty geometries
    if(any(sf::st_is_empty(int_1))){int_1 <- int_1[!sf::st_is_empty(int_1),]}

    if ("aw" %in% methodz | "aw" %in% char_methodz) {
        int_1$AREA_INT <- as.numeric(sf::st_area(int_1))
        int_1$AREA_W <- int_1$AREA_INT/int_1$AREA_TOTAL
    }
    if ("pw" %in% methodz) {
        int_1$POP_INT <- raster::extract(pop_raster, int_1, fun = sum, 
            na.rm = TRUE)
        int_1$POP_W <- int_1$POP_INT/int_1$POP_TOTAL
    }
    int_1_dt <- data.table::data.table(int_1)
    int_1_dt$geometry <- NULL
    if ("pw" %in% methodz) {
        if (sum(is.na(int_1_dt$POP_INT)) == 1) {
            suppressWarnings({
                w <- suppressMessages(as.matrix(stats::as.dist(raster::pointDistance(as.data.frame(sf::st_coordinates(sf::st_centroid(int_1))), 
                  lonlat = TRUE))))
            })
            diag(w) <- NA
            int_1_dt$POP_INT[is.na(int_1_dt$POP_INT)] <- mean(int_1_dt$POP_INT[t(apply(w, 
                1, order)[1:min(10, nrow(int_1)), is.na(int_1$POP_INT)])], 
                na.rm = TRUE)
            int_1_dt$POP_W[is.na(int_1_dt$POP_W)] <- int_1_dt$POP_INT[is.na(int_1_dt$POP_W)]/int_1_dt$POP_TOTAL[is.na(int_1_dt$POP_W)]
        }
        if (sum(is.na(int_1_dt$POP_INT)) > 1) {
            suppressWarnings({
                w <- suppressMessages(as.matrix(stats::as.dist(raster::pointDistance(as.data.frame(sf::st_coordinates(sf::st_centroid(int_1))), 
                  lonlat = TRUE))))
            })
            diag(w) <- NA
            int_1_dt$POP_INT[is.na(int_1_dt$POP_INT)] <- apply(t(apply(w, 
                1, order)[1:min(10, nrow(int_1)), is.na(int_1$POP_INT)]), 
                1, function(x) {
                  mean(int_1$POP_INT[x], na.rm = TRUE)
                })
            int_1_dt$POP_W[is.na(int_1_dt$POP_W)] <- int_1_dt$POP_INT[is.na(int_1_dt$POP_W)]/int_1_dt$POP_TOTAL[is.na(int_1_dt$POP_W)]
        }
    }
    if (!is.null(geo_vor)) {
        geo_vor_dt <- geo_vor
        sf::st_geometry(geo_vor_dt) <- NULL
        geo_vor_dt <- data.table::data.table(Unique_ID = geo_vor_dt$Unique_ID)
        Aggregation_Matrix <- data.table::data.table(dplyr::full_join(geo_vor_dt, 
            int_1_dt, by = "Unique_ID"))
    }
    else {
        poly_from$Unique_ID <- 1:nrow(poly_from)
        geo_vor_dt <- poly_from
        sf::st_geometry(geo_vor_dt) <- NULL
        geo_vor_dt <- data.table::data.table(Unique_ID = geo_vor_dt$Unique_ID)
        sf::st_geometry(geo_vor_dt) <- sf::st_geometry(poly_from)
        suppressWarnings({
            suppressMessages(Aggregation_Matrix <- data.table::data.table(sf::st_intersection(geo_vor_dt, 
                int_1)))
        })
    }
    ClassTypes_Variables <- data.table::data.table(Varz = varz, 
        Funz = funz, methodz = methodz)
    ClassTypes_Variables <- lapply(1:length(ClassTypes_Variables$Varz), 
        function(sub_iter) {
            Varz_Int <- unlist(ClassTypes_Variables$Varz[sub_iter])
            Funz_Int <- unlist(ClassTypes_Variables$Funz[sub_iter], 
                recursive = TRUE)
            Methodz_Int <- unlist(ClassTypes_Variables$methodz[sub_iter], 
                recursive = TRUE)
            NAValue <- unlist(ClassTypes_Variables$NAval[sub_iter], 
                recursive = TRUE)
            OutputMatrix <- data.table::data.table(Varz = Varz_Int, 
                Funz = Funz_Int, methodz = Methodz_Int)
            return(OutputMatrix)
        })
    ClassTypes_Variables <- data.table::rbindlist(ClassTypes_Variables, 
        fill = TRUE)
    if (any("pw" %in% char_methodz)) {
        CharacterVariables <- data.table::data.table(Varz = char_varz, 
            Funz = NA, methodz = "pw")
    }
    else {
        CharacterVariables <- data.table::data.table(Varz = char_varz, 
            Funz = NA, methodz = "aw")
    }
    if (is.null(char_varz) == FALSE) {
        ClassTypes_Variables <- data.table::rbindlist(list(ClassTypes_Variables, 
            CharacterVariables), fill = TRUE)
    }
    ClassTypes_Variables$Character <- FALSE
    ClassTypes_Variables$Character[ClassTypes_Variables$Varz %in% 
        char_varz] <- TRUE
    if (any(ClassTypes_Variables$Character)) {
        Character_Subset <- ClassTypes_Variables[ClassTypes_Variables$Character %in% 
            TRUE, ]
        Character_Aggregation_Matrix <- lapply(1:nrow(Character_Subset), 
            function(sub_iter) {
                if ("biggest_overlap" %in% char_assign) {
                  if ("aw" %in% Character_Subset$methodz[sub_iter]) {
                    locVar <- which(names(Aggregation_Matrix) %in% 
                      c("Return_ID", "AREA_W", Character_Subset$Varz[sub_iter]))
                    Internal_Matrix <- Aggregation_Matrix[, locVar, 
                      with = FALSE]
                    names(Internal_Matrix)[!names(Internal_Matrix) %in% 
                      c("Return_ID", "AREA_W")] <- "var_"
                    Internal_Matrix <- by(data = Internal_Matrix, 
                      INDICES = Internal_Matrix$Return_ID, FUN = function(x) {
                        paste0(unique(x$var_[x$AREA_W %in% max(x$AREA_W, 
                          na.rm = TRUE)]), collapse = "|")
                      }, simplify = TRUE)
                    Internal_Matrix <- data.table::data.table(Return_ID = as.numeric(names(Internal_Matrix)), 
                      V1 = c(Internal_Matrix))
                    names(Internal_Matrix)[2] <- Character_Subset$Varz[sub_iter]
                  }
                  if ("pw" %in% Character_Subset$methodz[sub_iter]) {
                    locVar <- which(names(Aggregation_Matrix) %in% 
                      c("Return_ID", "POP_W", Character_Subset$Varz[sub_iter]))
                    Internal_Matrix <- Aggregation_Matrix[, locVar, 
                      with = FALSE]
                    names(Internal_Matrix)[names(Internal_Matrix) %in% 
                      c("Return_ID", "POP_W") == FALSE] <- "var_"
                    Internal_Matrix <- Internal_Matrix[, list(paste(unique(var_[POP_W %in% 
                      max(POP_W, na.rm = TRUE)]), collapse = "|")), 
                      by = list(Return_ID)]
                    names(Internal_Matrix)[2] <- Character_Subset$Varz[sub_iter]
                  }
                }
                if ("all_overlap" %in% char_assign) {
                  if ("aw" %in% Character_Subset$methodz[sub_iter]) {
                    locVar <- which(names(Aggregation_Matrix) %in% 
                      c("Return_ID", "AREA_W", Character_Subset$Varz[sub_iter]))
                    Internal_Matrix <- Aggregation_Matrix[, locVar, 
                      with = FALSE]
                    names(Internal_Matrix)[names(Internal_Matrix) %in% 
                      c("Return_ID", "AREA_W") == FALSE] <- "var_"
                    Internal_Matrix <- Internal_Matrix[, list(paste(unique(var_), 
                      collapse = "|")), by = list(Return_ID)]
                    names(Internal_Matrix)[2] <- Character_Subset$Varz[sub_iter]
                  }
                  if ("pw" %in% Character_Subset$methodz[sub_iter]) {
                    locVar <- which(names(Aggregation_Matrix) %in% 
                      c("Return_ID", "POP_W", Character_Subset$Varz[sub_iter]))
                    Internal_Matrix <- Aggregation_Matrix[, locVar, 
                      with = FALSE]
                    names(Internal_Matrix)[names(Internal_Matrix) %in% 
                      c("Return_ID", "POP_W") == FALSE] <- "var_"
                    Internal_Matrix <- Internal_Matrix[, list(paste(unique(var_), 
                      collapse = "|")), by = list(Return_ID)]
                    names(Internal_Matrix)[2] <- Character_Subset$Varz[sub_iter]
                  }
                }
                return(Internal_Matrix)
            })
        Empty_Sets <- sapply(Character_Aggregation_Matrix, function(x) is.null(x) || 
            grepl("Error", x))
        if (TRUE %in% Empty_Sets) {
            Character_Aggregation_Matrix <- Character_Aggregation_Matrix[Empty_Sets == 
                FALSE]
        }
        Character_Aggregation_Matrix <- purrr::reduce(Character_Aggregation_Matrix, 
            dplyr::full_join, by = "Return_ID")
    }
    Numeric_Subset <- ClassTypes_Variables[ClassTypes_Variables$Character %in% 
        FALSE, ]
    Numeric_Aggregation_Matrix <- lapply(1:nrow(Numeric_Subset), 
        function(sub_iter) {
            if ("aw" %in% Numeric_Subset$methodz[sub_iter]) {
                locVar <- which(names(Aggregation_Matrix) %in% 
                  c("Return_ID", "AREA_W", Numeric_Subset$Varz[sub_iter]))
            }
            if ("pw" %in% Numeric_Subset$methodz[sub_iter]) {
                locVar <- which(names(Aggregation_Matrix) %in% 
                  c("Return_ID", "POP_W", Numeric_Subset$Varz[sub_iter]))
            }
            Internal_Matrix <- data.table::as.data.table(Aggregation_Matrix[, 
                locVar, with = FALSE])
            if ("aw" %in% Numeric_Subset$methodz[sub_iter]) {
                names(Internal_Matrix)[!names(Internal_Matrix) %in% 
                  c("Return_ID", "AREA_W")] <- "var_"
            }
            if ("pw" %in% Numeric_Subset$methodz[sub_iter]) {
                names(Internal_Matrix)[!names(Internal_Matrix) %in% 
                  c("Return_ID", "POP_W")] <- "var_"
            }
            IntermediateFunction <- Numeric_Subset$Funz[sub_iter][[1]]
            searchWeight <- c("weighted", "w")
            if (grepl(paste(searchWeight, collapse = "|"), paste(as.character(attributes(IntermediateFunction)$srcref), 
                collapse = ""))) {
                if ("aw" %in% Numeric_Subset$methodz[sub_iter]) {
                  Output <- by(data = Internal_Matrix, INDICES = Internal_Matrix$Return_ID, 
                    FUN = function(x) {
                      IntermediateFunction(x$var_, w = x$AREA_W)
                    }, simplify = TRUE)
                  Output <- data.table::data.table(Return_ID = as.numeric(names(Output)), 
                    V1 = c(Output))
                }
                if ("pw" %in% Numeric_Subset$methodz[sub_iter]) {
                  Output <- by(data = Internal_Matrix, INDICES = Internal_Matrix$Return_ID, 
                    FUN = function(x) {
                      IntermediateFunction(x$var_, w = x$POP_W)
                    }, simplify = TRUE)
                  Output <- data.table::data.table(Return_ID = as.numeric(names(Output)), 
                    V1 = c(Output))
                }
            }
            else {
                suppressWarnings({
                  Output <- by(data = Internal_Matrix, INDICES = Internal_Matrix$Return_ID, 
                    FUN = function(x) {
                      IntermediateFunction(x$var_)
                    }, simplify = TRUE)
                  Output <- data.table::data.table(Return_ID = as.numeric(names(Output)), 
                    V1 = c(Output))
                })
            }
            if ("pw" %in% Numeric_Subset$methodz[sub_iter]) {
                names(Output)[2] <- paste0(Numeric_Subset$Varz[sub_iter], 
                  "_pw")
            }
            if ("aw" %in% Numeric_Subset$methodz[sub_iter]) {
                names(Output)[2] <- paste0(Numeric_Subset$Varz[sub_iter], 
                  "_aw")
            }
            return(Output)
        })
    Empty_Sets <- sapply(Numeric_Aggregation_Matrix, function(x) is.null(x) || 
        grepl("Error", x))
    if (TRUE %in% Empty_Sets) {
        Numeric_Aggregation_Matrix <- Numeric_Aggregation_Matrix[Empty_Sets == 
            FALSE]
    }
    Numeric_Aggregation_Matrix <- purrr::reduce(Numeric_Aggregation_Matrix, 
        dplyr::full_join, by = "Return_ID")
    if (any(ClassTypes_Variables$Character)) {
        Output_Matrix <- purrr::reduce(list(Character_Aggregation_Matrix, 
            Numeric_Aggregation_Matrix), dplyr::full_join, by = "Return_ID")
    }
    else {
        Output_Matrix <- Numeric_Aggregation_Matrix
    }
    polyz_ <- dplyr::left_join(poly_to, Output_Matrix, by = "Return_ID")
    polyz_$Return_ID <- NULL
    polyz_ <- sf::st_cast(polyz_, "MULTIPOLYGON")
    return(polyz_)
}
