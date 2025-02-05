
polyz=map
pointz=cst_sf
# varz=char_varz[!char_varz%in%names(map)];funz=function(x){paste0(unique(x),collapse="|")}
varz=mean_varz;funz=function(x){mean(x,na.rm=T)}
varz=sum_varz;funz=function(x){sum(x,na.rm=T)}
na_val=NA
drop_na_cols = FALSE

names(cst_sf)

point2poly_simp_ <- function (pointz, polyz, varz, funz = list(function(x) {
    sum(x, na.rm = TRUE)
}), na_val = NA, drop_na_cols = FALSE){
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
    if (class(na_val) == "logical") {
        na_val <- list(na_val)
    }
    if (sf::st_crs(pointz) != sf::st_crs(polyz)) {
        polyz <- sf::st_transform(polyz, crs = sf::st_crs(pointz))
    }
    pointz_dt <- data.table::as.data.table(pointz)
    pointz_dt_NACols <- sapply(pointz_dt, function(x) {
        all(is.na(x))
    })
    if (drop_na_cols == TRUE) {
        pointz_dt <- pointz_dt[, pointz_dt_NACols == FALSE, with = FALSE]
    }
    polyz$polygon_id <- 1:nrow(polyz)
    suppressWarnings({
        suppressMessages(Point_Overlay <- sf::st_within(pointz, 
            polyz))
    })
    ErrorCheck <- sapply(Point_Overlay, function(x) length(x))
    pointz_dt$polygon_id <- NA
    pointz_dt$polygon_id[ErrorCheck == 1] <- polyz$polygon_id[unlist(Point_Overlay[ErrorCheck == 
        1])]
    pointz$polygon_id <- NA
    pointz$polygon_id[ErrorCheck == 1] <- polyz$polygon_id[unlist(Point_Overlay[ErrorCheck == 
        1])]
    ClassTypes_Variables <- data.table::data.table(Varz = varz, 
        Funz = funz, NAval = na_val)
    ClassTypes_Variables <- lapply(1:length(ClassTypes_Variables$Varz), 
        function(sub_iter) {
            Varz_Int <- unlist(ClassTypes_Variables$Varz[sub_iter])
            Funz_Int <- unlist(ClassTypes_Variables$Funz[sub_iter], 
                recursive = TRUE)
            NAValue <- unlist(ClassTypes_Variables$NAval[sub_iter], 
                recursive = TRUE)
            OutputMatrix <- data.table::data.table(Varz = Varz_Int, 
                Funz = Funz_Int, NAval = NAValue)
            return(OutputMatrix)
        })
    ClassTypes_Variables <- data.table::rbindlist(ClassTypes_Variables)
    Aggregation_Matrix <- lapply(1:nrow(ClassTypes_Variables), 
        function(sub_iter) {print(sub_iter)
            Select_Columns <- pointz_dt[, names(pointz_dt) %in% 
                c(ClassTypes_Variables$Varz[sub_iter], "polygon_id"), 
                with = FALSE]
            names(Select_Columns) <- c("var", "polygon_id")
            IntermediateFunction <- ClassTypes_Variables$Funz[sub_iter][[1]]
            suppressWarnings({
                Output <- by(data = Select_Columns, INDICES = Select_Columns$polygon_id, 
                  FUN = function(x) {
                    IntermediateFunction(x$var)
                  }, simplify = TRUE)
                Output <- data.table::data.table(polygon_id = as.numeric(names(Output)), 
                  V1 = c(Output))
            })
            names(Output)[2] <- ClassTypes_Variables$Varz[sub_iter]
            return(Output)
        })
    Empty_Sets <- sapply(Aggregation_Matrix, function(x) is.null(x) || 
        grepl("Error", x))
    if (TRUE %in% Empty_Sets) {
        Aggregation_Matrix <- Aggregation_Matrix[Empty_Sets == 
            FALSE]
    }
    Combined_Matrix <- purrr::reduce(Aggregation_Matrix, dplyr::full_join, 
        by = "polygon_id")
    if (any(is.na(Combined_Matrix$polygon_id))) {
        Combined_Matrix <- Combined_Matrix[is.na(Combined_Matrix$polygon_id) == 
            FALSE, ]
    }
    Coordinates <- sf::st_geometry(polyz)
    sf::st_geometry(polyz) <- NULL
    polyz_ <- dplyr::left_join(polyz, Combined_Matrix, by = "polygon_id")
    for (iter in 1:length(ClassTypes_Variables$Varz)) {
        VectorSubset <- polyz_[, names(polyz_) %in% ClassTypes_Variables$Varz[iter]]
        VectorSubset[is.na(VectorSubset)] <- ClassTypes_Variables$NAval[iter]
        polyz_[, names(polyz_) %in% ClassTypes_Variables$Varz[iter]] <- VectorSubset
    }
    sf::st_geometry(polyz_) <- Coordinates
    classez <- sapply(as.data.frame(pointz, stringsAsFactors = FALSE)[, 
        unique(unlist(varz))], class)
    polyz_ng <- sf::st_drop_geometry(polyz_)
    for (cl0 in 1:length(classez)) {
        polyz_ng[, names(classez[cl0])] <- methods::as(polyz_ng[, 
            names(classez[cl0])], classez[cl0])
    }
    polyz_ <- sf::st_set_geometry(polyz_ng, polyz_$geometry)
    rm(polyz_ng)
    return(polyz_)
}


oc_chars_0 <- point2poly_simp_(polyz=map,pointz=cst_sf,varz=char_varz[!char_varz%in%names(map)],funz=function(x){paste0(unique(x),collapse="|")},na_val=NA)