#############################################
# Function to check and fix broken geometries
#############################################


fix_geom <- function(x,self_int=TRUE){
  if(self_int){
  if(sum(grepl("Self-inter",st_is_valid(x,reason=T)))>0){
    suppressMessages(
      x <- x %>% st_buffer(dist=0)
      )
  }}
  if(!self_int){
  if(sum(!st_is_valid(x))>0){
    suppressMessages(
      x <- x %>% st_buffer(dist=0)
      )
  }}
  x
}


############################################
# Custom setnames function
############################################

setnames2 <- function(x, old, new, skip_absent=TRUE) {
  if (!skip_absent) {
    setnames(x, old, new)
  } else {
    ix <- match(names(x), old, 0L)
    setnames(x, old[ix], new[ix])
  }
}

############################################
# Function to aggregate points over polygons
############################################

pinp_fun <- function(polyz,pointz,varz,funz=function(x){sum(x,na.rm=T)},na_val=NA){
  # Empty points layer
  pointz_dt <- pointz[1,] %>% as.data.table()
  pointz_dt[,c(varz) := lapply(1:length(varz),function(.){na_val})] %>% (function(.){.[,o0 := 1]})
  # Match points to polygons
  o0 <- suppressMessages(
      pointz %>% st_within(polyz) %>% as.data.table()
      )
  # Add polygon index to point layer
  if(nrow(o0)>0){
    pointz_dt <- pointz %>% as.data.table() %>% (function(.){.[o0$row.id,o0 := o0$col.id]}) %>% select(-geometry)
  }
  # Aggregate
  pointz_agg <- lapply(seq_along(varz),function(j0){
    int_1_ <- pointz_dt[,list(w = funz(get(varz[j0]))),by=o0] %>% setnames("w",paste0(varz[j0]))
    if(j0>1){int_1_ <- int_1_ %>% select(-o0)}
    int_1_
    }) %>% bind_cols()
  # Merge with polygons
  polyz$o0 <- row.names(polyz)
  polyz_ <- merge(polyz,pointz_agg,by="o0",all.x=T,all.y=F) %>% (function(.){.[order(.$o0 %>% as.numeric()),]}) %>% replace(., is.na(.), na_val) %>% select(-o0)
  # Output
  return(polyz_)
}