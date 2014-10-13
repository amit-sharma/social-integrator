require(data.table)

data_path = "/mnt/bigdata/lastfm/partab/"

load_save_interacts <- function(data_domain, interact_type_str) {
  interacts_dt = as.data.table(read.csv(paste(data_path, data_domain,'_', interact_type_str,".tsv", sep=""),
                         colClasses=c("factor", "factor", "character", "factor",
                                      "factor", "integer", "integer"),
                         col.names=c("username", "item_id", "item_url", 
                                     "artist_id", "album_id", "timestamp", "rating")))
  interacts_dt[,created_time:=as.Date(as.POSIXct(timestamp, origin="1970-01-01"))]
  save(interacts_dt, file=paste(data_path, data_domain,'_', interact_type_str,".rdat", sep=""))
                   
}

Main <- function() {
    love = load_save_interacts("lastfm", "love")
    listen = load_save_interacts("lastfm", "listen")
}