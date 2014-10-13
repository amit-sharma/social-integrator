require(ggplot2)
require(reshape2)
source("create_datatables_binary2.R")

compare_interacts_by_user <- function(interact1, interact2, min_date, max_date) {
    agg_by_user1 = interact1[created_time>=min_date & created_time<=max_date,list(num_interacts1=length(rating)),by=username]
    agg_by_user2 = interact2[created_time>=min_date & created_time<=max_date,list(num_interacts2=length(rating)),by=username]
    m = merge(agg_by_user1, agg_by_user2, by="username")
    filtered_m = m[num_interacts1>=5]
    sorted_m = filtered_m[order(num_interacts1)]
    p1 = ggplot(sorted_m, aes(1:length(username), num_interacts1)) + geom_line() +
        geom_line(aes(1:length(username), num_interacts2))# + scale_y_log10()
    print(p1)
    p2 = ggplot(sorted_m, aes(1:length(username), num_interacts2/num_interacts1)) +
        geom_line()
    print(p2)
    return(sorted_m)
}

compare_interacts_by_time <- function(flisten, flove) {
    agg_by_date1 = flisten[, list(num_interacts1=length(rating)), by=created_time]
    agg_by_date2 = flove[, list(num_interacts2=length(rating)), by=created_time]
    m = merge(agg_by_date1, agg_by_date2, by="created_time")
    p = ggplot(m, aes(created_time, num_interacts1)) + geom_line() +
        geom_line(aes(y=num_interacts2)) + scale_y_log10()
    print(p)
}

compare_interacts_by_usertime <- function(flisten, flove, min=1, max=50) {
    agg_by_userdate1 = flisten[,list(num_interacts1=length(rating)), by=c("username", "created_time")]
    agg_by_userdate2 = flove[,list(num_interacts2=length(rating)), by=c("username", "created_time")]
    m = merge(agg_by_userdate1, agg_by_userdate2, by=c("username", "created_time"))
    #sorted_m = m[order(-num_interacts1)]
    p = ggplot(m[min:max], aes(created_time, num_interacts1)) + geom_line() +
        geom_line(aes(y=num_interacts2), color="red") + scale_y_log10() +
        facet_wrap(~username, ncol=5)
    print(p)
}

find_common_interactions <- function(interact1, interact2, min_date, max_date) {
    agg_by_user1 = interact1[created_time>=min_date & created_time<=max_date]
    agg_by_user2 = interact2[created_time>=min_date & created_time<=max_date]
    m = merge(agg_by_user1, agg_by_user2, by=c("username","item_url"))
    print(nrow(m))
    return(m)
}

Main <- function() {
    data_domain = "lastfm"
    min_date = as.Date("2013-11-20")
    max_date = as.Date("2014-02-19")
    love = load(paste(data_path, data_domain,'_', "love",".rdat", sep="")) 
    love = interacts_dt
    listen = load(paste(data_path, data_domain,'_', "listen",".rdat", sep="")) 
    listen = interacts_dt
    flove = love[created_time>=min_date & created_time<=max_date]
    flisten = listen[created_time>=min_date & created_time<=max_date]
    setkey(flove, username, item_url)
    setkey(flisten, username, item_url)
    
    compare_interacts_by_user(flisten, flove, min_date, max_date)
    common =find_common_interactions(unique(flisten), unique(flove), min_date, max_date)
    compare_interacts_by_time(flisten, flove)
    compare_interacts_by_usertime(flisten, flove)
    
}