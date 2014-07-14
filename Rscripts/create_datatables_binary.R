source('config.R')

create_user_friends_binary <- function(data_path) {
  user_friends = as.data.table(read.table(paste(data_path,"user_friends.tsv",sep=''), 
                                          header=TRUE))
  save(user_friends, file=paste(data_path, "user_friends.rdat", sep=''))
  return(user_friends)
}

create_rec_results_binary <- function(data_path) {
  rec_results = as.data.table(read.table(paste(data_path, "knn_recommender_results.tsv", sep=''), header=TRUE))
  setnames(rec_results, "User_id", "user_id")
  setkey(rec_results, user_id)
  rec_results = rec_results[,list(mean_circle_ndcg=mean(circle_ndcg), 
                                       mean_nonfriend_ndcg=mean(nonfriend_ndcg)), 
                                 by=user_id]
  save(rec_results, file=paste(data_path,"rec_results.rdat", sep=''))  
  return(rec_results)
}

create_social_adopts_binary <- function(user_friends, m_interacts, data_path) {
  fm = m_interacts[user_id %in% unique(user_friends$user_id)]
  setkey(user_friends, user_id, friend_id)
  setkey(fm, user_id)
  mf = merge(fm, user_friends, allow.cartesian=TRUE)
  setnames(mf, "user_id", "source_user")
  setnames(mf, "friend_id", "user_id")
  setkey(mf, user_id, item_id)
  mff = merge(mf, m_interacts) 
  setnames(mff, "user_id", "friend_id")
  setnames(mff, "source_user","user_id")
  setnames(mff, c("popularity.x", "rank.x"), c("popularity", "rank")) 
  mff[,popularity.y:=NULL]
  mff[,rank.y:=NULL]
  social_adopt=mff
  save(social_adopt, file=paste(data_path,"social_adopts.rdat", sep=''))
  return(social_adopt)
}

create_interacts_binary <- function(data_path) {
  items = as.data.table(read.table(paste(data_path, "items.tsv",sep=""), header=TRUE))
  items_ord = items[order(popularity)]
  items_ord[,rank:=1:nrow(items_ord)]
  setkey(items_ord, item_id)
  interacts = as.data.table(read.table(paste(data_path,"user_interacts.tsv",sep=""), header=TRUE, 
                                       stringsAsFactor=FALSE, sep="\t"))
  setkey(interacts, user_id, item_id)
  
  m_interacts= merge(interacts, items_ord)
  m_interacts[,timestamp:=parse_date_time(timestamp,
                                          "%m/%d/%Y %I:%M:%S %p")]
  setkey(m_interacts, user_id, item_id)
  save(m_interacts, file=paste(data_path,"m_interacts.rdat",sep=''))
  return(m_interacts)  
}

Main <- function() {
  rec_results = create_rec_results_binary(config.data_path)
  user_friends = create_user_friends_binary(config.data_path)
  m_interacts = create_interacts_binary(config.data_path)
  
  create_social_adopts_binary(user_friends, m_interacts, config.data_path)
}