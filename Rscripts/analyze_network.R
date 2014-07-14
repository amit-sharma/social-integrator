library(data.table)
library(ggplot2)
library(reshape)
source('config.R')
source('create_datatables_binary.R')

plot_social_adoptions_ratio <- function(social_adopt, m_interacts, user_friends){
  
  user_counts = social_adopt[,list(total_common_likes=length(item_id), 
                                   uniq_common_items = length(unique(item_id))),
                             by=user_id]
  
  mm=m_interacts[,list(num_items_liked=length(item_id), med_rank=as.integer(median(rank)),
  med_pop=as.integer(median(popularity))),by=user_id]
  setkey(mm, user_id)
  setkey(user_counts, user_id)
  mm3 = merge(user_counts, mm)
  setkey(mm3, user_id)
  user_num_friends = user_friends[,list(num_friends=length(friend_id)), by=user_id]
  setkey(user_num_friends, "user_id")
  user_social = merge(mm3, user_num_friends)  
  user_social[,social_adopt_ratio:=total_common_likes/(num_friends*num_items_liked)]
  user_social[,social_uniq_adopt_ratio:=uniq_common_items/num_items_liked]
  setkey(user_social, user_id)
  
  user_social2 = user_social[,list(user_id, social_adopt_ratio, social_uniq_adopt_ratio)]
  user_social2=user_social2[order(social_adopt_ratio)]
  user_social2[,row_id:=1:length(user_id)]
  m_usocial = as.data.table(melt(user_social2, id.vars=c("user_id", "row_id")))
  plot = ggplot(m_usocial, aes(row_id, value, color=variable))+geom_line()
  print(plot)

  #ggplot(mm3, aes(med_pop, uniq/num_items, color=(circle_ndcg-nonfriend_ndcg>0)) + geom_point())
  return(user_social)
}
create_prediction_datatable <-function(user_social, rec_results){
  predict_table = merge(user_social,rec_results)
  predict_table[,label:=(mean_circle_ndcg-mean_nonfriend_ndcg>0)]
  write.csv(predict_table, paste(path, "predict_table.csv",sep=''))
}

plot_social_influence<-function(social_adopt,m_interacts, user_friends){
  social_influence = social_adopt[timestamp.x-timestamp.y<=1]
  plot_social_adoptions_ratio(social_influence, m_interacts, user_friends)
  
}
plot_user_eccentricity<- function(user_social) {
  plot = ggplot(user_social[order(med_pop)], aes(1:length(user_id),med_pop))+geom_line()
  plot= plot + xlab("Users (sorted by the median popularity of their likes)") + ylab("Median popularity of items liked")
  print(plot)
}
Main<- function() {
  setwd("~/code/social-integrator/Rscripts")
  path = config.data_path
  
  load(paste(path, 'm_interacts.rdat',sep=''))
  load(paste(path, 'user_friends.rdat', sep=''))
  load(paste(path, 'social_adopts.rdat', sep=''))
  load(paste(path, 'rec_results.rdat', sep=''))
  
  





  #mm=m[,list(num_items=length(item_id), avg_rank=mean(rank), 
  #           avg_pop=mean(popularity)),by=user_id]
  mm=m_interacts[,list(num_items=length(item_id), med_rank=as.integer(median(rank)), 
           med_pop=as.integer(median(popularity))),by=user_id]
  setkey(mm, user_id)

  mm2 = merge(mm, rec_out)

mm2$k <- NULL
mm2$run_index<-NULL
melt_m=as.data.table(melt(mm2,id.vars=c("user_id", "num_items", "med_rank", "med_pop")))
ggplot(melt_m[order(med_pop)], aes(med_pop, value, color=variable)) + 
  geom_point()+ scale_x_log10()
ggplot(predict_table[order(med_pop)], aes(med_pop, (mean_circle_ndcg-mean_nonfriend_ndcg))) + geom_point()+ scale_x_log10()
ggplot(predict_table[order(med_pop)], aes(med_pop,social_uniq_adopt_ratio,color=(mean_circle_ndcg-mean_nonfriend_ndcg>0))) + geom_point()+ scale_x_log10()



}