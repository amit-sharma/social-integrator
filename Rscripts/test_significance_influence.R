library(data.table)

mod.test <- function(x1,x2,dif,...){
    avg.x1 <- mean(x1)
    avg.x2 <- mean(x2)
    sd.x1 <- sd(x1)
    sd.x2 <- sd(x2)
    
    sd.comb <- sqrt((sd.x1^2+sd.x2^2)/2)
    n <- length(x1)
    t.val <- (abs(avg.x1-avg.x2))*sqrt(n)/sd.comb
    ncp <- (dif*sqrt(n)/sd.comb)
    p.val <- pt(t.val,n-1,ncp=ncp,lower.tail=FALSE)
    return(p.val)
}

mod.test.paired <- function(x1,x2,dif,...){
    x = x1-x2
    avg.x <- mean(x)
    sd.x <- sd(x)
    
    #sd.comb <- sqrt((sd.x1^2+sd.x2^2)/2)
    sd.comb <- sd.x
    n <- length(x)
    t.val <- (abs(avg.x))*sqrt(n)/sd.comb
    ncp <- (dif*sqrt(n)/sd.comb)
    p.val <- pt(t.val,n-1,ncp=ncp,lower.tail=FALSE)
    return(p.val)
}

test_sig <- function(infdt) {
    print(t.test(infdt[,V3], infdt[,V4], paired=TRUE, alternative="g"))
    diff= 0.1*mean(infdt[,V4])
    print(diff)
    print(mod.test.paired(infdt[,V3], infdt[,V4], dif=diff))
    hist(infdt[V3!=0 |V4!=0,V3-V4],breaks=10000)
    #print(pl)
    
}


plot_distr <- function(dt, binwidth){
    x = dt[,list(mean_val=mean(V3-V4), mean_fr=mean(V3), mean_nonfr=mean(V4)),by=V5]
    p0 = ggplot(dt, aes(V3-V4)) + geom_histogram(binwidth=binwidth, colour="black", fill="white")
    print(p0)
    p = ggplot(x, aes(x=mean_val)) + geom_histogram(binwidth=binwidth, colour="black", fill="white")
    print(p)
    p2 =ggplot(x[order(mean_fr)], aes(1:nrow(x),mean_fr)) + geom_line() + geom_line(aes(y=mean_nonfr))
    print(p2)
    print(paste("Mean of means:",mean(x$mean_val), mean(x$mean_fr), mean(x$mean_nonfr)))
    return(p)
}


plotbox <- function() {
     
}

plot_means <- function(dt){
    x <- dt[,list(Influence=mean(V3-V4), InboundCorr=mean(V3)),by=V6]
    p = ggplot(x, aes(V6, Influence)) + geom_line() + geom_line(aes(y=InboundCorr), color="red") + xlab("M")
    print(p)
    
    p1= ggplot(dt, aes(V3-V4)) + geom_histogram(binwidth=0.01, colour="black", fill="white") + xlab("Copy-influence") +
        theme_bw()
    p0= ggplot(dt, aes(V3)) + geom_histogram(binwidth=0.01, colour="black", fill="white") + xlab("Friends-Overlap") +
        theme_bw()
    grid.arrange(p0,p1,ncol=2)

    return(x)
}

plot_user_distr <- function(path) {
    
}
Main<- function() {
    path = "../examples/compute/"
    
    
    inf_dt6 = as.data.table(read.table(paste(path, "test_results/suscept_testNone_m2050", sep=""), sep="\t", header=FALSE))
    ggplot(inf_dt6, aes(V6, V3)) + geom_line()
    test_sig(inf_dt6)
    sus_rand_dt = as.data.table(read.table(paste(path, "suscept_testrandom50", sep=""), sep="\t", header=FALSE))
    sus_inf_dt = as.data.table(read.table(paste(path, "suscept_testinfluence", sep=""), sep="\t", header=FALSE))
    sus_homo_dt = as.data.table(read.table(paste(path, "suscept_testhomophily50", sep=""), sep="\t", header=FALSE))
    
    p_rand = plot_distr(sus_rand_dt, binwidth=0.0001)
    p_homo = plot_distr(sus_homo_dt, binwidth=0.001)
    p_inf = plot_distr(sus_inf_dt, bindwidth=0.005)
    grid.arrange(p_rand, p_homo, p_inf, ncol=3)
    
    sus_love_songs_dt = as.data.table(read.table(paste(path, "suscept_test_lastfm_simplelovesongsTrue10000None1", sep=""), sep="\t", header=FALSE))
    pl_dt = sus_love_songs_dt
    pldt_by_m = pl_dt[,by=V]
    ggplot()
    
    
    
    sus_love_songs_dt[,V7:="songs_love"]
    sus_listen_songs_dt = as.data.table(read.table(paste(path, "suscept_testlistenFalse_part", sep=""), sep="\t", header=FALSE))
    sus_listen_songs_dt[,V7:="songs_listen"]
    
    sus_love_artists_dt = as.data.table(read.table(paste(path, "suscept_test1loveTrue_part", sep=""), sep="\t", header=FALSE))
    sus_love_artists_dt[,V7:="artists_love"]
    sus_listen_artists_dt = as.data.table(read.table(paste(path, "suscept_testlistenTrue_part", sep=""), sep="\t", header=FALSE))
    sus_listen_artists_dt[,V7:="artists_listen"]
    
    final_df = rbind(sus_love_songs_dt, sus_listen_songs_dt, sus_love_artists_dt, sus_listen_artists_dt)
    final_df10 = final_df[V6==10]
    mean_inf = final_df10[,list(mean_inbound_corr=mean(V3),mean_influence=mean(V3-V4)), by=V7]
    melt_inf = melt(mean_inf, id.vars="V7")
    ggplot(melt_inf, aes(x=V7,y=value, fill=variable)) + geom_bar(stat="identity", position=position_dodge(), colour="black")
    plot_means(sus_love_songs_dt[V6==10])
    plot_means(sus_listen_artists_dt)
    
    dt1  = as.data.table(read.table(paste(path, "fri_results/suscept_test_lastfm_simplelovesongsTrue12000None1", sep=""), 
                                    sep="\t", header=FALSE))
}

newfn <- function(){
    n <- 5000
    
    test1 <- replicate(100,
                       t.test(rnorm(n),rnorm(n,0.05))$p.value)
    table(test1<0.05)
    
    test2 <- replicate(100,
                       t.test(rnorm(n),rnorm(n,0.5))$p.value)
    table(test2<0.05)
    
    test3 <- replicate(100,
                       mod.test(rnorm(n),rnorm(n,0.05),dif=0.3))
    table(test3<0.05)
    
    test4 <- replicate(100,
                       mod.test(rnorm(n),rnorm(n,0.5),dif=0.3))
    table(test4<0.05)
    
    
}