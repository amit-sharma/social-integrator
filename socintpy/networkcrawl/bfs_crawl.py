def addUserToQueue(uid, visited_status, usr_queue, priority_finder):
    uid = uid.decode('ascii', 'ignore')
    entry_tuple = [visited_status, next(counter), uid]
    heapq.heappush(usr_queue, entry_tuple)
    priority_finder[uid] = entry_tuple

def removeUserFromQueue(uid, priority_finder):
    uid = uid.decode('ascii', 'ignore')
    entry = priority_finder.pop(uid)
    entry[-1] = "REMOVD"
    return entry[0]

def popUserFromQueue(usr_queue, priority_finder):
    while usr_queue:
        entry = heapq.heappop(usr_queue)
        if entry[-1] !="REMOVD":
            del priority_finder[entry[-1]]
            return entry[-1]
    raise KeyError("popUser from empty usr_queue")
        
print "Friend Explorer program started"

#Reading list of random seeds from database, or current list of users in the db
query = "SELECT uid,visited FROM users"
seed_users = userdb.read(query)
if not seed_users:
    IS_PRELIM_SEED = True
    rfile = open(SEEDUSERS_FILENAME)
    seed_users = []
    for line in rfile:
        uid = line.strip('"\n')
        seed_users.append({'uid': uid, 'visited': 0})

#BIG QUESTION: automatic restarting for those not successfully done.how to do this fairly, since omitting a node changes the network crawled?
"""Loop over all users that are either seed, or present in the database. If they have not been visited, visit them."""
for seed_usr in seed_users:
    if seed_usr['visited'] < 1:
        seed_uname = seed_usr['uid']
        if IS_PRELIM_SEED:
            #call api and store user in database
            user = network.get_user(seed_uname)
            try:
                info = user.get_info()
                info['visited'] = seed_usr['visited']
                addUserToQueue(seed_usr['uid'], seed_usr['visited'], usr_queue, priority_finder)
                write_flag = userdb.writeInfo(info)  
                sleep(1)      
            except pylast.WSError as wse:
                #Checking for rate limit errors
                exitIfRateExceeded(wse.get_id())
                
        else:
            addUserToQueue(seed_usr['uid'],seed_usr['visited'], usr_queue, priority_finder)

try:
    while usr_queue and num_users < MAXUSERS_TOFETCH:
        pprint(usr_queue)
        uname = popUserFromQueue(usr_queue, priority_finder)
        print "\nUser %s selected." %uname
        if userdb.checkVisited(uname):
            logging.info("Duplicate user (uname=%s) found. Ignoring..." % uname)
            continue
            
        friends = None
        try:
            user = network.get_user(uname)
            friends = user.get_friends(limit=MAXFRIENDS_PAGE)
            userdb.addFriends(user, friends)
            info = None
            for friend in friends:
                friend_id = friend.get_id().decode('ascii', 'ignore')
                if not userdb.checkVisited(friend_id): 
                    print "Now processing %s --> %s" % (user, friend_id) 
                    
                    if friend_id not in priority_finder:              
                        info = friend.get_info()
                        info['visited'] = 0
                        write_flag = userdb.writeInfo(info)
                        addUserToQueue(friend_id, 0, usr_queue, priority_finder)
                        logging.debug("Friend %s for user %s added to queue." % (friend, user))
                    else:
                        visited_status = removeUserFromQueue(friend_id, priority_finder)
                        addUserToQueue(friend_id, visited_status-1, usr_queue, priority_finder)
                        userdb.updateVisited(friend_id, visited_status-1)
            #mark user as fully visited
            userdb.markVisited(user)
            num_users += 1
            logging.info("User %s visited." % uname)
        except pylast.WSError as wse:
            fail_queue.append(user.get_name())
            logging.error("WSE:Current user = %s. %s" % (uname,wse))
            print "WSE:Current user = %s. %s" % (uname,wse)
            #Checking for rate limit errors
            exitIfRateExceeded(wse.get_id())
        except pylast.MalformedResponseError as mre:
            fail_queue.append(user.get_name())
            logging.error("MRE:Current user = %s. %s" % (uname,mre))
            print "MRE:Current user = %s. %s" % (uname,mre)
        except pylast.NetworkError as ne:
            fail_queue.append(user.get_name())
            logging.error("NE:Current user = %s. %s" % (uname,ne))
            print "NE:Current user = %s. %s" % (uname,ne)
        finally:
            sleep(1)
    
    if len(fail_queue) == 0:
        status_str = "Network done successfully!"
    else:
        failfile = open("dumps/failed_{0}".format(time()), "wb")
        pickle.dump(fail_queue, failfile)
        failfile.close()
        status_str = "Seed (uname=%s) Network done (with errors)!" % seed_uname
    print status_str
    logging.info(status_str)
    logging.info("Friend Explorer program successfully ended.")
except:
    logging.critical("Unexpected error! %s\nExiting..." % sys.exc_info()[0])
    raise
finally:    
    userdb.close()
