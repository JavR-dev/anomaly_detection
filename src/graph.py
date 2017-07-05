import pdb;
import math;
import time;
import datetime;

###############################################################################################
# class person
############################################################################################### 
# summary: instantiates a person
###############################################################################################
class person:
    ###############################################################################################
    # __init__
    ############################################################################################### 
    # summary: initialize a person
    # input:  
    #        name: numeric id of person
    #        friends: names of friend
    #        purchases: purchases made by this person
    #        total: holds total sum of all purchases (feature not implemented)
    #        local_network: the local network of the person (defined by D)
    ###############################################################################################
    def __init__(self, name = None, friends = None, purchases = None, total = None, network = None ):
      self.name = name;
      self.friends = friends;     # unique set of tupples, with lower id set first. 
      self.purchases = purchases;   
      #self.total = total;           
      self.local_network = None         # social network based on D
      self.anomalous = False

    ###############################################################################################
    # add_friend
    ############################################################################################### 
    # summary: adds a friendship
    # input:  None
    # output: adds a friendship
    ###############################################################################################
    def add_friend(self, friendship = None):
     self.friends.add( friendship )

    ###############################################################################################
    # remove_friend
    ############################################################################################### 
    # summary: remove_friend ends a friendship
    # input:  None
    # output: removes a friendship
    ###############################################################################################
    def remove_friend(self, friendship = None):
     self.friends.discard( friendship )

    ###############################################################################################
    # get_friends
    ############################################################################################### 
    # summary: get_friends
    # input:  None
    # output: returns friends
    ###############################################################################################
    def get_friends(self):
     return self.friends

    ###############################################################################################
    # make_purchase
    ############################################################################################### 
    # summary: updates purchases made by person
    # input: 
    #   amount: the amount of the purchase
    #   timestamp: the associated timestamp
    # output: None
    ###############################################################################################
    def make_purchase(self, amount = 0, timestamp = None ):
     self.purchases.append( { 'timestamp': timestamp, 'amount': amount }  );
    
    ###############################################################################################
    # calculate_mean_sd
    ############################################################################################### 
    # summary: calculates mean and standard deviation given a list of amounts
    # input: a list of amounts
    # output: a dictionary containing the mean and standard deviation
    ###############################################################################################
    def calculate_mean_sd( self, amounts = None ):
     mean = sum( amounts )/len( amounts )
     sd = 0;
     for i in range(0,len(amounts)):
       sd = sd + math.pow(amounts[i] - mean,2);
     sd = sd/(len(amounts))
     sd = math.sqrt( sd ); 
     return( {'mean': mean,'sd': sd})

    ###############################################################################################
    # is_anomalous
    ############################################################################################### 
    # summary: identifies anomalous purchase  and writes it to output files
    # input: 
    #       amounts: a list of purchases made in the persons social network
    #       purchase: the purchase being evaluated
    #       event: the event associated with the purchase
    #       outfile: the outfile to write to if the purchase is anomalous
    # output: NONE, outputs anomalous data to file if needed
    ###############################################################################################
    def is_anomalous( self, person = None, amounts = None, purchase = None, event = None, outfile = None ):
      mean_sd = self.calculate_mean_sd(amounts)
      mean = mean_sd['mean']
      sd = mean_sd['sd']
      sd3 = 3*sd
      upper_bound = mean+sd3
      #once the person is set anomalous, no going back.
      if not self.anomalous: 
       self.anomalous =  purchase > upper_bound
       if(self.anomalous):
        event['mean'] = "%.2f"%(mean)
        event['sd'] = "%.2f"%(sd)
        output_string = '{"event_type":"%s", "timestamp":"%s", "id": "%s", "amount": "%s", "mean": "%s", "sd": "%s"}\n'%( event['event_type'], event['timestamp'], event['id'], event['amount'], event['mean'], event['sd'] )
        outfile.write(output_string)
        outfile.flush()

    def add_local_network(self, local_network):
     self.local_network = local_network; 

    def remove_local_network(self):
     self.local_network = None; 

     #self.local_network_size = len( self.local_network[ 'people' ] ) 
     #self.calculate_mean_sd()
     # No need to calculate mean and sd yet as anomalous detection starts from the second input file. 

###############################################################################################
# class social_network
############################################################################################### 
# summary: holds the entire social network as a graph data structure (adjacency list)
###############################################################################################
class social_network:
 
    ###############################################################################################
    # __init__
    ############################################################################################### 
    # summary: initialize a social network
    # input:  
    #        D: degrees of removal to consider
    #        T: Number of purchases in local network to consider
    ###############################################################################################
    def __init__(self, persons={}, D = None, T = None):
     self.persons = persons
     self.D = D
     self.T = T
    
    ###############################################################################################
    # person_exists
    ############################################################################################### 
    # summary: check if a person exists
    # input:  
    #        id: the id of a person
    # output:
    #        returns True if the person exists in the network
    ###############################################################################################
    def person_exists(self, id = None):
     return int(id) in self.persons;


    ###############################################################################################
    # add_person
    ############################################################################################### 
    # summary: add a person if not present
    # input:  
    #        name: the numeric id of the person
    #        friends: friends of person
    #        purchases: purchases made by person
    #        total: the running total of purchases
    # output:
    #        None
    ###############################################################################################
    def add_person( self, name = None, friends = None, purchases = None, total = None ):
         self.persons[int(name)] = person( name = int(name), friends = friends , purchases = purchases, total = total, network = None ); # may need to interpret time stamp later

    ###############################################################################################
    # befriend
    ############################################################################################### 
    # summary: make two persons friends
    # input:  
    #        name1: first person
    #        name2: second person
    # output:
    #        None
    ###############################################################################################
    def befriend( self, name1 = None, name2 = None ):
     name1 = int(name1)
     name2 = int(name2)
     #now make them friends
     self.persons[ name1 ].add_friend( friendship = name2 ) 
     self.persons[ name2 ].add_friend( friendship = name1 ) 
     
    ###############################################################################################
    # befriend
    ############################################################################################### 
    # summary: end a friendship between two persons 
    # input:  
    #        name1: first person
    #        name2: second person
    # output:
    #        None
    ###############################################################################################
    def unfriend( self, name1 = None, name2 = None ):
     name1 = int( name1 )
     name2 = int( name2 )
     #now unfriend
     self.persons[ name1 ].remove_friend( friendship = name2 ) 
     self.persons[ name2 ].remove_friend( friendship = name1 ) 

    ###############################################################################################
    # process_purchase
    ############################################################################################### 
    # summary: process a purchase
    # input:  
    #        name: person who made the purchase
    #        amount: monatary amount
    #        timestamp: the timestamp
    # output:
    #        None
    ###############################################################################################
    def process_purchase( self, name = None, amount = None, timestamp = None ):
     self.persons[ name ].make_purchase( amount = amount, timestamp = timestamp )
     #print('++++++++++++++++++++++++++++++++++++++++++++++++')
     #print('add anomalous purchase handling in this function')
     #print('++++++++++++++++++++++++++++++++++++++++++++++++')



    ###############################################################################################
    # process_purchase
    ############################################################################################### 
    # summary: makes a timestamp tuple which keeps track of te timestamp, whether it was in the
    #          stream and which row it occured in.
    # input:  
    #        timestamp: the timestamp
    #        stream: boolean denoting stream file (assumed to come later)
    #        row: the row in the file of origin (to break ties)
    # output:
    #        the timestamp tuple
    ###############################################################################################
    def process_timestamp( self, timestamp = None, stream = None, row = None ):
     #timestamp = time.mktime( datetime.datetime.strptime( timestamp, "%Y-%m-%d %H:%M:%S" ).timetuple() )
     timestamp = (stream, timestamp,row )
     return timestamp


    ###############################################################################################
    # parse_network
    ############################################################################################### 
    # summary: creates a social network out of either input file
    #          
    # input:  
    #        timestamp: input data as supplied by either log file
    #        stream: which output file was used ( True if stream)
    #        outfile: Keep track of the outfile in case we have to write.
    # output:
    #        the social network object. 
    ###############################################################################################
    def parse_network( self, input_data = None, stream = False, outfile = None ):
     row = 0
     for event in input_data:
      # anomalous detection is handled at the end of each event. 
      # a few flags and variables for dealing with anomalous data.
      persons_altered = [] # used while processing stream
      was_purchase = False;# flag used to trigger anomalous detection
      purchase_amount = None; # needed for anomalous detection 
      person_who_purchased = None; #needed for anomalous detection
      
      
      #event processing starts:
      event_type = event[ 'event_type' ];
      timestamp = event[ 'timestamp' ];
      timestamp = self.process_timestamp(timestamp, stream, row);

      # process purchase events
      if event_type == 'purchase':
        cur_id = int( event[ 'id' ] )
        amount = float( event['amount'] )
        was_purchase = True;
        purchase_amount = amount;
        person_who_purchased = cur_id;
        if self.person_exists( event[ 'id' ] ):
         self.process_purchase( name = cur_id, amount = amount, timestamp = timestamp)
        else:
         self.add_person( name = cur_id, friends = set(), purchases = [{ 'timestamp': timestamp, 'amount':  amount }], total = amount )
        persons_altered.append( cur_id ) 

      # process befriend events
      elif event_type == 'befriend':
       id1 = int(event[ 'id1' ]);
       id2 = int(event[ 'id2' ]);
       persons_altered.append(id1)
       persons_altered.append(id2)

       # check that both people exist, if not make empty person
       if( self.person_exists( id1 ) ):
        pass;
       else:
        self.add_person( name = id1,friends = set(), purchases = [] );
       if( self.person_exists( id2 )):
        pass;
       else:
        self.add_person( name = id2, friends = set(), purchases = [] );
        
       #now make sure they are friends with eachother. 
       self.befriend( name1 = id1, name2 = id2 );

       
      #process unfriend events
      elif event_type == 'unfriend':
       id1 = int(event[ 'id1' ]);
       id2 = int(event[ 'id2' ]);
       persons_altered.append(id1)
       persons_altered.append(id2)

       # check that both people exist, if not make empty person
       if( self.person_exists(id1)):
        pass;
       else:
        self.add_person( name = id1, friends = set(), purchases = [] );
       if( self.person_exists( id2 )):
        pass;
       else:
        self.add_person( name = id2, purchases = [] );
        
       #now make sure they are not friends with eachother. 
       self.unfriend( name1 = id1, name2 = id2 );

      else:
       #ignore improperly formatted input
       print('STRANGE INPUT DETECTED')
       continue
     
      if(stream and was_purchase):
       print( "%s, %i of %i" %( event, row, len(input_data) ) )
       for altered_person in persons_altered:
         cur_person = self.persons[altered_person];
         #initializes local network and determines if purchase was anomalous
         self.init_local_network( person = cur_person, purchase = was_purchase, amount = purchase_amount, stream = stream, event = event, outfile = outfile );
         #save memory, no need to keep this info
         self.persons[altered_person].remove_local_network()
      row = row + 1;
    #initialize parsed graph given D and T
    #initial social network does not deal with anomalous data. 
    #this will speed up processing of live feed data
    def initialize_local_networks( self ):
      for key in self.persons.keys():
       cur_person = self.persons[ key ];
       self.init_local_network( person = cur_person, purchase = False, amount = None, stream = False, outfile = False);


    
    ###############################################################################################
    # parse_network
    ############################################################################################### 
    # summary:  initializes the social network for a specific person. 
    #          
    # input:  
    #        people: unique set of people in network
    #        purchase: is this a purchase?
    #        amount: the amount of the purchase
    #        stream: which file was this made in (must be stream for anomaly)
    #        event: the event which triggered the function to be called (will need to write output)
    #        outfile: the outfile to write to if an anomaly is detected. 
    # output:
    #        None
    ###############################################################################################
    def init_local_network( self, person = None, purchase = False, amount = None, stream = False, event = None,  outfile = None ):
     # people should be unique but timestamp[i] should correspond to  purchase [i];
     friend_network = { 'people': set(), 'timestamps': [], 'amounts': [] } 
     self.get_friend_network( D = self.D, person = person, network = friend_network, top_of_recursion = True )
     person.add_local_network( friend_network )
     if stream & purchase:
      self.purchase_is_anomalous( person = person, friend_network = friend_network, current_purchase = amount, event = event, outfile = outfile );

    

     
    ###############################################################################################
    # purchase_is_anomalous
    ############################################################################################### 
    # summary: check if the purchase is anomalous. 
    #          Note: comparing timestamps was time expensive. Faewer operations would be needed
    #                if delayed till an anomaly needed to be detected. 
    #                no need to convert every single timestamp... just the ones where purchases are made.
    # input:  
    #        people: unique set of people in network
    #        purchase: is this a purchase?
    #        amount: the amount of the purchase
    #        stream: which file was this made in (must be stream for anomaly)
    #        event: the event which triggered the function to be called (will need to write output)
    #        outfile: the outfile to write to if an anomaly is detected. 
    # output:
    #        None
    ###############################################################################################
    def purchase_is_anomalous( self, person = None, friend_network = None, current_purchase = None, event = None,  outfile = None):
     # first we must pick events 
     timestamps_info = friend_network['timestamps']
     if len( timestamps_info ) >1:
      for i in range(0, len(timestamps_info)):
       element = timestamps_info[i] 
       list_element = list(element); 
       new_stamp = time.mktime( datetime.datetime.strptime( list_element[1], "%Y-%m-%d %H:%M:%S" ).timetuple() )
       list_element[1] = new_stamp
       element = tuple( list_element )
       timestamps_info[i] = element;
     
      sorted_values = sorted( timestamps_info, reverse = True )
      # now we take the top T values
      # because we include row numbers in the time stamps, these actually actually act as unique keys
      # in either the batch of stream file. The touple has first row TRUE if its part of the stream. 
      # preference is given to rows in the streaming file as the implication is that they will 
      # have timestamps that are later than the batch file.  
      relevant_rows = sorted_values[0:self.T]
      relevant_amounts = []
      for i in range( 0, len( timestamps_info ) ):
       for j in range( 0, len( relevant_rows ) ):
        if( relevant_rows[j] == timestamps_info[i] ):
         relevant_amounts.append(friend_network['amounts'][i])
      
      person.is_anomalous( person = person, amounts = relevant_amounts, purchase = current_purchase, event = event,  outfile = outfile );
      
          
    ###############################################################################################
    # get_friend_network
    ############################################################################################### 
    # summary: 
    #          recursive function fora local network traversal and creation 
    # input:  
    #        D: levels of frienship to explore (1) only my friends (2) friends of friends ...
    #        person: the person we will start with 
    #        network: the whole social network
    #        top of recursion: is this the starting person?
    # output:
    #        the social network as a dictionary
    ###############################################################################################
    def get_friend_network( self,  D = None , person = None, network = None, top_of_recursion = None ):
     # no need to further recurse
     # redundantly added in case of edge-case
     if(D < 0):
      pass
     else:
      #ignore my purchases for my own friend network.
      # note that we add this person to the network 
      # after purchases are added. 
      # this means the children of the parent node
      # will already have the parent when we reach them
      # Therefore the parent purchase will not be added back. 
      if( not top_of_recursion ):
       if( person.name not in network[ 'people' ] ): 
        for purchase in person.purchases:
          network[ 'amounts' ].append( purchase['amount']  )
          network[ 'timestamps' ].append( purchase['timestamp']  )
      #add myself to my friend network at first (also exclusion list) 
      # this is a set, so no repititions are allowed
      network[ 'people' ].add( person.name ) 
 
      # decrement D and setup next recursion don't bother if next recursion is D = -1
      if D > 0:
       for friend in person.friends:
        cur_friend = self.persons[ friend ]
        self.get_friend_network(D = D-1, person = cur_friend, network = network)

      # if we are back at the person of origin, D will be the same as self.D  
      # we remove this person from the list of friends. (you cannot be in your own network) 
      if D == self.D:
       network['people'].discard(0)



