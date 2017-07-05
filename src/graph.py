import pdb;
import math;
import time;
import datetime;

### defines person object ###########################################################################
class person:
    #def __init__(self, name = '', friends = set(), purchases = [], total = 0, network = [] ):
    def __init__(self, name = None, friends = None, purchases = None, total = None, network = None ):
      self.name = name;
      self.friends = friends;     # unique set of tupples, with lower id set first. 
      self.purchases = purchases;   
      #self.total = total;           
      self.local_network = None         # social network based on D
      self.anomalous = False

    def add_friend(self, friendship = None):
     self.friends.add( friendship )

    def remove_friend(self, friendship = None):
     self.friends.discard( friendship )

    def has_friend(self, edge = None):
       pdb.set_trace()

    def get_friends(self):
     return self.friends

    def make_purchase(self, amount = 0, timestamp = None ):
     self.purchases.append( { 'timestamp': timestamp, 'amount': amount }  );
    
    def calculate_mean_sd( self, amounts = None ):
     mean = sum( amounts )/len( amounts )
     sd = 0;
     for i in range(1,len(amounts)):
       sd += math.pow(amounts[i] - mean,2);
     sd = sd/len(amounts)
     sd = math.sqrt( sd ); 
     return( {'mean': mean,'sd': sd})


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
        print('ANOMALY!!!')
        event['mean'] = "%.2f"%(mean)
        event['sd'] = "%.2f"%(sd)
        output_string = '"event_type":"%s", "timestamp":"%s", "id": "%s", "amount": "%s", "mean": "%s", "sd": "%s"\n'%( event['event_type'], event['timestamp'], event['id'], event['amount'], event['mean'], event['sd'] )
        outfile.write(output_string)
        outfile.flush()

    def add_local_network(self, local_network):
     self.local_network = local_network; 

    def remove_local_network(self):
     self.local_network = None; 

     #self.local_network_size = len( self.local_network[ 'people' ] ) 
     #self.calculate_mean_sd()
     # No need to calculate mean and sd yet as anomalous detection starts from the second input file. 

class social_network:
    def __init__(self, persons={}, D = None, T = None):
     self.persons = persons
     self.D = D
     self.T = T
    
    # object functions
    def person_exists(self, id = None):
     return int(id) in self.persons;

    def add_person( self, name = None, friends = None, purchases = None, total = None ):
         self.persons[int(name)] = person( name = int(name), friends = friends , purchases = purchases, total = total, network = None ); # may need to interpret time stamp later

    def befriend( self, name1 = None, name2 = None ):
     name1 = int(name1)
     name2 = int(name2)
     #now make them friends
     self.persons[ name1 ].add_friend( friendship = name2 ) 
     self.persons[ name2 ].add_friend( friendship = name1 ) 
     

    def unfriend( self, name1 = None, name2 = None ):
     name1 = int( name1 )
     name2 = int( name2 )
     #now unfriend
     self.persons[ name1 ].remove_friend( friendship = name2 ) 
     self.persons[ name2 ].remove_friend( friendship = name1 ) 

    def process_purchase( self, name = None, amount = None, timestamp = None ):
     self.persons[ name ].make_purchase( amount = amount, timestamp = timestamp )
     #print('++++++++++++++++++++++++++++++++++++++++++++++++')
     #print('add anomalous purchase handling in this function')
     #print('++++++++++++++++++++++++++++++++++++++++++++++++')

    def process_timestamp( self, timestamp = None, stream = None, row = None ):
     #timestamp = time.mktime( datetime.datetime.strptime( timestamp, "%Y-%m-%d %H:%M:%S" ).timetuple() )
     timestamp = (stream, timestamp,row )
     return timestamp


    # parse the batch input if stream is false
    # parse stream if stream is True
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


    # initializes the social network for an individual. 
    def init_local_network( self, person = None, purchase = False, amount = None, stream = False, event = None,  outfile = None ):
     # people should be unique but timestamp[i] should correspond to  purchase [i];
     friend_network = { 'people': set(), 'timestamps': [], 'amounts': [] } 
     self.get_friend_network( D = self.D, person = person, network = friend_network, top_of_recursion = True )
     person.add_local_network( friend_network )
     if stream & purchase:
      self.purchase_is_anomalous( person = person, friend_network = friend_network, current_purchase = amount, event = event, outfile = outfile );

    

     
    # comparing timestamps was time consuming. Put time consuming step inside, where fewer operations would be needed.
    # no need to convert every single timestamp... just the ones where purchases are made.
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
      
          
    #recursive function for local network traversal 
    def get_friend_network(self,  D = None , person = None, network = None, top_of_recursion = None ):
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



