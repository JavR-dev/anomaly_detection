# Table of Contents
1. [Solution Summary](README.md#challenge-summary)
2. [Details of Implementation](README.md#details-of-implementation)
3. [Anomalous Purchases](README.md#anomalous-purchases)


# Solution Summary

A social network is a simple undirected graph and I have implemented my solution in process_log.py and graph.py . I have implemented the solution in Python. There are  no dependancies other than python 2 or python 3.  Base python packages relied on are json, sys, pdb, math, time, and datetime. Simply run the code as you intended from within the test directory, and you should execute without a problem.   



# Details of Implementation


### Social network construction 

A social network is a simple undirected graphs where each node represents a person and each edge represents a friendship. Hence we implemented two classes in graph.py, which is an implementation of an adjacency list, which is a node-centric view of the graph. The first class (“person”)  held information about a  a person and their friends, their purchase history,  as well as some accessory functions for working with a person.  The second class (social_network) holds information about the social network as a whole. This information includes the people in the network as well as parameters D and T. 

#### why use an adjacency list? 

An adjacency list was used because social networks are sparce, which means an adjacency matrix would have been far too memory intensive.  Search time between the two is comparable, but in general the adjacency list is best in this case. In the end we will recieve node centric data, and a simple recursive algorithm can be used to search the graph up to D friends away. 


### Batch and stream data processing
Events were handled one at a time. We assumed all stream events would happen after the batch events, and kept track of this using a boolean variable (stream). Regardless of the type of event, the graph was checked to see if an update was needed, by added edges, removing edges and adding nodes. Note our implementation allows for people to join the graph at will. Purchases were tracked in a variable for each person, and edges were simply a set of the ids of the adjacent persons.

During stream processing, if a purchase was made, it triggered a function which then checked if the purchase was anomalous. First, starting from the node that made the purchase and a recursive function, all purchases and timestamps were collected up to D friendships away. Then, the most recent T purchases were used to determine if the purchase was an anomaly. If it was an anomaly the event was immediately reported to the output file.  NOTE: I assumed that each person only needed to be mentioned once. Hence, only the first anomalous purchase is reported.


