import json
from pprint import pprint
import sys
import pdb
import graph

#helper function checkes file input is proper json and handles accordingly.
def read_file(data_file):
 read_file = None;
 try:
    read_file = json.load( data_file , parse_float = True );
    if type(read_file) == type(dict()):
      read_file = [read_file]
 except ValueError:
    print('fixing batch logfile was not a proper json file');
    data_file.seek(0)
    lines = data_file.readlines();
    read_file = [];
    for i in range( 0, len(lines)-1 ):
     read_file.append( json.loads(lines[i], parse_float = True ) );
 return( read_file );




### Read in input json files #######################################################################
data_file = open( sys.argv[1], 'r' );
batch_log = read_file( data_file );

# note the original data_file was not a properly formatted json file. 
# the code below tries to open the file and if it fails, tries to fix it. 
input_params = batch_log[ 0 ]
batch_input = batch_log[ 1: ]

data_file = open( sys.argv[2], 'r' );
stream_data = read_file( data_file )
####################################################################################################


### open output file handle ########################################################################
output_file = sys.argv[3];
output_handle = open(output_file, 'w');
####################################################################################################


### build graph ####################################################################################
print('building social_network from batch_input...')
g = graph.social_network( D = int(input_params['D']), T = int(input_params['T']) )
g.parse_network( input_data = batch_input , stream = False, outfile = None );
#g.initialize_local_networks(); # initializing local_networks was memory intensive
####################################################################################################

### parse stream ##################################################################################
print('processing stream...')
g.parse_network( input_data = stream_data, stream = True, outfile = output_handle );
###################################################################################################


### Close output_handle ############################################################################
print( "Anomalous purchases output to: %s"%(output_file))
output_handle.close()
####################################################################################################
