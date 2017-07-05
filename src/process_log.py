import json
from pprint import pprint
import sys
import pdb
import graph

### Read in input json files #######################################################################
data_file = open(sys.argv[1]);
batch_log = json.load(data_file, parse_float = True);
#
input_params = batch_log[0]
batch_input = batch_log[1:]

data_file = open(sys.argv[2]);
stream_data = json.load( data_file, parse_float = True );
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
