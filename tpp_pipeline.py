#!/usr/bin/env python3
import numpy as np

import logging
#logger = logging.getLogger(__name__) # Need to dig into this; what does it do? Is it necessary?
#logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
#logging.debug('This message should go to the log file')
#logging.info('So should this')
#logging.warning('And this, too')
#logging.error('And non-ASCII stuff, too, like Øresund and Malmö')
"""
On the use of logging:
 - "debug" tag will be used for writing information that will be put into the database manager.
 - "info" will report status.
 - error and warnings will be used as intended.

This way, a database-ingestion script can easily scan and interpret logs.
"""


import argparse
import glob
import your
from your import Your
import subprocess
import os
from your.utils.misc import YourArgparseFormatter
import time
import numpy
"""
PIPELINE ITSELF:
0) Set up logging.
1) Read in file name from the command line
2) Ingest header information. We’ll need: 
Center freq
BW
Tsamp
Length of dataset
3) Set up search parameters:
What max DM is feasible?
This maximum DM seems approximately reasonable judging by the fact that the smearing in about 300kHz bandwidth at 1.0GHz center frequency is around 25ms (which is just within the 32ms maximum pulse width we are going to be searching). However, this ignores all other telescope configurations and is only really a standard setup for GBT.
What boxcar sampling times are needed (up to 32ms)?
4) Run actual pipeline parts
5) 
6) 
7) 
8) 
"""

logging.info('So should this')



#PIPELINE ITSELF:
#0) Set up logging.


#1) Read in file name and directory (from command line).
parser = argparse.ArgumentParser(
    prog="tpp_pipeline.py",
    description="Convert PSRFITS to FILTERBANK, makes a dedispersion plan, decimate the files if needed, runs HEIMDALL, makes h5s files of candidates, classifies using FETCH",
    formatter_class=YourArgparseFormatter,
)
parser.add_argument(
    "-f",
    "--files",
    help="Paths of input files to be converted to an output format.",
    required=True,
    nargs="+",
)
parser.add_argument(
    "-dl",
    "--low_dm",
    help="Lower limit of the DM to search for",
    required=True,
)
parser.add_argument(
    "-du",
    "--high_dm",
    help="Upper limit of the DM to search for",
    required=True,
)
values = parser.parse_args()
    


#2) Ingest header information. We’ll need: 
#Center freq
#BW
#Tsamp
#Length of dataset
y= Your(values.files)
print("Reading raw data from "+str(values.files)+"\n")
center_freq=y.your_header.center_freq
print("The center frequency is "+str(center_freq)+" MHz\n")
bw=y.your_header.bw
print("The bandwidth is "+str(bw)+" MHz\n")
tsamp=y.your_header.native_tsamp
print("The native sampling time is "+str(tsamp)+" s\n")
obs_len = y.your_header.native_nspectra*tsamp
if obs_len >= 60:
    obs_len_min = obs_len/60
    print("Dataset length is "+str(obs_len_min)+" minutes\n")
else:
    print("Dataset length is "+str(obs_len)+" seconds\n")

#3) Set up search parameters:
#What max DM is feasible?
#This maximum DM seems approximately reasonable judging by the fact that the smearing in about 300kHz bandwidth at 1.0GHz center frequency is around 25ms (which is just within the 32ms maximum pulse width we are going to be searching). However, this ignores all other telescope configurations and is only really a standard setup for GBT.
#What boxcar sampling times are needed (up to 32ms)?


#4) Run actual pipeline parts


#Running your_writer with standarf RFI mitigation/
print('Preparing to run your_writer to convert the PSRFITS to FILTERBANK and to do RFI mitigation on the fly\n')


cmd="your_writer.py -v -f"+y.your_header.filename+" -t fil"
subprocess.call(cmd,shell=True)

# Running DDplan.py
if center_freq<1000: 
    print("Low frequency (< 1 GHz) data. Preparing to run DDplan.py....\n")

    ddplan_cmd="DDplan.py -o "+y.your_header.basename+"_ddplan -l 0 -d 3600 -f "+str(center_freq)+ " -b "+str(np.abs(bw))+ " -n "+str(y.your_header.native_nchans)+ " -t"+str(tsamp)+" -w >"+ y.your_header.basename+"_ddplan.txt" 
    subprocess.call(ddplan_cmd,shell=True)
    print('DDplan completed. A text file is created\n')
    # Read the input from the text file and decimate. To be fixed....
    deci_cmd="decimate *fil -t 2 -c 1 >"+str(y.your_header.basename)+"_decimated.fil" 
    subprocess.call(deci_cmd,shell=True)



# Running heimdall
your_fil_object = Your(glob.glob('*fil'))
print("Using the RFI mitigated filterbank file " + str(your_fil_object.your_header.filename)+" for Heimdall")
print("Preparing to run Heimdall..\n")

heimdall_cmd = "your_heimdall.py -f *fil -dm "+str(values.low_dm)+" "+ str(values.high_dm)
subprocess.call(heimdall_cmd,shell=True)

# go to the new directory with the heimdall cands
cand_dir=os.chdir(os.getcwd()
        + "/"
        + your_fil_object.your_header.basename)
print("Now you are at "+str(os.getcwd())"\n\n")

#path_to_candcsv='/scratch/rat0022/tpp/'

print('Creating a csv file to get all the info from all the cand files...\n')
fil_file=your_fil_object.your_header.filename
#print(str(fil_file))
os.system('python /scratch/rat0022/tpp/candcsvmaker.py -v -f ../'+str(fil_file)+' -c *cand')

#Create a directory for the h5s
try:
    os.makedirs("h5")
except FileExistsError:
    pass

print('Preparing to run your_candmaker.py that makes h5 files.....\n')
candmaker_cmd ="your_candmaker.py -c *csv -g 0 -n 4 -o ./h5/"
subprocess.call(candmaker_cmd,shell=True)

#make a directory for h5 files
if os.path.isfile('*h5'):
    print('making h5s\n')
else:
    print("h5s are not created!\n")

os.chdir(os.getcwd()+'/h5')
print("Now you are at "+str(os.getcwd())"\n")

print("Preparing to run FETCH....\n")
fetch_cmd='predict.py -c . -m a -p 0.1'
subprocess.call(fetch_cmd,shell=True)

exit()
