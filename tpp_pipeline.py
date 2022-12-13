#!/usr/bin/env python3
import numpy as np

import logging
#logger = logging.getLogger(__name__) # Need to dig into this; what does it do? Is it necessary?
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')
logging.error('And non-ASCII stuff, too, like Øresund and Malmö')
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
import subprocess
from your import Your, Writer
import os
from your.utils.misc import YourArgparseFormatter

"""
PIPELINE ITSELF:
0) Set up logging.
1) Read in file name and directory (from command line).
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

files ='/lorule/scratch/rat0022/tpp/*.fits'
    


#2) Ingest header information. We’ll need: 
#Center freq
#BW
#Tsamp
#Length of dataset
y= Your(glob.glob(files))
print(files)
center_freq=y.your_header.center_freq
print("The center frequency is "+str(center_freq))
bw=y.your_header.bw
print("The bandwidth is "+str(bw))
tsamp=y.your_header.native_tsamp
print("The native sampling time is "+str(tsamp))
obs_len = y.your_header.native_nspectra*tsamp
if obs_len >= 60:
    obs_len_min = obs_len/60
    print("Dataset length is "+str(obs_len_min)+" minutes")
else:
    print("Dataset length is "+str(obs_len)+" seconds")

#3) Set up search parameters:
#What max DM is feasible?
#This maximum DM seems approximately reasonable judging by the fact that the smearing in about 300kHz bandwidth at 1.0GHz center frequency is around 25ms (which is just within the 32ms maximum pulse width we are going to be searching). However, this ignores all other telescope configurations and is only really a standard setup for GBT.
#What boxcar sampling times are needed (up to 32ms)?


#4) Run actual pipeline parts
# Running writert
#files=print(values.fin[0])
#output = check_output(["your_writer.py","-f",/lorule/scratch/rat0022/tpp/vegas_59087_79895_Fermi_0004_0001.fits,"--type",fil])
#files=print(values.fin[0])
#subprocess.run(args,shell=True)
filename='/lorule/scratch/rat0022/tpp/vegas_59087_79895_Fermi_0004_0001.fits'
cmd="your_writer.py -v -f"+str(filename)+" -t fil -r -sksig 4 -sgsig 4 -sgfw 15"
subprocess.Popen(cmd,shell=True)

# Running decimate
#deci_cmd="decimate *fil -t 2 -z 1 > " 
#subprocess.Popen(deci_cmd,shell=True)

# Running heimdall

#heimdall_cmd = "your_heimdall.py -f *fil -dm 0 100"
#subprocess.Popen(heimdall_cmd,shell=True)


#

