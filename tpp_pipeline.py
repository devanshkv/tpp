#!/usr/bin/env python3
import numpy as np

import logging

"""
Assumptions: We are converting all input files to filterbanks after doing default RFI mitigation, searching all spectra, only stokes I is searched, runs on default gpu Id, i.e 0, adaptive scrunching on heimdall is enabled, candmaker runs on gpu 0, FETCH uses model a and a probability of 0.1. Subbanded search is not yet implemented.

Print statements are to help with logging. Time commands too.
  
""" 

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
from timeit import default_timer as timer
import numpy as np
import pandas as pd
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

#logging.info('So should this')



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
    help="Input files to be converted to an output format.",
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


#Running your_writer with standard RFI mitigation. Clean file to run heimdall and candmaker on. Doesn't have to do RFI mitigation on each step. Also, filterbanks required for decimate.
print('WRITER:Preparing to run your_writer to convert the PSRFITS to FILTERBANK and to do RFI mitigation on the fly\n')

writer_start=timer()
writer_cmd="your_writer.py -v -f"+y.your_header.filename+" -t fil -r -sksig 4 -sgsig 4 -sgfw 15 -name "+y.your_header.basename+"_converted"
subprocess.call(writer_cmd,shell=True)
writer_end=timer()
print('WRITER: your_writer.py took '+str(writer_end-writer_start)+' s')

'''

# Running DDplan.py
if center_freq<1000: 
    print("Low frequency (< 1 GHz) data. Preparing to run DDplan.py....\n")

    ddplan_cmd="DDplan.py -o "+y.your_header.basename+"_ddplan -l 0 -d 3600 -f "+str(center_freq)+ " -b "+str(np.abs(bw))+ " -n "+str(y.your_header.native_nchans)+ " -t"+str(tsamp)+" -w >"+ y.your_header.basename+"_ddplan.txt" 
    subprocess.call(ddplan_cmd,shell=True)
    print('DDplan completed. A text file is created\n')
    # Read the input from the text file and decimate. To be fixed....
    deci_cmd="decimate *fil -t 2 -c 1 >"+str(y.your_header.basename)+"_decimated.fil" 
    subprocess.call(deci_cmd,shell=True)

'''

# Running heimdall
#name = str(y.your_header.basename).split('_')
heimdall_start=timer()
your_fil_object=Your(y.your_header.basename+"_converted.fil")
print("HEIMDALL:Using the RFI mitigated filterbank file " + str(your_fil_object.your_header.filename)+" for Heimdall")
print("HEIMDALL:Preparing to run Heimdall..\n")

heimdall_cmd = "your_heimdall.py -f "+ your_fil_object.your_header.filename+" -dm "+str(values.low_dm)+" "+ str(values.high_dm) 
subprocess.call(heimdall_cmd,shell=True)
heimdall_end=timer()
print('HEIMDALL: your_heimdall.py took '+str(heimdall_end-heimdall_start)+' s')


# go to the new directory with the heimdall cands
cand_dir=os.chdir(os.getcwd()
        + "/"
        + your_fil_object.your_header.basename)
print("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")


print('CANDCSVMAKER:Creating a csv file to get all the info from all the cand files...\n')
fil_file=your_fil_object.your_header.filename
#print(str(fil_file))
os.system('python ../candcsvmaker.py -v -f ../'+str(fil_file)+' -c *cand')
candidates=pd.read_csv(str(your_fil_object.your_header.basename)+".csv")
num_cands=str(candidates.shape[0])
print('CHECK:Number of candidates created = '+num_cands)

#Create a directory for the h5s
try:
    os.makedirs("h5")
except FileExistsError:
    pass

candmaker_start=timer()
print('CANDMAKER:Preparing to run your_candmaker.py that makes h5 files.....\n')
if your_fil_object.your_header.nchans <= 256:
	gg=-1
else:
	gg=0 
candmaker_cmd ="your_candmaker.py -c *csv -g "+str(gg)+" -n 4 -o ./h5/"
subprocess.call(candmaker_cmd,shell=True)
candmaker_end=timer()
print('CANDMAKER: your_candmaker.py took '+ str(candmaker_end-candmaker_start)+' s')

os.chdir(os.getcwd()+'/h5')
print("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")

dir_path='./'
count = 0
for path in os.listdir(dir_path):
    if os.path.isfile(os.path.join(dir_path, path)):
        count += 1
#print('File count:', count)
num_h5s = count-1   #subracting the log file

if int(num_h5s)==int(num_cands):
    print('CHECK:All candidiate h5s created')
else:
    print('CHECK:Not all cand h5s are created')

#check if h5s exist.
#if os.path.isfile('*h5'):
#    print('making h5s\n')
#else:
#    print("h5s are not created!\n")

fetch_start=timer()
print("FETCH:Preparing to run FETCH....\n")
fetch_cmd='predict.py -c . -m a -p 0.2'
subprocess.call(fetch_cmd,shell=True)
fetch_end=timer()
print('FETCH: predict.py took '+str(fetch_end-fetch_start)+' s')
if os.path.isfile('results_a.csv'):
	print('FETCH: FETCH ran successfully')
	plotter_cmd="your_h5plotter.py -c results_a.csv"
	subprocess.call(plotter_cmd,shell=True)

else:
	print('FETCH:FETCH did not create a csv file')
exit()
