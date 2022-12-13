#!/usr/bin/bash/lorule/scratch/rat0022/tpp/myfork/your/bin


import argparse
import glob
import your
import subprocess
from your import Your, Writer
import os
from your.utils.misc import YourArgparseFormatter


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
cmd="your_writer.py -v -f /lorule/scratch/rat0022/tpp/vegas_59087_79895_Fermi_0004_0001.fits -t fil -r -sksig 4 -sgsig 4 -sgfw 15"
subprocess.Popen(cmd,shell=True)

# Running decimate
deci_cmd="decimate *fil -t 2 -z 1 > " 
subprocess.Popen(deci_cmd,shell=True)

# Running heimdall

heimdall_cmd = "your_heimdall.py -f *fil -dm 0 100"
subprocess.Popen(heimdall_cmd,shell=True)


#



