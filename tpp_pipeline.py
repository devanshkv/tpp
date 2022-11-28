#!/usr/bin/env python3

"""
*If we have a wrapper, we can avoid putting database links in our core TPP pipeline.*
WRAPPER:
[REPORT JOB STARTED TO DATABASE]
[CALL PIPELINE]-->job should return relevant pipeline info and results as a dictionary.
[SCAN PIPELINE LOGGING FOR FAILURES OR HANG-UPS?]
[IF SUCCESSFUL COMPLETION, REPORT INFO TO DATABASE]
[IF UNSUCCESSFUL COMPLETION, REPORT FAILURE TO DATABASE]


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




#PIPELINE ITSELF:
#0) Set up logging.


#1) Read in file name and directory (from command line).



#2) Ingest header information. We’ll need: 
#Center freq
#BW
#Tsamp
#Length of dataset
#3) Set up search parameters:
#What max DM is feasible?
#This maximum DM seems approximately reasonable judging by the fact that the smearing in about 300kHz bandwidth at 1.0GHz center frequency is around 25ms (which is just within the 32ms maximum pulse width we are going to be searching). However, this ignores all other telescope configurations and is only really a standard setup for GBT.
#What boxcar sampling times are needed (up to 32ms)?


#4) Run actual pipeline parts

