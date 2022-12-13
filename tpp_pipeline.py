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

logging.info('So should this')



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

