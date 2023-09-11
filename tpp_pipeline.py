#!/usr/bin/env python3


"""
Assumptions: We are converting all input files to filterbanks after doing default RFI mitigation, 
searching all spectra, only stokes I is searched, runs on default gpu Id, i.e 0, 
adaptive scrunching on heimdall is enabled, candmaker runs on gpu 0, 
FETCH uses model a and a probability of 0.1. Subbanded search is not yet implemented.

Print statements are to help with logging. Time commands too.

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

# Initiate Logging. Logging types  are:
#    info (for blah blah blah)
#    warn (for warnings)
#    debug (for timestamp checks or other temp debugging)
#    error (self explanatory)
import logging
logging.basicConfig(format='%(asctime)s  %(levelname)s: %(message)s',datefmt='%m-%d-%Y_%H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

parser = argparse.ArgumentParser(
    prog="tpp_pipeline.py",
    description="Convert PSRFITS to FILTERBANK, makes a dedispersion plan, decimate the files if needed, runs HEIMDALL, makes h5s files of candidates, classifies using FETCH. For TPP pipeline usage only, turn on database manager with the secret code. ",
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
    "-t",
    "--tpp_db",
    help="Turn on updating to database manager. THIS IS FOR TPP OFFICIAL USE ONLY. To avoid mistaken turn-on, you must include the following argument to turn it on for real: mastersword",
    required=False,
)
parser.add_argument(
    "-v",
    "--verbose",
    help="Turn on DEBUG-level (general) logging.",
    required=False,
    action='store_true'
)



values = parser.parse_args() 


# Check logging level
if (values.verbose):
    logger.setLevel(logging.DEBUG)
    # For some reason the below line needs to be included for logging to
    # function hereafter... haven't figured out why. - SBS
logging.info("(This current line is of no consequence)")


# Read and check data files
your_files = Your(values.files)
logger.info("Reading raw data from "+str(values.files))

center_freq=your_files.your_header.center_freq
logger.info("The center frequency is "+str(center_freq)+" MHz")

bw=your_files.your_header.bw
logger.info("The bandwidth is "+str(bw)+" MHz")

tsamp=your_files.your_header.native_tsamp
logger.info("The native sampling time is "+str(tsamp)+" s")

obs_len = your_files.your_header.native_nspectra*tsamp
if obs_len >= 60:
    obs_len_min = obs_len/60
    logger.info("Dataset length is "+str(obs_len_min)+" minutes")
else:
    logger.info("Dataset length is "+str(obs_len)+" seconds")



# Check Database Manager connection request
print ("my value is "+str(values.tpp_db))
db_on = False
if values.tpp_db is not None:
    db_password = values.tpp_db
    if (db_password != "mastersword"):
        logger.error("******************************************************************")
        logger.error("***** It looks like you tried to turn on the TPP Manager but *****")
        logger.error("*****        provided the wrong password. Exiting now.       *****")
        logger.error("******************************************************************")
        exit()
    elif (db_password == "mastersword"):
        logger.warning("******************************************************************")
        logger.warning("*****Pipeline results will be pushed to TPP Database Manager.*****")
        logger.warning("*****      If this is unintentional, abort your run now.     *****")
        logger.warning("******************************************************************")
        db_on = True
else:
    logger.info("No connections will be made to TPP Database Manager.")
    db_on = False


#TPPDB
 




#Running your_writer with standard RFI mitigation. Clean file to run heimdall and candmaker on. Doesn't have to do RFI mitigation on each step. Also, filterbanks required for decimate.

logger.info('WRITER:Preparing to run your_writer to convert the PSRFITS to FILTERBANK and to do RFI mitigation on the fly\n')


writer_start=timer()
writer_cmd="your_writer.py -v -f"+your_files.your_header.filename+" -t fil -r -sksig 4 -sgsig 4 -sgfw 15 -name "+your_files.your_header.basename+"_converted"
subprocess.call(writer_cmd,shell=True)
writer_end=timer()
logger.debug('WRITER: your_writer.py took '+str(writer_end-writer_start)+' s')

'''

# Running DDplan.py
if center_freq<1000: 
    logger.warning("Low frequency (< 1 GHz) data. Preparing to run DDplan.py....\n")

    ddplan_cmd="DDplan.py -o "+your_files.your_header.basename+"_ddplan -l 0 -d 3600 -f "+str(center_freq)+ " -b "+str(np.abs(bw))+ " -n "+str(your_files.your_header.native_nchans)+ " -t"+str(tsamp)+" -w >"+ your_files.your_header.basename+"_ddplan.txt" 
    subprocess.call(ddplan_cmd,shell=True)
    logger.info('DDplan completed. A text file is created\n')
    # Read the input from the text file and decimate. To be fixed....
    deci_cmd="decimate *fil -t 2 -c 1 >"+str(your_files.your_header.basename)+"_decimated.fil" 
    subprocess.call(deci_cmd,shell=True)

'''

# Running heimdall
heimdall_start=timer()
your_fil_object=Your(your_files.your_header.basename+"_converted.fil")
logger.info("HEIMDALL:Using the RFI mitigated filterbank file " + str(your_fil_object.your_header.filename)+" for Heimdall")
logger.info("HEIMDALL:Preparing to run Heimdall..\n")
def dm_max(obslen,f_low,f_high):
    dm_h=(obslen*10**3/4.15)*(1/((1/f_low**2)-(1/f_high**2)))
    return dm_h
f_low=(center_freq+bw/2)*10**(-3) #in GHz
f_high=(center_freq-bw/2)*10**(-3) #in GHz
max_heimdall_dm=int(min(dm_max(obs_len,f_low,f_high),10000))
heimdall_cmd = "your_heimdall.py -f "+ your_fil_object.your_header.filename+" -dm 0 " + str(max_heimdall_dm) 
subprocess.call(heimdall_cmd,shell=True)
heimdall_end=timer()
logger.debug('HEIMDALL: your_heimdall.py took '+str(heimdall_end-heimdall_start)+' s')


# go to the new directory with the heimdall cands
cand_dir=os.chdir(os.getcwd()
        + "/"
        + your_fil_object.your_header.basename)
logger.debug("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")


logger.info('CANDCSVMAKER:Creating a csv file to get all the info from all the cand files...\n')
fil_file=your_fil_object.your_header.filename
os.system('python ../candcsvmaker.py -v -f ../'+str(fil_file)+' -c *cand')
candidates=pd.read_csv(str(your_fil_object.your_header.basename)+".csv")
num_cands=str(candidates.shape[0])
logger.info('CHECK:Number of candidates created = '+num_cands)

#Create a directory for the h5s
try:
    os.makedirs("h5")
except FileExistsError:
    pass

"""

NOTE IT IS HERE THAT WE NEED TO DO COORDINATE CORRECTION FOR DRIFTSCAN DATA

"""



candmaker_start=timer()
logger.info('CANDMAKER:Preparing to run your_candmaker.py that makes h5 files.....\n')
if your_fil_object.your_header.nchans <= 256:
	gg=-1
else:
	gg=0 
candmaker_cmd ="your_candmaker.py -v -c *csv -g "+str(gg)+" -n 4 -o ./h5/"
subprocess.call(candmaker_cmd,shell=True)
candmaker_end=timer()
logger.debug('CANDMAKER: your_candmaker.py took '+ str(candmaker_end-candmaker_start)+' s')

os.chdir(os.getcwd()+'/h5')
logger.debug("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")

dir_path='./'
count = 0
for path in os.listdir(dir_path):
    if os.path.isfile(os.path.join(dir_path, path)):
        count += 1
num_h5s = count-1   #subracting the log file

if int(num_h5s)==int(num_cands):
    logger.debug('CHECK:All candidiate h5s created')
else:
    logger.debug('CHECK:Not all cand h5s are created')

fetch_start=timer()
logger.info("FETCH:Preparing to run FETCH....\n")
fetch_cmd='predict.py -v -c . -m a -p 0.2'
subprocess.call(fetch_cmd,shell=True)
fetch_end=timer()
logger.debug('FETCH: predict.py took '+str(fetch_end-fetch_start)+' s')
if os.path.isfile('results_a.csv'):
	logger.info('FETCH: FETCH ran successfully')
	plotter_cmd="your_h5plotter.py -c results_a.csv"
	subprocess.call(plotter_cmd,shell=True)

else:
	logger.warning('FETCH:FETCH did not create a csv file')
exit()
