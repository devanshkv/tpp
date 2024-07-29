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

#TPPDB: determine job start
from datetime import datetime
time_start_UTC = datetime.utcnow()


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
import database as db #TPPDB: This line needs to be fixed.
import candcsvmaker
import traceback

"""


TPPDB in case of DB communication issue, Bikash recommends writing the
desired "push" dictionaries to a file (and maybe transferring it to
tingle.) These could be regularly sucked over to tingle by some other
external cron job running on TF/DS and then pushed from tingle (from a
separate cron), or they could be attempted to be directly pushed by a
cron job on TF/DS for each user directly onto TPPDB. However, perhaps
the user's home directory should contain a folder that is the nominal
static end location for that kind of thing.


"""


def print_dberr():
    logger.error("*****DB COMMUNICATIONS ERROR, could not push to database.*****")
    logger.error("**************************************************************")
    # I don't think we want exiting behavior here, but this is
    # where we might be able to add in a "save for next update
    # later when TPPDB communications are back up".
    return
    
def dm_max(obslen,f_low,f_high):
    dm_h=(obslen*10**3/4.15)*(1/((1/f_low**2)-(1/f_high**2)))
    return dm_h

def tpp_state(status):
    """.
    This updates the processing_outcomes status.
    An attempted (but failed) update current provides an EXCEPT but no exit.
    """

    time_now = datetime.now().isoformat()

    try:
        # POST UPDATE.
        data={"job_state_time":time_now,
              "job_state": status}
        db.patch("processing_outcomes",outcomeID,data=data)
    except:
        print_dberr()
        
    return


def do_RFI_filter(filenames,basename):
    '''
    Reshma comment: Running your_writer with standard RFI
    mitigation. Clean file to run heimdall and candmaker on. Doesn't
    have to do RFI mitigation on each step. Also, filterbanks
    required for decimate.
    '''
    
    #!RESHMA TPPDB: Somewhere here (probably in your_writer.py) we will have to
    #!RESHMA TPPDB: get the code to update the RFI fraction and pre/post-zap RMS values.

    mask_start=timer()
    mask_cmd="your_rfimask.py -v -f "+str(filenames)+" -sk_sigma 4 -sg_sigma 4 -sg_frequency 15"
    logger.debug('RFI MASK: command = ' + mask_cmd)
    subprocess.call(mask_cmd,shell=True)
    mask_end=timer()
    logger.debug('RFI MASK: your_rfimask.py took '+str(mask_end-mask_start)+' s')
    mask_basename=str(basename)+'_your_rfi_mask'
    killmask_file= f"{mask_basename}.bad_chans"
    with open(killmask_file,'r') as myfile:
    	file_str = myfile.read()
    my_list = [] ##initializing a list
    for chan in file_str.split(' '): ##using split function to split, the list.this splits the value in index specified and return the value.
         my_list.append(chan)
    for chan in my_list:
    	 if chan == '':
        	 my_list.remove(chan)

    return len(my_list)

def do_heimdall(your_fil_object):
    """.

    Heimdall performs GPU-based dedispersion and a basic clustering
    and event rejection algorithm.

    It produces .cand files populated with meta-data for individual
    events, including peak DM, S/N, number of clusters, etc.

    """
    heimdall_start=timer()
    logger.info("HEIMDALL:Using the RFI mitigated filterbank file " + str(your_fil_object.your_header.filename)+" for Heimdall")
    logger.info("HEIMDALL:Preparing to run Heimdall..\n")
    f_low=(center_freq+bw/2)*10**(-3) #in GHz
    f_high=(center_freq-bw/2)*10**(-3) #in GHz
    max_heimdall_dm=int(min(dm_max(obs_len,f_low,f_high),10000))
    heimdall_cmd = "your_heimdall.py -f "+ your_fil_object.your_header.filename+" -dm 0 " + str(max_heimdall_dm) 
    subprocess.call(heimdall_cmd,shell=True)
    heimdall_end=timer()
    logger.debug('HEIMDALL: your_heimdall.py took '+str(heimdall_end-heimdall_start)+' s')

def do_candcsvmaker(your_fil_object):
    """.

    candcsvmaker.py has a simple function: create one big csv file
    with all of the candidates inside of it.

    It produces a file called (input-file-base-name).csv

    """
    candcsvmaker_start = timer()
    logger.info('CANDCSVMAKER:Creating a csv file to get all the info from all the cand files...\n')
    fil_file=your_fil_object.your_header.filename
    file_list = "../"+str(fil_file)
    # The threshold values below are set to let heimdall, your, and fetch control what gets through.
    n_events,n_members = candcsvmaker.gencandcsv(candsfiles="*.cand",filelist=file_list,snr_th=0,clustersize_th=0,dm_min=0,dm_max=10000,label=1)

    #!!!RESHMA, GRAHAM, BIKASH AND OTHERS: Large change here, I changed
    #! candcsvmaker to directly call the source function from the code
    #! (candcsvmaker.py was just a "main" input parser around the core
    #! function gencandcsv). However, this new modality is untested and
    #! we should make sure it's working on a rerun of this.
    #!
    #! Secondly, I have set the thresholds to zero for candcsvmaker to
    #! avoid unwanted filtering of candidates, which should be
    #! controlled by us explicitly throughout the pipeline. This extra
    #! layer of filtering seems redundant, given that these values
    #! should be controlled by heimdall, I believe? Is that right?
    #os.system('python ../candcsvmaker.py --snr_th 0 --dm_min_th 0 --dm_max_th 10000 --clustersize_th 0 -v -f ../'+str(fil_file)+' -c *cand')

    candcsvmaker_end = timer()
    logger.debug('CANDCSVMAKER: your_candmaker.py took '+ str(candcsvmaker_end-candcsvmaker_start)+' s')
    logger.debug('CANDCSVMAKER: found ' + str(n_events) ' events with ' + str(n_members) + ' members.')

    return n_events,n_members

def do_your_candmaker(your_fil_object):
    candmaker_start=timer()
    logger.info('CANDMAKER:Preparing to run your_candmaker.py that makes h5 files.....\n')
    if your_fil_object.your_header.nchans <= 256:
        gg = -1
    else:
        gg = 0 
    candmaker_cmd ="your_candmaker.py -v -c *csv -g "+str(gg)+" -n 4 -o ./h5/"
    subprocess.call(candmaker_cmd,shell=True)
    candmaker_end=timer()
    logger.debug('CANDMAKER: your_candmaker.py took '+ str(candmaker_end-candmaker_start)+' s')

def do_fetch():
    fetch_start=timer()
    logger.info("FETCH:Preparing to run FETCH....\n")
    fetch_cmd='predict.py -v -c . -m a -p 0.2'
    subprocess.call(fetch_cmd,shell=True)
    fetch_end=timer()
    logger.debug('FETCH: predict.py took '+str(fetch_end-fetch_start)+' s')

def do_your_h5plotter():
    h5_start=timer()
    logger.info("YOUR_H5PLOTTER: Preparing to plot h5 files....\n")
    plotter_cmd="your_h5plotter.py -c results_a.csv"
    subprocess.call(plotter_cmd,shell=True)
    h5_end=timer()
    logger.debug('YOUR_H5PLOTTER: Took '+str(h5_end-h5_start)+' s') 
        
if __name__ == "__main__":
    # Initiate Logging. Logging types  are:
    #    info (for blah blah blah)
    #    warn (for warnings)
    #    debug (for timestamp checks or other temp debugging)
    #    error (self explanatory)
    import logging
    logging.basicConfig(format='%(asctime)s  %(levelname)s: %(message)s',datefmt='%m-%d-%Y_%H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # For some reason the below line needs to be included for logging to
    # function hereafter... haven't figured out why. - SBS
    logging.info("(This current line is of no consequence)")
    

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
        help="Turn on updating to database manager. THIS IS FOR TPP OFFICIAL USE ONLY. To avoid mistaken turn-on, you must include the following arguments to turn it on for real: mastersword outcomeID working_dir",
        required=False,
        nargs=3
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
    filelist = your_files.your_header.filelist #list of filenames
    filestring = ' '.join(filelist) #single string containing all file names

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


    #TPPDB: !!!!! May need to read declination from the TPPDB, not the file itself; Graham says that sometimes declination is listed with seconds>60. !!!!!!
    
    #TPPDB: !!!!! NEED TO ADD AN INTERNAL CHECK HERE TO MAKE SURE THAT THE DATA INFORMATION READ IS SANE!!!


    # Check Database Manager connection request
    logger.info("My database writer value is "+str(values.tpp_db))
    db_on = False
    if values.tpp_db is not None:
        db_password = values.tpp_db[0]
        outcomeID = values.tpp_db[1]
        working_dir = values.tpp_db[2]
        if (db_password != "mastersword"):
            logger.error("******************************************************************")
            logger.error("***** It looks like you tried to turn on the TPP Manager but *****")
            logger.error("*****        provided the wrong password. Exiting now.       *****")
            logger.error("******************************************************************")
            exit()
        elif (len(values.tpp_db) < 3):
            logger.error("******************************************************************")
            logger.error("***** It looks like you tried to turn on the TPP Manager but *****")
            logger.error("*****  didn't provide pw, outcomeID, directory. Exiting now. *****")
            logger.error("******************************************************************")
            exit()
        elif (db_password == "mastersword"):
            logger.warning("******************************************************************")
            logger.warning("*****Pipeline results will be pushed to TPP Database Manager.*****")
            logger.warning("*****      If this is unintentional, abort your run now.     *****")
            logger.warning("******************************************************************")
            db_on = True
            logger.debug("Changing to the directory provided by TPP-DB interface, " + working_dir)
            os.chdir(working_dir)

        # Test basic TPPDB connection and existence of outcomeID.
        try:
            outcome_doc = db.get("processing_outcomes",outcomeID)
            submissionID = outcome_doc['submissionID']
            dataID = outcome_doc['dataID']
        except:
            print_dberr()
            exit()
            
    else:
        logger.info("No connections will be made to TPP Database Manager.")
        db_on = False


    # Determine node_name and current working directory.
    node_name = os.uname()[1]
    cwd = os.getcwd()
    logger.info("Processing in directory "+str(cwd)+" on node "+str(node_name)+", began at UTC "+str(time_start_UTC.isoformat()))
    if (db_on):
        tpp_state("started")
        try:
            data = {"node_name":node_name,
                    "job_start":time_start_UTC.isoformat()}
            db.patch("processing_outcomes",outcomeID,data=data)
        except:
            print_dberr()



    ############## ############## ############## 
    ##############  YOUR_WRITER   ############## 
    ############## ############## ############## 
    # Runs RFI filtering; also converts psrfits-format files to an RFI-filtered filterbank-format file.
    
    logger.info('WRITER:Preparing to run your_writer to convert the PSRFITS to FILTERBANK and to do RFI mitigation on the fly\n')

    if (db_on):
        tpp_state("your_writer")

    try:
        n_zapped = do_RFI_filter(filestring,basename)
    except:
        if (db_on):
            status = "ERROR in your_writer: "+ str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()
        
    your_fil_object=Your(your_files.your_header.basename+"_converted.fil")

    # Report level of zapping
    if (n_zapped == 0):
         logger.info(f'RFI MASK: No channels zapped')
    else:
         logger.debug(f'RFI MASK: Number of channels zapped = {len(my_list)}')
         rfi_fraction = n_zapped/your_files.your_header.nchans
         logger.info('RFI MASK: Percentage of channels zapped = '+str(rfi_fraction*100)+' %')

    # Report RFI_fraction (and ideally pre-/post-zap RMS, though we might be able to obtain info for these values).
    if (db_on):
        try:
            data = {"rfi_fraction":rfi_fraction}
            db.patch("processing_outcomes",outcomeID,data=data)
        except:
            print_dberr()


    logger.debug('Writer done, moving on')

    

            

    ############## ############## ############## 
    ##############     DDPLAN     ############## 
    ############## ############## ############## 
    # Will be used for low-frequency data.
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


    ############## ############## ############## 
    ##############    HEIMDALL    ############## 
    ############## ############## ############## 

    if (db_on):
        tpp_state("heimdall")


    try:
        do_heimdall(your_fil_object)
    except:
        if (db_on):
            status = "ERROR in heimdall: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()


    ############## ############## ############## 
    ##############  CANDCSVMAKER  ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("candcsvmaker")

    # Go to the new directory with the heimdall cands
    cand_dir=os.chdir(os.getcwd()
                      + "/"
                      + your_fil_object.your_header.basename)
    logger.debug("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")

    try:
        n_events,n_members = do_candcsvmaker(your_fil_object)
    except:
        if (db_on):
            status = "ERROR in candcsvmaker: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()

    # POST UPDATE of n_events and n_members
    if (db_on):
        try:
            data={"n_detections": n_events,
                  "n_members": n_members}
            db.patch("processing_outcomes",outcomeID,data=data)
        except:
            print_dberr()

     
    logger.info('CHECKPOINT: Number of candidates created = '+num_events)


    #Create a directory for the h5s
    # RESHMA can you check the directory tracing here? Why are we making an h5 directory and is it appropriately used below? Is this a folder that your_candmaker explicitly needs but doesn't create itself?
    cwd = os.getcwd()
    try:
        os.makedirs("h5")
    except FileExistsError:
        logger.error("Could not create h5 directory in current directory "+str(cwd))
        exit()
    
    """
    !!!
    NOTE IT IS HERE THAT WE NEED TO DO COORDINATE CORRECTION FOR DRIFTSCAN DATA
    !!!
    """



    ############## ############## ############## 
    ############## YOUR_CANDMAKER ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("your_candmaker")

    try:
        do_your_candmaker(your_fil_object)
    except:
        if (db_on):
            status = "ERROR in your_candmaker: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()

        
    # Go into h5 directory, check all h5 files created appropriately.
    os.chdir(cwd+'/h5')
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
        logger.warning('POSSIBLE ISSUE: Not all cand h5s are created')
        #!RESHMA can you check if you agree that this is an exit-able offense?
        if (db_on):
            logger.error("ERROR in h5 file creation: Not all cand h5s were created.")
            tpp_state("ERROR in h5 file creation: Not all cand h5s were created.")
            exit()




    ############## ############## ############## 
    ##############     FETCH      ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("fetch")

    try:
        do_fetch()
    except:
        if (db_on):
            status = "ERROR in fetch: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()




    ############## ############## ############## 
    ##############   H5 PLOTTER   ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("your_h5plotter")
        
        
    if os.path.isfile('results_a.csv'):
        logger.info('FETCH: FETCH ran successfully')
        
        try:
            do_your_h5plotter()
        except Exception as error:
            if (db_on):
                status = "ERROR in your_h5plotter: "+str(traceback.format_exc())
                tpp_state(status)
            else:
                logger.error(str(traceback.format_exc()))
            exit()


        #!H TPPDB: gather all relevant info for RESULTS and push every
        #TPPDB: detection to database. Is there a way to do this in bulk?
        #TPPDB: ---ask Bikash. We will need to make sure we catch
        #TPPDB: range/format issues here and report them appropriately.
        
    else:
        logger.warning('FETCH:FETCH did not create a csv file')

    #TPPDB: Determine output directory and submit to OUTCOMES
    #TPPDB: output_directory? --- we will not do this here if the job launcher
    #TPPDB: handles the transfer and disk management.
    
    #TPPDB: at any point of failure above, the job_end should be updated and job_state should be updated to "ERROR: " with a relevant message.


    
    ############## ############## ############## 
    ########## SUBMIT CANDS TO DATABASE ######## 
    ############## ############## ############## 

    #!!! Here need a "write results to a file if db connection not
    #!!! happening successfully." We don't want to lose candidate
    #!!! records at this stage!

    
    n_push_success = 0
    if (db_on):
        
        for all candidates:
            # Populate candidate TPPDB data. Does this need to be done earlier?
            data = {'outcomeID':outcomeID,
                    'submissionID':submissionID,
                    'dataID':dataID}
            try:
                
            except:
                print_dberr()
            else:
                n_push_success += 1


    ############## ############## ############## 
    ##############     WRAP-UP    ############## 
    ############## ############## ############## 
    time_end_UTC = datetime.utcnow()
    if (db_on):
        tpp_state("complete")
        try:
            data = {"job_end":time_end_UTC}
            db.patch("processing_outcomes",outcomesID,data=data)
        except:
            print_dberr()

    delta_time = time_end_UTC - time_start_UTC
    duration_minutes = int(delta_time.seconds()/60)

    logger.info(f"Job complete after {duration_minutes:d} minutes")

    # (All done).


    
    #TPPDB PUSH: CHECK OUTPUT DIRECTORY AND TELL TPPDB what's the best location.
    # Joe's thing: /tingle/data/results/survey/MJDint/####/(hd5 or png)

    #TPPDB CHECK: read results document and double check everything exists and is populated.
