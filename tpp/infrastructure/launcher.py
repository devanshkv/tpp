""".
TPP Job Launcher: Thorny Flat Edition


Author:    Sarah Burke-Spolaor but also maybe mostly Joe Glaser
Init Date: 22 May 2023

This code will be called by the Globus file transfer script (intended
to be written by Joe Glaser). The purpose of this code is to:

 - Be runnable on Thorny Flat (and potentially easily repurposed for
   Dolly Sods in the future).

 - Access the TPP database manager (TPP-DB).

 - Identify the location of requested data from TPP-DB.

 - Set up the job command and call Slurm.

 - Also initiate a Globus transfer of H5 files to JBOD at the end of
   the script? Or will this be done in the processing script? - Joe G
   to comment.


THERE ARE SEVERAL REASONS LAUNCHER SHOULD FORCE-FAIL:

 - It can't reach the TPP database.
 - It can't find the file.
 - There's not enough space on thorny flat.

""" 

# -----------------------------------------------
# General Module Imports
# -----------------------------------------------
import subprocess # To call sbatch/slurm.
import yaml       # For reading private authentication data.
import globus_sdk as globus
from globus_sdk.scopes import TransferScopes
import argparse
import database as db
from datetime import datetime
import getpass
import traceback
import file_manager as fm
import random


# -----------------------------------------------
# BEGIN MAIN LOOP
# -----------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launches the TPP pipeline.")
    parser.add_argument('--dataID', '-d', dest='dataID', type=int, default=None,
                        help="The Unique Data Identifier for the file to be processed by the pipeline.")
    args = parser.parse_args()

    # -----------------------------------------------
    # Data Identifier & Configuration File
    # -----------------------------------------------
    # Data ID requested by user (unique identifier in DM of file to be processed).
    dataID = args.dataID
    if dataID == None:
        print("Please enter the Unique Data Identifier to be processed. Otherwise this code cannot run.")
        exit()


    # -----------------------------------------------
    # GLOBUS Configuration
    # -----------------------------------------------

    # Requires Globus Authentication with West Virginia University as the IdP
    CLIENT_ID = db.dbconfig.globus_client_id
    auth_client = globus.NativeAppAuthClient(CLIENT_ID)
    auth_client.oauth2_start_flow(refresh_tokens=True, requested_scopes=TransferScopes.all)

    
    # Begin authorization via URL & User Input Code to Retrieve Token
    ## TODO: Use the Refresh_Tokens to Enable SSO Authentication for 24-Hours (work-around to avoid constant duofactor authentication)
    ## Joe says we need to have a loop here to check if the key exists and skip the auth lines if it does.
    ### Joe doesn't know how to write that off the top of his head. See if graham will reach out to globus support to do help this. NOTE TIM O DOES THIS ALL THE TIME (related to PSC) SO ASK HIM.
    authorize_url = auth_client.oauth2_get_authorize_url()
    print(f"Please go to this URL and login:\n\n{authorize_url}\n")
    auth_code = input("Please enter the code here: ").strip()
    tokens = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_tokens = tokens.by_resource_server["transfer.api.globus.org"]

    # Construct the AccessTokenAuthorizer to Enable the TransferClient (tc)
    tc = globus.TransferClient(authorizer=globus.AccessTokenAuthorizer(transfer_tokens["access_token"]))

    # Set Up the Storage Collection and Compute Collection IDs
    storage_id = db.dbconfig.globus_stor_id
    compute_id = db.dbconfig.globus_comp_id


    # Construct the Location on Compute FS
    #   (This is the working directory that the file will be
    #   transferred to, and where tpp_pipeline will initiate its
    #   processing)
    ## TODO: Finalize FS Structure on Compute (OPTIONAL. DONT HAVE A CENTRAL GROUP SCRATCH SPACE RIGHT NOW, WE ARE USING USER SCRATCH SPACES AND ARE HAPPY ABOUT THAT RIGHT NOW.) 
    comp_location = db.dbconfig.globus_comp_dir



    # Construct the Location on Compute FS of Products.
    #   (This is the working directory location of all tpp_pipeline
    #   results, from which we will transfer to permanent storage)
    comp_location_final = db.dbconfig.globus_comp_dir+"/"#+!!! RESHMA what is the name of the directory where the final results come out? Check (or fix tpp_pipeline) after testing.

    
    # Construct the Location on Storage FS of Products
    #   (This is the final storage directory on Tingle)
    """.

    Because we'll be processing on Thorny Flat or Dolly Sods, there's
    no clear way here to access Tingle to auto-query which directory
    the results should be in.  Here is our solution:

      - In 1 PB of data, with around 15GB per file, there will be
        O(100000) outcomes directories. There will be more outcomes if
        we process any files multiple times or if file sizes are
        typically smaller.

      - If we have 500 random result directory divisions (labelled
        001, 002, etc.), there will be about 200 outcome directories
        per division. Lots of wiggle room for more to be in a
        particular division.

      - We can randomly generate a number between 1 and 400,
        sprinkling the results semi-evenly into the directories.

      - The output directory will be named for the outcomeID.

      - The randomizer defining "divisions" below can always be
        changed, so if for some reason 500 divisions aren't enough and
        we get overcrowding, we can always change this much later in
        the processing.

    Signed - Sarah, who you can blame if this is a bad decision.

    """
    result_division = random.randrange(1,500,1)
    stor_location_final = db.dbconfig.globus_res_dir+"/"+str(result_division)+"/"+outcomeID


    
    # -----------------------------------------------
    # Set up TPP-DB connections
    # -----------------------------------------------

    time_start = datetime.utcnow()
    
    try:
        # Check that the current pipeline is being run (and test of TPPDB connection).
        current_pipelineID = db.current_pipelineID()

        #!!! We might be able to at least check that tpp_pipeline.py
        #!!! and other "tpp" package software is current by looking at
        #!!! the "githash" We get this has by running:
        #!!!       git rev-parse HEAD
        #!!! (in the main tpp code directory, not a subdirectory)

        # Test that the requested dataID exists.
        db_response = db.get("data",dataID)
        file_dir = db_response['location_on_filesystem']
        file_base = db_response['regex_filename']

        #TODO Joe how will globus respond to a regex filename? How can we deal with this if there are multiple files? While we're on it... How can we check that all the data was successfully transferred? We will have all of the md5 hashes to know how big the files should be from source, in case that helps.
        #TODO Joe (and sarah) when passing a regex string will this cause issues to e.g. subprocess? Do special characters like ? or * need us to add some kind of "escape" \ symbol or something?
        # Joe says ask Tim O
        #Initiate submissions doc, after we are sure that the job is likely to be launched successfully.
        submissionID = db.init_document("job_submissions",dataID,pipelineID=current_pipelineID)
        print("Created submissionID "+str(submissionID))
        username = getpass.getuser()
        db.patch("job_submissions",submissionID,data={"started_globus":time_start.isoformat(),"username":username})
    except:
        # Hopefully db will print all appropriate errors.
        # Here we want to exit if there are fundamental issues with the DB.
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_start.isoformat(),"error":traceback.format_exc()}})
        exit()
                


    # -----------------------------------------------
    # Transfer Necessary Files to Compute FS
    # -----------------------------------------------        
    stor_location = file_base + file_dir  ###!!!!TODO ... is this backwards? shouldn't dir come first??
    print("Will transfer file from " + stor_location)

    # Transfer the Required files from Storage to Compute
    try:
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"started_transfer_data":time_UTC,"target_directory":comp_location})
    except:
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()


    # Here is a main operation: Transfer data from storage to compute location.
    try:
        fm.manage_single_transfer(tc, storage_id, compute_id, stor_location, comp_location)
    except:
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()

    
    # -----------------------------------------------
    # Launch the TPP Pipeline via the SLURM Command
    # -----------------------------------------------

    # Set up logging directory and file. !H!H!H need to get slurm to write to this log!
    log_name = f"{time_start.year:04d}{time_start.month:02d}{time_start.day:02d}_{time_start.hour:02d}{time_start.minute:02d}{time_start.second:02d}_{submissionID}.log"
    log_dir = comp_location
    
    # Initiate outcome doc before job submission.
    # Also Initiate RESULTS document? -- I don't think so, it can be written at
    # end of pipeline. Outcomes will track progress. But add it in here if
    # there's a need to have the diagnostics that an incomplete results DB could
    # provide (over, for instance, the job log alone).
    try:
        outcomeID = db.init_document("processing_outcomes",dataID,submissionID=submissionID)
        #!H!H!H  ADD RESULTS DOC HERE?
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"started_slurm":time_UTC,"log_name":log_name,"log_dir":log_dir})
    except:
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()
    
    tpp_pipe = "../../tpp_pipeline.py" #!!! THE LOCATION OF tpp_pipeline.py--needs to be self-referenced in the code or imported maybe?

    max_jobtime = 5760 # Set jobs to force fail after 4 full days of processing.

    # !!!! EMAIL PER JOB HERE INCLUDED HERE ONLY FOR TESTING PURPOSES! We can turn this off after we know launcher is working.
    # The -W option in sbatch forces the sub-process to not finish until the batch job actually completes (with failure or success).
    sbatch_command = "sbatch -W --time=5800 --nodes=1 --ntasks-per-node=10 --job-name=\"TPP-"+submissionID+"\" --partition=comm_gpu_week --gres=gpu:1 --mail-user="+username+"@mix.wvu.edu --mail-type=BEGIN,END,FAIL -o \"" + log_dir + log_name + "\" --wrap='module load singularity ; singularity exec /shared/containers/radio_transients/radio_transients.sif "+tpp_pipe+" -tppdb mastersword " + outcomeID + " " + comp_location + " -f " + file_base + "'"

    # Report intended launch command.
    print("\n\nI'm launching the following sbatch command and will wait until it completes fully before continuing:\n")
    print(sbatch_command)
    print("\n\n")

    # Launch to slurm
    try:
        sbatch_response = subprocess.getoutput(sbatch_command)
    except:
        print("\n\nMAJOR ERROR: SBATCH SUBMISSION FAILED.\n\n")
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()


    print(sbatch_response)
    print("The job submitted through sbatch has completed with the response printed above.") 


    # Communicate to TPP-Database that the SLURM Job has been Launched
    # Wait for Slurm Job to Complete, Alerting any Errors
    # Communicate to TPP-Database the Final Status of SLURM Job

    
    # -----------------------------------------------
    # Transfer Products from Compute to Storage
    # -----------------------------------------------
    try:
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":"final transfer"})
    except:
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()

    # SARAH MAKE SURE comp_location_final IS READ FROM whatever makes that directory

    #Transfer the Final Products from Compute to Storage
    #TODO !!! Joe, below here we need the transfer to actually transfer
    # everything from the directory to the target location. Can we do
    # that with manage_single_transfer as written?
    # Joe says probably not (9/19/2024)--- comp_location_final might be given as a directory and transfer everything, but it might not do that. ASK GLOBUS and/or TIM O. Sarah also make sure that h5 files or directories get to their own outcome folder tagged to job name (so we avoid overwrites--also check for existing folder to make sure it doesn't exist).
    try:
        fm.manage_single_transfer(tc, compute_id, storage_id, comp_location_final, stor_location_final)
    except:
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()
        
    time_end = utc.datetime()

    delta_time = time_end - time_start
    duration_minutes = int(delta_time.seconds()/60)
    
    try:
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"competed":True,"date_of_completion":time_UTC},"duration":duration_minutes,"output_directory":stor_location_final})
    except:
        # Send error to submissionID STATUS. This will only work if there isn't a tppdb comms error.
        time_UTC = datetime.utcnow().isoformat()
        db.patch("job_submissions",submissionID,data={"status":{"date_of_completion":time_UTC,"error":traceback.format_exc()}})
        exit()

    # JOE SAYS WE NEED TO ADD A COMMAND HERE TO DELETE EVERYTHING - to delete files after the specific files are transferred. Or we can write our own delete to make sure the entire directory tree is deleted.    
    #!!! JOE, how/when will all the data be cleaned up in the end? Will we just have a bunch of copies? Do we need to write a clean-up script after processing is done? Should this be separate?
    #(note for sarah self, this will affect where we write slurm logs to)
    #(note, this will also affect peoples ability to follow up on issues. maybe we don't do clean-up if we are still in the testing phase but turn auto-clean-up on later.)

