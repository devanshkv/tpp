""".
TPP Job Launcher: Thorny Flat Edition


Author:    Sarah Burke-Spolaor
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
import requests   # Allow communications with TPP-DB API.#!!!! Superceded by db functionality
import subprocess # To call sbatch/slurm.
import yaml       # For reading private authentication data.
import globus_sdk as globus
from globus_sdk.scopes import TransferScopes
import argparse
import database as db

def manage_single_transfer(transfer_client, src, dest, src_location, dest_location):
    # Initiate Transfer using TransferClient
    task_data = globus.TransferData(source_endpoint=src, destination_endpoint=dest)
    task_data.add_item(src_location, dest_location)
    task_session = transfer_client.submit_transfer(task_data)
    task_id = task_session["task_id"]
    print(f"Submitted Transfer under Transfer ID: {task_id}")

    # Wait Until the Transfer is Complete (Time in Seconds)
    while not tc.task_wait(task_id, timeout=43200, polling_interval=15):
        print(".", end="")
    print(f"\n Transfer {task_id} has completed the following transfers:")
    for info in tc.task_successful_transfers(task_id):
        print(f"     {info['source_path']} ---> {info['destination_path']}")

    # TODO: Verify the Transfer Completed Correctly, Retry Loop if there are Failures

# -----------------------------------------------
# BEGIN MAIN LOOP
# -----------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launches the TPP pipeline.")
    parser.add_argument('--dataID', '-d', dest='dataID', type=int, default=None,
                        help="The Unique Data Identifier for the file to be processed by the pipeline.")
    args= parser.parse_args()

    # -----------------------------------------------
    # Data Identifier & Configuration File
    # -----------------------------------------------
    # Data ID requested by user (unique identifier in DM of file to be processed).
    dataID = args.dataID
    if dataID == None:
        print("Please enter the Unique Data Identifier to be processed. Otherwise this code cannot run.")
        exit()

    

   
    # -----------------------------------------------
    # Check that the latest pipeline is what's being run.
    # -----------------------------------------------

    try:
        current_pipelineID = db.current_pipelineID()
    except:
        exit()
        
    # -----------------------------------------------
    # GLOBUS Configuration
    # -----------------------------------------------

    # Requires Globus Authentication with West Virginia University as the IdP
    CLIENT_ID = db.dbconfig.globus_client_id
    auth_client = globus.NativeAppAuthClient(CLIENT_ID)
    auth_client.oauth2_start_flow(refresh_tokens=True, requested_scopes=TransferScopes.all)

    # Begin authorization via URL & User Input Code to Retrieve Token
    ## TODO: Use the Refresh_Tokens to Enable SSO Authentication for 24-Hours
    authorize_url = auth_client.oauth2_get_authorize_url()
    print(f"Please go to this URL and login:\n\n{authorize_url}\n")
    auth_code = input("Please enter the code here: ").strip()
    tokens = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_tokens = tokens.by_resource_server["transfer.api.globus.org"]

    # Construct the AccessTokenAuthorizer to Enable the TransferClient
    tc = globus.TransferClient(authorizer=globus.AccessTokenAuthorizer(transfer_tokens["access_token"]))

    # Set Up the Storage Collection and Compute Collection IDs
    storage = db.dbconfig.globus_stor_id
    compute = db.dbconfig.globus_comp_id
    
    # -----------------------------------------------
    # Transfer Necessary Files to Compute FS
    # -----------------------------------------------
    # Get the location of dataID on Storage from TPP-DB
    ## TODO: Sarah needs to add a comms failure check at this point.
    # Test DB connection and existence of data ID.
    try:
        db_response = db.get("data",dataID)
    except:
        # Hopefully db will print all appropriate errors.
        # Here we want to exit if there are fundamental issues with the DB.
        exit()

    stor_location = db_response['location_on_filesystem']

    #TPPDB INITIATE OUTCOMES and SUBMISSIONS documents here (and RESULTS?), after we are sure that the job is going to be launched. This way, the TPP pipeline only has to deal with updating information (not creating a new document) and knows the submissionID, dataID, etc relevant to this job.
    try:
        submissionID = db.init_document("job_submissions",dataID,pipelineID=current_pipelineID)
        outcomeID = db.init_document("processing_outcomes",dataID,submissionID=submissionID)
    except:
        exit()

    # Construct the Location on Compute FS
    ## TODO: Finalize FS Structure on Compute
    comp_location = db.dbconfig.globus_scratch_dir+"BLAHBLAHBLAH"

    # Transfer the Required files from Storage to Compute
    manage_single_transfer(tc, storage, compute, stor_location, comp_location)

    # -----------------------------------------------
    # Launch the TPP Pipeline via the SLURM Command
    # -----------------------------------------------

    #!H!H!H
    # Use command line to call slurm.
    #subprocess.run(["sbatch","--time=5-23:45:00 --nodes=1 --ntasks-per-node=10 --job-name=\"Reshma\" --partition=thepartitiontouse --wrap=\"SINGULARITY CALL ; COMMAND TO RUN"]) ###### NEED TO FIX THIS

    # Communicate to TPP-Database that the SLURM Job has been Launched

    # Wait for Slurm Job to Complete, Alerting any Errors

    # Communicate to TPP-Database the Final Status of SLURM Job

    # -----------------------------------------------
    # Transfer Products from Compute to Storage
    # -----------------------------------------------
    #Construct the Location on Compute FS of Products
    comp_location_hdf5 = db.dbconfig.globus_comp_dir+"BLAHBLAHBLAH"+".hdf5"

    #Construct the Location on Storage FS of Products
    stor_location_hdf5 = db.dbconfig.globus_res_dir+"/BLAHBLAHBLAH"+".hdf5"

    #Transfer the Final Products from Compute to Storage
    manage_single_transfer(tc, compute, storage, comp_location_hdf5, stor_location_hdf5)


    
# CUSTOM CONFIG FILE FUNCTIONALITY not yet allowed or implemented. It remains here as a historical relic.
#config    parser.add_argument('--config', '-c', dest='config_file', type=str, default=None,
#config                        help="The user-specific Configuration File required for the pipeline (USUALLY YOU SHOUDL NOT SPECIFY THIS. By default user's config.yml will be read from the TPP pipeline install directory.", required=False)
#config    # Configuration YAML provided by the user (contains tokens, networking settings, etc)
#config    config_file = args.config_file
#config    if config_file == None:
#config        config_file = input("Please enter the absolute path of your TPP Configuration File: ").strip()
#config
#config    # Read config file for authentication info
#config    with open(config_file, 'r') as file:
#config        config = yaml.safe_load(file)
#config
#config    # Set Required Variables
#config    tppdb_ip = config['tpp-db']['url']
#config    tppdb_port = config['tpp-db']['port']
#config    user_token = config['tpp-db']['token']
#config    # -----------------------------------------------
#config    # TPP-Database Communication Configuration
#config    # -----------------------------------------------
#config    tppdb_base = "http://" + tppdb_ip + ":" + tppdb_port
#config    tppdb_data = tppdb_base + "/data"
#config    headers = {"Authorization": f"Bearer{user_token}"}
#config    CLIENT_ID = config['globus']['client_id']
#config    storage = config['globus']['storage_collection_id']
#config    compute = config['globus']['compute_collection_id']
#config    comp_location = config['globus']['compute_scratch_dir']+"BLAHBLAHBLAH"
