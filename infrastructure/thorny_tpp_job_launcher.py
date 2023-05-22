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

"""

# Allow communications with TPP-DB API
import requests

# JOE QUESTION
### (how do we deal with the authentication here from within the
### globus call? We will need the IP address of the node and the
### user's personal token. What's the best way to do this securely?
### Should each user on TF have this info locally stored so we can
### keep this launcher code on our public repo?

# Set up TPP-DB communications.
tppdb_ip = "999.999.99.999"
tppdb_port = "9999"
tppdb_base = "http://" + tppdb_ip + ":" + tppdb_port
tppdb_data = tppdb_base + "/data"
user_token = ""
headers = {"Authorization": f"Bearer{token}"}


#
# Data ID requested by user (unique identifier in DM of file to be processed).
#
# JOE QUESTION
# How this is obtained here depends on how the Globus call is
# integrated into this script...
data_id = ""

# Get the location of DATA_ID from TPP-DB
mydata = requests.get(tppdb_data + "/" + data_id, headers=headers).json()
filename = mydata['location_on_filesystem']



"""

#!/bin/bash
# Copy/paste this job script into a text file and submit with the command:

#sbatch thefilename

#SBATCH --time=5-23:45:00   # walltime limit (DD-HH:MM)
#SBATCH --nodes=1  # number of nodes
#SBATCH --ntasks-per-node=10
#SBATCH --job-name="Reshma"
#SBATCH --partition=Korok

### WHAT KIND OF GPU RESOURCE REQUESTS DO WE NEED HERE? ###

# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
### ADD SINGULARITY CALL HERE.
your_writer.py -f /wvu/scopes/gbo/AGBT20B_407_03_Cband/vegas_59092_80721_Fermi_0007*fits -t fil -nstart 82758620 -nsamp 82758620 -o ./ -name fermi_2_4hr -r -sksig 4 -sgsig 4 -sgfw 15 -g 4096 -fd 8 

"""
# Can also set up slurm resources on command line.
# sbatch --nodes=1 --wrap="command to run"
