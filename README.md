# TPP
The Petabyte Project, an extensive search for FRBs in all major historic single-dish pulsar and transients surveys.

This repository contains the core TPP pipeline and networking/communications infrastructure code.


# Dependencies:
requests
yaml
subprocess


# Thorny Flat set-up for new users

Running the full TPP infrastructure at WVU will require access to the
Thorny Flat supercomputing system and to Globus. The instructions
below assume you are a member of the WVU community and have the
relevant base account to obtain access to these resources. If you do
not, please speak with your advisor or TPP contact about getting a WVU
mix ID.

Steps:

1. Obtain Thorny Flat account as described here:
https://docs.hpc.wvu.edu/text/21.GettingAccess.html

2. Get your Globus account linked to your WVU ID as described here:
https://docs.globus.org/how-to/get-started

3. Edit the config.yml file
Ask your advisor for the IP, port, and how to get a token. (note - sarah would like to have some little functions that do things like generate tokens, check authentication, etc).

4. 


You will need an account 


To set up:

Edit the config file, insert correct globus key.

The flow goes like this:

you'll launch the TPP job through globus.

globus transfers file and runs job launcher

job launcher runs slurm.

globus tracks job launcher script.

globus code will clean up when slurm job done.

