import traceback
import requests   # Allow communications with TPP-DB API.
import yaml       # For reading private authentication data.
import config as dbconfig
#from . import config as db
#from tpp import infrastructure as db # Need to understand if this is the right thing to do and then replace the below instances of auth_info with db.auth

#!H!H!H Exiting in the exceptions throughout here might not be desirable if we want to continue running the pipeline


def init_document(collection,dataID,pipelineID=None,submissionID=None):
    """.

    Initiates an empty collection with the appropriate fields and
    returns the unique collection identifier.

    Input: 
        collection name (string)

    Output: 
        unique ID (string)

    """

    """.

    I'VE TEMPORARILY MADE THESE COMMENTED BECAUSE THEY'RE NOT
    REALLY GOING TO BE PART OF THE ONGOING BACK-AND-FORTH
    COMMUNICATIONS

    if (collection == "data"):
        dbdata = {
            'start_date_time': None,                # float MJD (45000-63000)
            'obs_length': None,                     # float seconds (1, 40_000)
            'ra_j': None,                           # float decimal degrees (0, 360)
            'dec_j': None,                          # float decimal degrees (-90, 90)
            'source_name': None,                    # string
            'beam': None,                           # int default to zero if observing system doesn't have beams.
            'regex_filename': None,                 # string without '/'
            'n_files': None,                        # int
            'md5_file': None,                       # List of strings
            'location_on_filesystem': None,         # str
            'survey': None,                         # str maximum of 20 characters
            'size': None                            # int in the units of MB.
        }
    elif (collection == "survey"):
        dbdata = {
            'survey': None,                         # str; !H!H but does this need to be given on instantiation?
            'parent_survey': None,                  # str limit to 20 characters
            'f_hi': None,                           # float (250 - 40_000)MHz
            'f_low': None,                          # float (250 - 40_000)MHz
            'zap_array': None,                      # Array of integers (0, 32000)
            'sampling_time': None,                  # float (1 - 40_000)microseconds
            'number_of_frequency_channels': None,   # int (10-20_000)
            'backend': None,                        # str Default to 16 characters
            'backend_mode': None,                   # str Default to 16 characters
            'telescope': None,                      # str Default to 16 characters
            'number_of_bits': None,                 # int (1 - 64)
            'no_of_pols': None,                     # int (1 - 4)
            'pol_type': None                        # str less than 10 characters
        }

    elif (collection == "candidate_results"):
        dbdata = {
            "submissionID": None,                   # str
            "outcomeID": None,                      # str
            "dataId": None,                         # str
            "dm": None,                             # float pc/cm^3
            "tcand": None,                          # float (45000-62000)MJD
            "cutout_start": None,                   # float (45000-62000)MJD
            "cutout_end": None,                     # float (45000-62000)MJD
            "fetch_width": None,                    # float (0-128)ms
            "gl": None,                             # float (0-360) degrees
            "gb": None,                             # float [-90, 90] degrees
            "f_ctr": None,                           # float (200-50000)MHz
            "detected_width": None,                 # float (0-128)ms
            "sn": None,                             # float
            "fetch_score": None,                    # float (0-1)
            "ymw_dm_mw": None,                      # float
            "ymw_dist": None,                       # float
            "ymw_z": None,                          # float
            "ne_dm_mw": None,                       # float
            "ne_dist": None,                        # float
            "result_location": None,                # str
            "result_name": None,                    # str
            "proposed_type": None,                  # str max 6 characters
            "confirmed_type": None,                 # str max 6 characters
            "interesting_info": {
                "is_interesting": None,             # True or False
                "user": None                        # str max 20 characters
            },
            "periodicity_info": {
                "periodicity_done": None,           # True or False
                "user": None                        # str max 20 characters
            },
            "differencing_info": {
                "differencing_done": None,          # True or False
                "user": None                        # str max 20 characters
            },
            "inspection_info": {
                "was_inspected": None,                 # True or False
                "user": None                            # str max 20 characters
            },
            "note_info": {
                "note": None,                       # str max 200 characters
                "when_submitted": None,              # "YYYY-MM-DDTHH:MM:SS"
                "user": None                          # str max 20 characters
            }
        }
    elif (collection == "pipeline_versions"):
        dbdata = {
            'launcher_version': None,               # str
            'pipeline_version': None,               # str
            'heimdall_version': None,               # str
            'your_version': None,                   # str
            'candcsvmaker_version': None,           # str
            'decimate_version': None,               # str
            'ddplan_version': None                  # str
        }
    """

    if (collection == "job_submissions"):
        if (pipelineID == None):
            #BAD
            response = "You tried to initiate a "+collection+" document with database.init_document() but did not include the required pipelineID."
            raise Exception(response)
        dbdata = {
            "pipelineID": pipelineID,               # str
            "dataID": dataID,                       # str
            "started_globus": None,                 # "YYYY-MM-DDTHH:MM:SS"
            "started_transfer_data": None,          # "YYYY-MM-DDTHH:MM:SS"
            "started_slurm": None,                  # "YYYY-MM-DDTHH:MM:SS"
            "status": {
                "completed": None,                  # Optional: True or False
                "date_of_completion": None,         # Optional: "YYYY-MM-DDTHH:MM:SS"
                "error": None                       # Optional: str
            },
            "username": dbconfig.auth['tpp_user'],  # str
            "duration": None,                       # float in seconds
            "target_directory": None,               # str
            "log_name": None,                       # str
            "log_dir": None                         # str
        }
    elif (collection == "processing_outcomes"):
        if (submissionID == None):
            #BAD
            response = "You tried to initiate a "+collection+" document with database.init_document() but did not include the required submissionID."
            raise Exception(response)
        dbdata = {
            'submissionID': submissionID,           # str
            'dataID': dataID,                       # str
            'node_system': None,                    # str
            'rfi_fraction': None,                   # float (0.0 - 1.0)
            'rms_prezap': None,                     # float
            'rms_postzap': None,                    # float
            'job_start': None,                      # "YYYY-MM-DDTHH:MM:SS"
            'job_end': None,                        # "YYYY-MM-DDTHH:MM:SS"
            'job_state_time': None,                 # "YYYY-MM-DDTHH:MM:SS"
            'job_state': None,                      # str (e.g., "Completed", "Failed", etc.)
            'fetch_histogram': None,                # List of floats
            'n_members': None,                      # int should be >= 0
            'n_detections': None,                   # int should be >= 0
            'n_candidates': None,                   # int should be >= 0
            'working_directory': None,              # str
            'output_directory': None                # str
        }

    
    try:
        collection_url = dbconfig.auth['tpp_url'] + str(collection)
        print("I'll try requests.post("+collection_url+",json="+str(dbdata)+",headers="+str(dbconfig.auth['tpp_headers']))
        response = requests.post(collection_url,json=dbdata,headers=dbconfig.auth['tpp_headers'])
        check_return_status(response)
    except LookupError:
        print(traceback.format_exc())
        exit()
        
    except:
        # An exception to a post requests usually means some kind of basic communications error that doesn't return a "response".
        print_comms_error()
        exit()

    # BIKASH
    # Need a query line here to understand what the self-autogenerated ID is.
    responseID = response.json()['inserted_id'][0]
    print("The ID I found was "+responseID+" --- it worked!")
        
    return responseID



#!H!H!H I think we need a tpp_wait that counts to a certain amount of time then times out and dies if it can't communicate with the database. This should probably go here in the post command (maybe callable as a separate thing).
def post(collection,data):
    ### SARAH need to check PATCH operation and fix this here.
    """.
    
    Pushes a piece of information to the database. 

    Input:
        database collection name, information

    Output: 
        status (failure,success) and error message if applicable.

    """
    try:
        collection_url = dbconfig.auth['tpp_url'] + str(collection)
        print("I'll try requests.post("+collection_url+",json="+str(data)+",headers="+str(dbconfig.auth['tpp_headers']))
        response = requests.post(collection_url,json=data,headers=dbconfig.auth['tpp_headers'])
        check_return_status(response)

    except LookupError:
        print(traceback.format_exc())
        exit()
        
    except:
        # An exception to a post requests usually means some kind of basic communications error that doesn't return a "response".
        print_comms_error()
        exit()
    
    return


def get(collection,filter):
    """.

    Basic database search.

    Input:
        database collection name.

    Output: filtered results (not sure what form??)

    """
    results = Null
    return results


def search_data_position(myRA,myDec,mySize):
    """.

    Simple "box" search. Will search for sources within both ra+/-size
    and dec+/-size. 

    !!! THIS SEARCH WILL WORK INCREASINGLY POORLY THE FURTHER AWAY YOU
        ARE FROM THE EQUATORIAL REGION!

    !!! It also needs to be fixed to actually take the beam size into
        account (that is, we only want to return pointings in
        principle within one HPBW of the requested position).

    !!! It will also not work properly wherever the range spans the
        sidereal day change (e.g. RA 350 to 10).

    
    Input: RA, Dec, +/-range in decimal degrees

    Output:
        Query results dictionary

    """


    try:

        # Set up query dictionary:
        query_dict = {"ra_j":{"$gte":myRA-mySize, "$lte":myRA+mySize},"dec_j": {"$gte":myDec-mySize, "$lte":myDec+mySize}}
        
        collection_url = dbconfig.auth['tpp_url'] + "data"
        #print("I'll try requests.post("+collection_url+",json="+str(data)+",headers="+str(dbconfig.auth['tpp_headers']))
        response = requests.get(collection_url, json=query_dict, headers=dbconfig.auth['tpp_headers'])
        check_return_status(response)
    
    except LookupError:
        print(traceback.format_exc())
        exit()
        
    except:
        # An exception to a post requests usually means some kind of basic communications error that doesn't return a "response".
        print_comms_error()
        exit()

    myresults = response.json()
    print("\n\n**********************\nI found THE FOLLOWING RESULTS\n**********************\n" + str(results))
    
    return myresults


#!H!H The long list of errors below aren't currently being passed properly.
#Actually on 8 January they seem to be working well!!! I think the "raise" format allowed it to work appropriately.
def check_return_status(response):

    """.

    Checks the return status of the database and reports any failurs
    as such. TPP-izes the HTTP error codes.

    The meaning of various HTTP status responses are detailed here:
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status

    Input:
        DB response numerical value

    Output: 
        status (failure,success) and error message if applicable.

    """

    # Default to failure. Guilty until proven innocent :(.
    # Note I think the "status" relevance is now obsolete.
    status = False

    print (response.json())
    code_num = response.status_code
    
    # Successful completion
    if (code_num >= 200 and code_num < 300):
        # Response may have been fine even if there was a TPPDB-side internal error.
        if ('error' in response.json().keys()):
            raise LookupError("A TPP DB internal error occurred: "+response.json()['error'])
        elif ('message' in response.json().keys()):
            status = True
            db_reply = response.json()['message']
#        elif (response.json() == [])
#            status = True #!H!H!H
    elif (code_num == 61):
        raise LookupError("Error 61: If the traceback notes some kind of connection error, you may be running on a computer that does not have direct access to the TPP database node. Try running from Link.")
    elif (code_num == 113):
        raise LookupError("Error 113 No route to host; You likely have the wrong IP or port. Double check your config.yml file and if needed ask Bikash/Sarah B-S for the latest database IP/port.")
    elif (code_num == 400):
        raise LookupError("Error 400 Bad request; Check your data schema name or entry against the expected format.")
    elif (code_num == 401):
        raise LookupError("Error 401 Unauthorized; Your TPP database authentication information is invalid. Please check the information your config.yml file and correct it (particularly TPP database username, password, token).")
    elif (code_num == 403):
        raise LookupError("Error 403 Forbidden; Your TPP database authentication information was valid, but for some reason you don't have permission to access the desired resource. Reach out to Bikash or Sarah B-S to proceed.")
    elif (code_num == 404):
        raise LookupError("Error 404 Not Found: While it is possible the database is down, it is also likely that your TPP Database IP or port is invalid, but possible also you are trying to access a collection or data field that does not exist. Start by double checking your config.yml file; ask Bikash or Sarah B-S to verify the correct IP and port.")
    elif (code_num == 405):
        raise LookupError("Error 405 Method Not Allowed: You're trying to use a method (push, get, delete) that's not allowed by the database. Double check with Bikash that what you're trying to do is valid.")
    elif (code_num == 408):
        raise LookupError("Error 408 Request Timeout: The database might be down. Please check with Bikash or Sarah B-S.")
    elif (code_num == 422):
        raise LookupError("Error 422 Unprocessable content: Double check the name of the collection you're submitting to. If that doesn't work, check the 'data' you're submitting to TPP DB. It may be out of expected range or of the wrong data type. Double check what you've submitted against the github and/or spreadsheet schema lists!")
    elif (code_num == 429):
        raise LookupError("Error 429 Too Many Requests: The TPP-database server is overloaded because you've sent too many requests in a short amount of time. Please wait before you try sending again.")
    elif (code_num == 451):
        raise LookupError("Error 451 Unavailable for Legal Reasons: QUIT BREAKING THE LAW!")
    elif (code_num == 500):
        raise LookupError("Error 500 Internal Server Error: The server has encountered a situation that it's not sure how to handle. Deep investigation of the steps leading to this response are required.")
    elif (code_num == 501):
        raise LookupError("Error 501 Not Implemented: It's likely that the method you tried to call (e.g. get, put, delete) is not available for the TPP database. Double check that you are doing what you think you're doing.")
    elif (code_num == 502):
        raise LookupError("Error 502 Bad Gateway: I'm not sure what this means but you better investigate.")
    elif (code_num == 503):
        raise LookupError("Error 503 Service Unavailable: The TPP Database seems to be unavailable right now; either it's down for maintenance or is overloaded. Try again much later.")
        #### !!!!!!!H Maybe add some kind of delayed push or wait loop.
    elif (code_num == 504):
        raise LookupError("Error 504 Gateway Timeout: TPP database timed out. Please report this to Bikash, it's likely a server-end error.")
    elif (code_num == 505):
        raise LookupError("Error 505: The HTTP version used in the request is not supported by the TPP Database. Please let Bikash know you received this error message, it's likely a problem on our end.")
    elif (code_num == 507):
        raise LookupError("Error 507 Insufficient Storage: Oh no, this is actually really bad. Report to TPP team IMMEDIATELY!")
    elif (code_num == 511):
        raise LookupError("Error 511 Network Authentication Requred: Seems you forgot to authenticate (or your authentication was invalid; if the latter, please check your config.yml file.")
    else:
        raise LookupError("TPP DATABASE SERVER ERROR: An unrecognized error occurred.")

    if status:
        print ("Server responded OK. (Status "+str(status)+").")
        print ("Message from TPP database: "+str(db_reply))
    else:
        raise LookupError("TPP DATABASE ERROR OCCURRED but I'm not sure how to diagnose it. Please contact Error Master Burke-Spolaor!")

    # Note it's not strictly neccessary to return anything here at all.
    return status




def gen_user(username,password):
    """.

    Generate a TPP database username and password.
    
    Author:    Sarah Burke-Spolaor
    Init Date: 12 Dec 2023
    
    This script can be run to instantiate a TPP database user.
    It will then initiate a token for the user.

    Input: 
        username [string]
        password [string]
        length   [token expiry time in days]
        
    Output: 
        token [currently needs to be added to config file by hand]

    """

    # Read config file for IP/port info.
    #auth = read_config()

    try:
        response = requests.post(dbconfig.auth['tpp_url'] + "sign_up", json={"username":username,"password":password})
        check_return_status(response)

    except LookupError:
        print(traceback.format_exc())
        exit()
        
    except:
        # An exception to a post requests usually means some kind of basic communications error that doesn't return a "response".
        print_comms_error()
        exit()


    dbconfig.auth['tpp_user'] = username
    dbconfig.auth['tpp_pass'] = password
    
    token = gen_token()


    print("Username "+username+" created.")
    print("****************************************************\nMake sure you update the following tpp-db information in your config.yml file:\nuser: \""+username+"\"\npass: \""+password+"\"\ntoken: \""+token+"\"\n****************************************************\n");

    return


def gen_token(length=3650):
    """.

    Generate a TPP database token.
    
    Author:    Sarah Burke-Spolaor
    Init Date: 23 May 2023
    
    This script can be run to instantiate a new user token. It is
    autoset to authenticate a token for 10 years. The user's
    config.yml must contain a correct IP and port.

    Input: 
        length [expiry in days]
        
    Output: 
        token [currently needs to be added to config file by hand]

    """

    # Read config file for authentication info
    tpp_token_call = dbconfig.auth['tpp_url'] + "token?=" + str(length)
    
    # Get the location of DATA_ID from TPP-DB
    try:
        token = requests.post(tpp_token_call, data = {"username":dbconfig.auth['tpp_user'],"password":dbconfig.auth['tpp_pass']}).json()['access_token']
          
    except LookupError:
        print(traceback.format_exc())
        exit()
          
    except:
        print_comms_error()
        exit()
          
    print("SUCCESS!!!")
    print("Your new token is: " + token + "\n It will expire in " + str(length) + " days.")
    print("Make sure you update the tpp-db \"token\" field in your config.yml file.\n")
    
    return token



def print_comms_error():
    """.

    Print some helpful interpretation if a database communications error occurs.

    """

    print(traceback.format_exc())
    print("BASIC DB COMMUNICATION ERROR! See traceback above.")
    print("FIRST, CHECK:\n\tDid you use a valid collection name? Check against the \n\tofficial collection names on the tpp_mongodb_fastapi github.")
    print("SECOND, CHECK:\n\tCheck that your config.yml file has the correct username,\n\tpassword, and token. It is also possible your token has\n\texpired and you need to generate a new one.")
    print("If you got error 61:\n\tYou may be running on a computer that does not have\n\tdirect access to the TPP database node. Try running\n\tfrom Link.")
    print("If your connection timed out or you got error 113:\n\tYou likely have the wrong IP/port in your config.yml\n\tfile; check it and confirm with Bikash/Sarah if needed.")
    print("Finally:\n\tIt is possible that the TPP server is down. If you have\n\tchecked for the above errors and are still having trouble,\n\tplease contact Bikash/Sarah.")

    return





#----------------------This code might not be needed-----------------------------
# This procedure is redunant but can be used in isolation if desired.
def check_tpp_auth(auth_info):
    """.

    Pings TPP database to verify authentication data. The ping is
    actually reading the "versions" schema.

    Input:
        auth_info dictionary

    Output: 
        status:  boolean; true (good), false (fail)
        message: None or String; error message, if any
    """

    try:
        response = requests.get(dbconfig.auth['tpp_url'], headers=dbconfig.auth['tpp_headers'])
        check_return_status(response)

    except LookupError:
        print(traceback.format_exc())
        exit()
        
    except:
        # An exception to a post requests usually means some kind of basic communications error that doesn't return a "response".
        print_comms_error()
        exit()

    return


def read_config():
    """.

    Reads authentication data from config.yml file.

    Returns a dictionary:
    tpp-token (long string)
    tpp-ip (e.g. 123.45.67.890
    tpp-port (e.g. 2000)
    tpp-url (e.g. https://tpp-ip:tpp-port/)
    tpp-headers (construct needed for communication)
    globus-token (globus key; not sure of format)

    """

    # Read tpp-db authentication
    config_file = "/Users/sbs/soft/tpp/config.yml" ###### !H NEED TO FIX THIS and below references to config_file- Emmanuel knows how to deal with "general config loading as global variable for the whole code"
    
    # Read config file for authentication info
    try:
        with open(config_file, 'r') as file:
            authentication = yaml.safe_load(file)
    except FileNotFoundError:
        print(traceback.format_exc())
        print("BASIC TPP CODE SETUP ERROR!\n\tYou seem to not have a config.yml file in the expected\n\tplace: "+config_file+"\n\tOr, the file is for some reason unreadable.")
        exit()
        
#    except:
#        print(traceback.format_exc())
#        print("BASIC TPP CODE SETUP ERROR! You seem to not have a config.yml file in the expected place, or the file is unreadable.")
#        exit()
        
    tpp_user  = authentication['tpp-db']['user']
    tpp_pass  = authentication['tpp-db']['pass']
    tpp_token = authentication['tpp-db']['token']
    tpp_ip    = authentication['tpp-db']['ip']
    tpp_port  = authentication['tpp-db']['port']
    tpp_url = "http://" + tpp_ip + ":" + tpp_port + '/'
    tpp_headers = {"Authorization": f"Bearer{tpp_token}"}

    # Read globus authentication
    globus_token = authentication['globus']['token']
    
    # Create dictionary for return.
    mydict = {'tpp_user': tpp_user,
              'tpp_pass': tpp_pass,
              'tpp_token': tpp_token,
              'tpp_ip': tpp_ip,
              'tpp_port': tpp_port,
              'tpp_url': tpp_url,
              'tpp_headers': tpp_headers,
              'globus_token': globus_token
    }

    #global auth_info
    #auth_info = mydict

    return mydict



def check_globus_auth(auth_info):
    """.

    Checks globus authentication info is valid.

    Input:
        auth_info dictionary

    Output:
        true (good), false (fail)
    """

    #!!!! NEED TO WRITE. Might be a Joe thing.
    
    return



