import os
import yaml
import traceback

try:
    config_file = str(os.getenv('TPP_HOME'))+"/config.yml"
    with open(config_file, 'r') as file:
        auth = yaml.safe_load(file)
except FileNotFoundError:
    print(traceback.format_exc())
    print("BASIC TPP CODE SETUP ERROR!\n\tYou seem to not have a config.yml file in the expected\n\tplace: "+config_file+"\n\tDouble check you have the TPP_HOME environment variable\n\tset to where you cloned the git repo (that's where config.yml\n\tusually lives). It is also possible the file is for some\n\treason unreadable.")
    exit()

# Set TPP DB authentication
tpp_user  = auth['tpp-db']['user']
tpp_pass  = auth['tpp-db']['pass']
tpp_token = auth['tpp-db']['token']
tpp_ip    = auth['tpp-db']['ip']
tpp_port  = auth['tpp-db']['port']
tpp_url = "http://" + tpp_ip + ":" + tpp_port + '/'
tpp_headers = {"Authorization": f"Bearer {tpp_token}"}

# Read globus authentication
globus_token   = auth['globus']['refresh_token']
globus_stor_id = auth['globus']['storage_collection_id']
globus_comp_id = auth['globus']['compute_collection_id']
globus_comp_dir= auth['globus']['compute_scratch_dir']
globus_res_dir = auth['globus']['storage_result_dir']
globus_client_id= auth['globus']['client_id']


# Set up dictionary for convenience
auth = {'tpp_user': tpp_user,
        'tpp_pass': tpp_pass,
        'tpp_token': tpp_token,
        'tpp_ip': tpp_ip,
        'tpp_port': tpp_port,
        'tpp_url': tpp_url,
        'tpp_headers': tpp_headers,
        'globus_token': globus_token
}


